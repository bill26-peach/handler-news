# -*- coding: utf-8 -*-
"""
ES7 并发加速版（保留你现有的 json_to_excel 写入逻辑）
- helpers.scan 流式拉取
- AI 调用并发、连接复用、自动重试 + 手动重试 + 限流、关键日志
- 最终仍调用 json_to_excel(json_list, f"{month}_news.xlsx")
"""
import json
import re
import calendar
import time
import logging
import random
import threading
from collections import deque
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from elasticsearch import Elasticsearch, helpers, exceptions
from json_excel import json_to_excel

# ======================= 日志设置 =======================
logging.basicConfig(
    level=logging.INFO,  # 可改为 DEBUG 以查看更详细日志
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# ======================= 配置区 =======================
ES_HOSTS = ["http://10.10.25.22:9200"]
INDEX = "biz_tos_rss_news"
USERNAME = "elastic"
PASSWORD = "easymer@es@2023"

NEWS_CHANNELS = [
    "今日新闻网-焦点", "今日新闻网-要闻", "ETtoday新闻云-即时新闻-热门", "ETTODAY新闻云-焦点新闻",
    "自由时报-首页-热门新闻", "联合新闻网-要闻", "中时新闻网-台湾", "东森新闻-热门",
    "三立新闻网-热门新闻板块", "TVBS新闻网-热门"
]

API_KEY = "app-bLCySrNdNLlYdLmYft16DgTY"
URL = "http://10.10.25.34:8660/v1/workflows/run"

# ========== 并发 / 超时 / 重试 / 限速 参数（按服务能力微调）==========
MAX_WORKERS = 8           # AI 并发线程数（遇到超时可先降到 8）
CONNECT_TIMEOUT = 5        # 连接超时（秒）
READ_TIMEOUT = 60          # 读取超时（秒，模型生成慢时要更长）
REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

MAX_RETRIES = 5            # urllib3 自动重试总次数（含连接/读取）
BACKOFF_FACTOR = 1.0       # 自动重试退避系数
STATUS_FORCELIST = (429, 500, 502, 503, 504)

MANUAL_RETRIES = 3         # 在 urllib3 自动重试之后再做几次手动重试
PROGRESS_EVERY = 50        # AI 完成多少条打印一次进度

MAX_QPS = 8                # 全局 QPS 限速（每秒最多多少请求）
# ======================================================

# ======================= 工具与数据处理 =======================
_CN_NUM = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10, "十一": 11, "十二": 12}

SITE_MAP = {
    "今日新闻网": {"domain": "www.nownews.com", "color": "蓝营"},
    "ETTODAY新闻云": {"domain": "www.ettoday.net", "color": "绿营"},
    "ETtoday新闻云": {"domain": "www.ettoday.net", "color": "绿营"},
    "自由时报": {"domain": "news.ltn.com.tw", "color": "绿营"},
    "联合新闻网": {"domain": "udn.com", "color": "蓝营"},
    "中时电子报": {"domain": "www.chinatimes.com", "color": "蓝营"},
    "东森新闻": {"domain": "news.ebc.net.tw", "color": "蓝营"},
    "ETtoday 东森新闻": {"domain": "news.ebc.net.tw", "color": "蓝营"},
    "三立新闻网": {"domain": "www.setn.com", "color": "绿营"},
    "TVBS新闻网": {"domain": "news.tvbs.com.tw", "color": "蓝营"},
}

def _parse_month(month_str: str):
    """解析 '8月'/'08'/'8'/'一月' 等，返回 (YYYY-MM-DD, YYYY-MM-DD)"""
    m = None
    m_digits = re.search(r'(\d{1,2})', month_str)
    if m_digits:
        m = int(m_digits.group(1))
    else:
        for k, v in _CN_NUM.items():
            if k in month_str:
                m = v
                break
    if not m or not (1 <= m <= 12):
        raise ValueError('month 非法，应类似 "8月" / "08" / "8" / "一月"')

    year = date.today().year
    first_day = date(year, m, 1)
    last_dom = calendar.monthrange(year, m)[1]
    cap_to_30 = True
    last_day = date(year, m, min(30 if cap_to_30 else last_dom, last_dom))
    return first_day.strftime("%Y-%m-%d"), last_day.strftime("%Y-%m-%d")

def get_es_client() -> Elasticsearch:
    """只创建一次，外层复用"""
    return Elasticsearch(ES_HOSTS, http_auth=(USERNAME, PASSWORD), verify_certs=False)

def export_all(es: Elasticsearch, index: str, query: dict) -> Iterable[Dict]:
    """生成器：逐条产出 _source（dict）"""
    for doc in helpers.scan(
        es,
        index=index,
        query=query,
        scroll="2m",
        size=1000,
        request_timeout=120,
        preserve_order=False,
        _source_includes=["catm", "site_name", "nopl", "cntt", "titl"]
    ):
        yield doc["_source"]

def normalize_item(item: Dict) -> Dict:
    """站点映射 + 时间格式归一"""
    site_name = item.get("site_name")
    mapping = SITE_MAP.get(site_name)
    if mapping:
        item["domain"] = mapping["domain"]
        item["color"] = mapping["color"]
    else:
        item["domain"] = None
        item["color"] = None

    catm_val = item.get("catm")
    if catm_val:
        try:
            dt = datetime.strptime(catm_val, "%Y-%m-%d %H:%M:%S")
            item["catm"] = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return item

# ======================= QPS 限速器 =======================
class RateLimiter:
    """滑动窗口限速：最多 qps 次/秒"""
    def __init__(self, qps: int):
        self.qps = qps
        self.lock = threading.Lock()
        self.window = deque()  # 存最近 1 秒的时间戳

    def acquire(self):
        if self.qps <= 0:
            return
        with self.lock:
            now = time.time()
            # 清理超过 1 秒的请求
            while self.window and now - self.window[0] > 1.0:
                self.window.popleft()
            if len(self.window) >= self.qps:
                sleep_for = 1.0 - (now - self.window[0])
                if sleep_for > 0:
                    time.sleep(sleep_for)
            self.window.append(time.time())

rate_limiter = RateLimiter(MAX_QPS)

# ======================= 并发 AI 客户端 =======================
class AIClient:
    """复用连接 + 自动重试 + 手动重试 + 限流"""
    def __init__(self, url: str, api_key: str):
        self.url = url
        self.session = requests.Session()
        retry = Retry(
            total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=STATUS_FORCELIST,
            allowed_methods=frozenset(["POST"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS * 2
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    def call_api_one(self, new_obj: Dict) -> str:
        """
        输入 dict，输出 JSON 字符串（与你的 json_to_excel 兼容）
        - urllib3 自动重试之后，再做一层手动重试（ReadTimeout/429/5xx）
        - 全局 QPS 限速，避免堆积
        """
        payload = {
            "inputs": {"json": json.dumps(new_obj, ensure_ascii=False)},
            "response_mode": "blocking",
            "user": "admin"
        }

        for attempt in range(1, MANUAL_RETRIES + 1):
            # QPS 限速
            rate_limiter.acquire()
            try:
                resp = self.session.post(
                    self.url, headers=self.headers, json=payload,
                    timeout=REQUEST_TIMEOUT
                )
                status = resp.status_code

                if status in STATUS_FORCELIST:
                    wait = (BACKOFF_FACTOR * (2 ** (attempt - 1))) + random.uniform(0, 0.3)
                    logging.warning(f"[AI] HTTP {status}（第{attempt}/{MANUAL_RETRIES}次），退避 {wait:.2f}s")
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                data = resp.json()
                outputs_get = (data.get("data", {}) or {}).get("outputs", {}).get("result", "")
                if outputs_get:
                    # 如果返回本身是 JSON 字符串就直接用；否则包在 ai_result 字段里
                    try:
                        json.loads(outputs_get)
                        return outputs_get
                    except Exception:
                        return json.dumps({**new_obj, "ai_result": outputs_get}, ensure_ascii=False)
                # 没有 result 就回写原文
                return json.dumps(new_obj, ensure_ascii=False)

            except requests.ReadTimeout as e:
                wait = (BACKOFF_FACTOR * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                logging.warning(f"[AI] ReadTimeout（第{attempt}/{MANUAL_RETRIES}次），退避 {wait:.2f}s：{e}")
                time.sleep(wait)
                continue
            except requests.ConnectionError as e:
                wait = (BACKOFF_FACTOR * (2 ** (attempt - 1))) + random.uniform(0, 0.5)
                logging.warning(f"[AI] ConnectionError（第{attempt}/{MANUAL_RETRIES}次），退避 {wait:.2f}s：{e}")
                time.sleep(wait)
                continue
            except requests.RequestException as e:
                logging.warning(f"[AI] 请求异常，直接回退原文：{e}")
                return json.dumps(new_obj, ensure_ascii=False)

        logging.error("[AI] 多次重试仍失败，回退原文")
        return json.dumps(new_obj, ensure_ascii=False)

# ======================= 主流程 =======================
def search_datas(month: str) -> List[str]:
    logging.info(f"开始处理月份：{month}")
    month_gte, month_lte = _parse_month(month)
    results: List[str] = []
    es = get_es_client()
    ai = AIClient(URL, API_KEY)

    total_docs = 0
    docs: List[Dict] = []

    # 1) 拉取并规范化 ES 数据（带频道级日志）
    for channel in NEWS_CHANNELS:
        logging.info(f"[ES] 开始查询频道：{channel}")
        t0 = time.time()
        query = {
            "query": {"bool": {
                "must": [
                    {"match": {"nopl": channel}},
                    {"range": {"catm": {"gte": f"{month_gte} 00:00:00",
                                        "lte": f"{month_lte} 23:59:59"}}}
                ]
            }}
        }
        count = 0
        try:
            for src in export_all(es, INDEX, query):
                docs.append(normalize_item(src))
                count += 1
            elapsed = time.time() - t0
            logging.info(f"[ES] 完成频道：{channel}，共 {count} 条，用时 {elapsed:.2f}s")
            total_docs += count
        except exceptions.AuthenticationException as e:
            logging.error(f"[ES] 频道 {channel} 认证失败：{e}")
        except exceptions.AuthorizationException as e:
            logging.error(f"[ES] 频道 {channel} 权限不足：{e}")
        except exceptions.ConnectionError as e:
            logging.error(f"[ES] 频道 {channel} 连接失败：{e}")
        except Exception as e:
            logging.error(f"[ES] 频道 {channel} 其他错误：{e}")

    logging.info(f"[ES] 所有频道数据获取完成，总计 {total_docs} 条记录")

    # 2) 并发调用 AI（带进度日志）
    logging.info(f"[AI] 开始并发调用，任务数：{len(docs)}，并发度：{MAX_WORKERS}，限速：{MAX_QPS} qps")
    t_ai = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [pool.submit(ai.call_api_one, obj) for obj in docs]
        total = len(futures)
        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1
            if done % PROGRESS_EVERY == 0 or done == total:
                logging.info(f"[AI] 已完成 {done}/{total} 条（{done / total * 100:.1f}%）")

    logging.info(f"[AI] 全部调用完成，用时 {time.time() - t_ai:.2f}s")
    logging.info(f"查询与分析完成，总计 {len(results)} 条")

    return results

def main():
    import argparse
    parser = argparse.ArgumentParser(description="新闻解析（并发+关键日志+稳态版）")
    parser.add_argument("--month", required=True, help="月份（如 8 或 8月 或 八月）")
    args = parser.parse_args()

    # 获取与分析
    json_results = search_datas(args.month)

    # 写 Excel
    outfile = f"{args.month}_news.xlsx"
    logging.info(f"[Excel] 开始写入：{outfile}")
    t0 = time.time()
    json_to_excel(json_results, outfile)
    logging.info(f"[Excel] 写入完成，用时 {time.time() - t0:.2f}s")

if __name__ == "__main__":
    main()
