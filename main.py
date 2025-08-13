# -*- coding: utf-8 -*-
"""
ES7 并发加速版（保持你的 json_to_excel 写入逻辑不变）
- helpers.scan 流式拉取
- AI 调用并发、连接复用、重试
- 最终仍调用 json_to_excel(json_str_list, f"{month}_news.xlsx")
"""
import json
import re
import calendar
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from elasticsearch import Elasticsearch, helpers, exceptions


# -----------------------------------------------------------------------

# ============== 配置 ==============
ES_HOSTS = ["http://10.10.25.22:9200"]
INDEX = "biz_tos_rss_news"
username = "elastic"
password = "easymer@es@2023"

news_channels = [
    "今日新闻网-焦点", "今日新闻网-要闻", "ETtoday新闻云-即时新闻-热门", "ETTODAY新闻云-焦点新闻",
    "自由时报-首页-热门新闻", "联合新闻网-要闻", "中时新闻网-台湾", "东森新闻-热门",
    "三立新闻网-热门新闻板块", "TVBS新闻网-热门"
]

API_KEY = "app-bLCySrNdNLlYdLmYft16DgTY"
URL = "http://10.10.25.34:8660/v1/workflows/run"

# 并发/重试参数（按你服务的QPS与稳定性微调）
MAX_WORKERS = 16
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.6

_CN_NUM = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10, "十一": 11,
           "十二": 12}

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


def get_client_basic():
    """只创建一次，外层复用"""
    return Elasticsearch(ES_HOSTS, http_auth=(username, password), verify_certs=False)


def export_all(es: Elasticsearch, index: str, query: dict):
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


def normalize_item(item: dict) -> dict:
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


# ----------- 并发 AI 客户端 -----------
class AIClient:
    def __init__(self, url: str, api_key: str):
        self.url = url
        self.session = requests.Session()
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["POST"])
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS * 2)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    def call_api_one(self, new_obj: dict) -> str:
        """
        输入 dict，输出 JSON 字符串（与你的 json_to_excel 兼容）
        - 优先返回 AI 结果（如果 API 返回 "result" 字段）
        - 失败则回退写原始对象
        """
        payload = {"inputs": {"json": json.dumps(new_obj, ensure_ascii=False)},
                   "response_mode": "blocking",
                   "user": "admin"}
        try:
            resp = self.session.post(self.url, headers=self.headers, json=payload, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            outputs_get = (data.get("data", {}) or {}).get("outputs", {}).get("result", "")
            if outputs_get:
                # 如果 AI 返回的是 JSON 字符串，直接返回；否则把它包进 result 字段
                try:
                    json.loads(outputs_get)
                    return outputs_get
                except Exception:
                    return json.dumps({**new_obj, "ai_result": outputs_get}, ensure_ascii=False)
            # 无结果时回写原始对象
            return json.dumps(new_obj, ensure_ascii=False)
        except requests.RequestException as e:
            print(f"请求失败: {e}")
            return json.dumps(new_obj, ensure_ascii=False)


# ----------- 主流程 -----------
def search_datas(month: str) -> list[str]:
    month_gte, month_lte = _parse_month(month)
    results: list[str] = []
    es = get_client_basic()
    ai = AIClient(URL, API_KEY)

    # 1) 先把 ES 结果“规范化”为 dict 列表（这里分频道遍历，但复用同一个 ES 连接）
    def iter_docs():
        for channel in news_channels:
            query = {
                "query": {"bool": {
                    "must": [
                        {"match": {"nopl": channel}},
                        {"range": {"catm": {"gte": f"{month_gte} 00:00:00",
                                            "lte": f"{month_lte} 23:59:59"}}}
                    ]
                }}
            }
            try:
                for src in export_all(es, INDEX, query):
                    yield normalize_item(src)
            except exceptions.AuthenticationException as e:
                print("认证失败：", e)
            except exceptions.AuthorizationException as e:
                print("权限不足：", e)
            except exceptions.ConnectionError as e:
                print("连接失败：", e)
            except Exception as e:
                print("其他错误：", e)

    # 2) 并发调用 AI，加速处理
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_list = [pool.submit(ai.call_api_one, obj) for obj in iter_docs()]
        for fut in as_completed(future_list):
            results.append(fut.result())

    print(f"查询与分析完成，共 {len(results)} 条")
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="新闻解析（保持自定义Excel写入逻辑）")
    parser.add_argument("--month", required=True, help="月份")
    args = parser.parse_args()

    json_results = search_datas(args.month)
    # —— 保持你的写入逻辑不变：
    json_to_excel(json_results, f"{args.month}_news.xlsx")
