# -*- coding: utf-8 -*-
"""
ES7 获取数据（支持认证）示例
- 基本查询：es.search
- 大量数据：helpers.scan（基于 scroll）

"""
import json
from elasticsearch import Elasticsearch, helpers, exceptions
import re
import calendar
from datetime import date, datetime
import requests
import argparse
from json_excel import json_to_excel

# ES_HOSTS = ["http://172.23.25.96:9200"]  # 可放多个节点
ES_HOSTS = ["http://10.10.25.22:9200"]  # 可放多个节点
# INDEX = "biz_post_list_szr1"
INDEX = "biz_tos_rss_news"
username = "elastic"
password = "easymer@es@2023"
news = [
    "今日新闻网-焦点",
    "今日新闻网-要闻",
    "ETtoday新闻云-即时新闻-热门",
    "ETTODAY新闻云-焦点新闻",
    "自由时报-首页-热门新闻",
    "联合新闻网-要闻",
    "中时新闻网-台湾",
    "东森新闻-热门",
    "三立新闻网-热门新闻板块",
    "TVBS新闻网-热门"
]
# AI
API_KEY = "app-6ZraQ41qXnNwWrmzAqdyj4qX"
URL = "http://10.10.23.34:8660/v1/chat-messages"

_CN_NUM = {
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6,
    "七": 7, "八": 8, "九": 9, "十": 10, "十一": 11, "十二": 12
}

def get_client_basic():
    """用户名/密码认证（推荐配 CA 证书）"""
    return Elasticsearch(
        ES_HOSTS, http_auth=(username, password), verify_certs=False
    )


def export_all(es, index: str, query: dict):
    """
    大批量查询并逐条打印（基于 scroll 的 helpers.scan）

    :param es: Elasticsearch 客户端实例
    :param index: 索引名
    :param query: 查询 DSL（dict 格式）
    """
    count = 0
    datas = []
    for doc in helpers.scan(
            es,
            index=index,
            query=query,
            scroll="2m",  # scroll 游标有效期
            size=1000,  # 每批数量
            request_timeout=120,
            preserve_order=False,
            _source_includes=["catm", "site_name", "nopl", "cntt", "titl"]  # 只返回这两个字段
            # _source_includes=["catm", "page_user_type_chn", "topic_type", "cntt", "cont_source_chn"]  # 只返回这两个字段
    ):
        datas.append(json.dumps(doc["_source"], ensure_ascii=False))
        count += 1
    print(f"查询完成，共 {count} 条")

    return datas




def _parse_month(month_str: str):
    """
    支持 '8月'/'08'/'8'/'一月'/'二月' 等格式，
    返回 (month_gte, month_lte) 形式的日期字符串元组
    """
    m = None
    # 先尝试数字
    m_digits = re.search(r'(\d{1,2})', month_str)
    if m_digits:
        m = int(m_digits.group(1))
    else:
        # 再尝试中文
        for k, v in _CN_NUM.items():
            if k in month_str:
                m = v
                break

    if not m or not (1 <= m <= 12):
        raise ValueError('month 非法，应类似 "8月" / "08" / "8" / "一月"')

    # 计算区间
    year = date.today().year
    first_day = date(year, m, 1)
    last_dom = calendar.monthrange(year, m)[1]  # 当月天数
    cap_to_30 = True
    # 如果 cap_to_30=True，最多到 30 号；否则到当月最后一天
    last_day = date(year, m, min(30 if cap_to_30 else last_dom, last_dom))
    month_gte = first_day.strftime("%Y-%m-%d")
    month_lte = last_day.strftime("%Y-%m-%d")
    return month_gte, month_lte





def change(es_result: list) -> list:
    # 网站映射表
    site_map = {
        "今日新闻网": {"domain": "www.nownews.com", "color": "蓝营"},
        "ETtoday新闻云": {"domain": "www.ettoday.net", "color": "绿营"},
        "自由时报": {"domain": "news.ltn.com.tw", "color": "绿营"},
        "联合新闻网": {"domain": "udn.com", "color": "蓝营"},
        "中时电子报": {"domain": "www.chinatimes.com", "color": "蓝营"},
        "东森新闻": {"domain": "news.ebc.net.tw", "color": "蓝营"},
        "三立新闻网": {"domain": "www.setn.com", "color": "绿营"},
        "TVBS新闻网": {"domain": "news.tvbs.com.tw", "color": "蓝营"},
    }
    datas = []
    for item_str in es_result:
        # 把 JSON 字符串转成字典
        item = json.loads(item_str)

        # 添加 domain / color
        site_name = item.get("site_name")
        if site_name in site_map:
            item["domain"] = site_map[site_name]["domain"]
            item["color"] = site_map[site_name]["color"]
        else:
            item["domain"] = None
            item["color"] = None

        # 转换时间格式
        catm_val = item.get("catm")
        if catm_val:
            try:
                dt = datetime.strptime(catm_val, "%Y-%m-%d %H:%M:%S")
                item["catm"] = dt.strftime("%Y-%m-%d")
            except ValueError:
                # 格式不匹配时保留原值
                pass

        datas.append(json.dumps(item, ensure_ascii=False))
    return datas


def search_datas(month: str) -> list:
    month_gte, month_lte = _parse_month(month)
    results = []
    for new in news:
        query = {
            "query": {"bool": {
                "must": [
                    {"match": {"nopl": f"{new}"}},
                    # {"match": {"topic_type": "用户分享"}},
                    {
                        "range": {
                            "catm": {
                                "gte": f"{month_gte} 00:00:00",
                                "lte": f"{month_lte} 23:59:59"
                            }
                        }
                    }
                ]
            }
            }
        }
        try:
            es = get_client_basic()
            datas = change(export_all(es, INDEX, query))
            # 数据经过ai agent分析
            for data in datas:
                print(data)
                results.append(call_api(data))
        except exceptions.AuthenticationException as e:
            print("认证失败：", e)
        except exceptions.AuthorizationException as e:
            print("权限不足：", e)
        except exceptions.ConnectionError as e:
            print("连接失败：", e)
        except Exception as e:
            print("其他错误：", e)
    return results




def call_api(new_str: str) -> str:
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "inputs": {
            "json": f"{new_str}"
        },
        "query": "hello,my name is bill!",
        "response_mode": "blocking",
        "conversation_id": "",
        "user": "admin"
    }
    try:
        response = requests.post(URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()  # 如果不是 2xx 会抛异常
        # 返回 JSON 格式数据
        response_data = response.json()
        return response_data.get("answer")
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="新闻解析")
    parser.add_argument("--month", required=True, help="月份")
    args = parser.parse_args()

    results = search_datas(args.month)
    # print(results)
    json_to_excel(results, f"{args.month}_news.xlsx")
