总体目标
按给定月份，从 ES 的多条“频道”里批量拉新闻 → 做字段归一与去重 → 并发调用你本地的 AI 工作流处理 → 把结果按原有 json_to_excel 逻辑写成 Excel；同时确保过程稳态（限速、在途上限、重试、慢请求告警、进度日志）。

模块拆解
日志与配置

统一 logging；可切 INFO/DEBUG。

ES 连接参数、索引名、账号密码、频道列表。

AI 服务 URL、API Key。

并发与稳态参数：线程数 MAX_WORKERS、在途上限 MAX_INFLIGHT、全局 MAX_QPS、超时、自动/手动重试、退避系数、慢请求阈值等。

去重配置：DEDUP_KEYS = ["site_name", "titl"]（可改为 ["titl"]）。

工具与数据处理

_parse_month(month_str)：把“8 / 8月 / 八月”等解析为当年 YYYY-MM-01 到 YYYY-MM-30 的区间（上限 cap 到 30）。

SITE_MAP：把站点名映射出 domain 和政治色彩 color。

normalize_item(item)：补全映射字段；把 catm 从“到秒”统一为 YYYY-MM-DD（解析失败就保持原样）。

去重辅助

_norm_text_for_dedup(s)：NFKC 归一、去空白/重复空格、小写、去常见中英标点。

make_dedup_key(doc)：按 DEDUP_KEYS 取字段 → 规范化 → 拼接 → sha1，得到稳定的去重键。

限速与在途控制

RateLimiter：滑动窗口，确保全局 QPS 不超过 MAX_QPS。

inflight_sema：信号量限制同时在途请求数不超过 MAX_INFLIGHT。

lat_samples：收集最近请求耗时，计算近似 P90；若高于读取超时的 50%，就“自适应放缓”短暂 sleep。

AI 客户端（并发 + 重试 + 退避）

复用 requests.Session，挂 HTTPAdapter，用 urllib3.Retry 做自动重试（429/5xx）；状态码在 STATUS_FORCELIST 里会指数退避。

外层再做 手动重试（MANUAL_RETRIES 次）。

每次请求前过 QPS 限流 & 在途上限；请求后记录耗时、慢请求提醒（> SLOW_REQ_THRESHOLD 秒）。

返回值：若 AI 输出是 JSON 字符串则直回；否则把原对象与 ai_result 封装回 JSON，避免全流程崩溃。

ES 读取（scan 流式）

export_all 用 helpers.scan 按查询条件流式拉 _source，包含你关心的字段。

每个频道单独查询，按月份的 catm 范围。对每个频道计数、计时与异常捕获（鉴权/连接/权限等）。

Python 层去重（核心差异点）

维护 best_by_key: key -> doc 字典：

生成 key = make_dedup_key(item)；

若新 key 首次出现，直接收录；

若重复：按时间比较（优先解析 YYYY-MM-DD HH:MM:SS，其次 YYYY-MM-DD），保留更晚的一条。

最后把 best_by_key.values() 展开为去重后的 docs，记录“原始条数 / 去重后条数 / 剔除重复数”。

并发 AI 调用

用 ThreadPoolExecutor(max_workers=MAX_WORKERS) 并发处理去重后的 docs。

处理期间按 PROGRESS_EVERY 打印进度（完成数、百分比）。

汇总 AI 返回的 JSON 字符串到 results。

写 Excel

调用你已有的 json_to_excel(results, outfile)，把所有 AI 处理结果落盘为 "{month}_news.xlsx"，并打印写入耗时。

命令行入口

--month 必需；驱动上述流程。

主流程一图流