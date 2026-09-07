# astock-data-mcp

A 股全维数据 MCP 服务：**45 个工具 / 6 个业务域**，覆盖行情、资金流、涨停打板、龙虎榜、热榜舆情、财报 F10、研报告警、期权、公司行动。数据源全部直连（东方财富 / 新浪 / 腾讯 / 同花顺 / mootdx 通达信 / 巨潮资讯），**零 akshare 依赖**。

> 从 [Athena](https://github.com/JingxuanC) 多 Agent 量化交易系统的 py-sidecar 抽取的 A 股数据子集，独立成仓开源。

## 工具清单（按域）

### market · 行情（12）

| 工具 | 说明 |
|------|------|
| `get_a_realtime` | 实时行情（腾讯），支持批量 `600519,000001` |
| `get_a_hist` | 日 K 线（新浪源），支持 count / 起止日期 |
| `get_a_intraday` | 分时分钟量价 |
| `get_a_indices` | 大盘指数（sh/sz/cy/kc50/hs300/sz50/zz500/bj50） |
| `get_a_stock_info` | 基础信息：行业、总/流通股本、市值、上市日期（东财） |
| `get_a_search` | 按名称/代码搜索股票 |
| `get_a_time_info` | 当前时间 + 最近交易日 |
| `get_a_fund_flow_120d` | 近 120 交易日资金流（主力/超大/大/中/小单） |
| `get_a_fund_flow_minute` | 盘中分钟级资金流（klt=1/5/15/30/60） |
| `get_a_north_flow` | 北向资金日度净流入（沪深港通） |
| `get_a_north_flow_minute` | 北向资金分钟级实时流 |
| `get_a_news` | 个股新闻（东财） |

### sentiment · 情绪与打板（14）

| 工具 | 说明 |
|------|------|
| `get_a_limit_up_pool` | 涨停池：连板数、封板时间、封单额、炸板次数 |
| `get_a_broken_board` | 炸板池：触板未封个股 |
| `get_a_limit_down_pool` | 跌停池 |
| `get_a_yesterday_zt` | 昨日涨停今日表现（晋级率） |
| `get_a_zt_reason` | 涨停揭秘（同花顺）：题材归因、封板率 |
| `get_a_board_emotion` | 打板情绪：炸板率、最高板、连板梯队、涨跌停家数 |
| `get_a_dragon_tiger` | 个股龙虎榜：上榜记录 + 买卖席位 TOP5 + 机构动向 |
| `get_a_daily_dragon_tiger` | 全市场龙虎榜（按日） |
| `get_a_hot_rank` | 同花顺热榜（hour/day）：热度值、概念、排名变动 |
| `get_a_hot_reason` | 强势股题材标签（同花顺编辑精选） |
| `get_a_em_hot_rank` | 东财人气榜 |
| `get_a_hot_concept` | 个股当前热门概念命中（东财） |
| `get_a_industry_rank` | 全行业涨跌幅排名（top/bottom N） |
| `get_a_concept_blocks` | 个股所属板块/概念/地域全集（东财 slist） |

### fundamental · 基本面（5）

| 工具 | 说明 |
|------|------|
| `get_a_financials` | 关键财务指标：PE/PB/市值/EPS（腾讯行情推导） |
| `get_a_financial_statements` | 新浪财报三表（资产负债/利润/现金流量） |
| `get_a_mootdx_finance` | mootdx 季度财务快照（37 字段：EPS/ROE/BVPS/营收/利润…） |
| `get_a_mootdx_f10` | F10 文本（9 类：公司概况/财务分析/股东研究/股本结构…） |
| `get_a_eps_forecast` | 机构一致预期 EPS（同花顺） |

### research · 研报与公告（6）

| 工具 | 说明 |
|------|------|
| `get_a_reports` | 个股机构研报（东财） |
| `get_a_industry_reports` | 行业研报（东财，industry_code='*' 全量） |
| `research_reports` | 批量个股研报（近 30 天，东财） |
| `download_report_pdf` | 按 infoCode 下载研报 PDF |
| `get_a_irm_qa` | 互动易投资者问答（巨潮） |
| `get_a_announcements` | 巨潮公告全文列表（动态 orgId） |

### corporate · 公司行动（5）

| 工具 | 说明 |
|------|------|
| `get_a_dividend` | 分红送转历史 |
| `get_a_holder_num` | 股东户数变化（集中度信号） |
| `get_a_lockup_expiry` | 限售解禁日历（历史 + 未来 90 天） |
| `get_a_margin` | 融资融券明细 |
| `get_a_block_trade` | 大宗交易记录（折溢价） |

### options · 期权（3）

| 工具 | 说明 |
|------|------|
| `get_a_option_codes` | ETF 期权合约代码（510050/510300…，按月份） |
| `get_a_option_tquote` | T 型报价：买卖盘、持仓量、行权价 |
| `get_a_option_greeks` | 希腊字母 + 隐含波动率 IV |

## 快速开始

```bash
pip install -r requirements.txt
```

### 跑法一：整跑（HTTP 数据服务）

```bash
python3 server.py --port 50052
# GET  /health      健康检查
# GET  /tools       全部 45 个工具 schema
# POST /call-tool   {"tool": "get_a_realtime", "arguments": {"symbol": "600519"}}
# POST /mcp         MCP JSON-RPC（initialize / tools/list / tools/call）
```

### 跑法二：分域 MCP（推荐，故障隔离）

```bash
python3 mcp_domain_server.py --domain market    --port 50056
python3 mcp_domain_server.py --domain sentiment --port 50055
python3 mcp_domain_server.py --domain fundamental --port 50054
python3 mcp_domain_server.py --domain research  --port 50057
python3 mcp_domain_server.py --domain corporate --port 50058
python3 mcp_domain_server.py --domain options   --port 50059
```

每个域独立进程/端口，只暴露本域工具；额外端点：`GET /quota`、`GET /queue-stats`、`GET /jobs/<id>`（异步任务，见下）。

## Docker 部署

无需本地 Python 环境，一条命令起服务（默认整跑模式，45 个工具）：

```bash
docker compose up -d        # 构建镜像 + 启动容器（首次构建约 1-3 分钟）
docker compose ps           # 查看状态
docker compose logs -f      # 跟踪日志
```

验证：

```bash
curl http://127.0.0.1:50052/health
curl http://127.0.0.1:50052/tools   # 应返回 45 个工具
curl -X POST http://127.0.0.1:50052/call-tool \
  -H 'Content-Type: application/json' \
  -d '{"tool": "get_a_realtime", "arguments": {"symbol": "sh600519"}}'
```

分域模式：`docker-compose.yml` 里附了 market / sentiment 两个域的注释示例，
取消注释后 `docker compose up -d` 即整跑 + 分域并存（其余四域照抄改
`--domain` 和端口即可）。

license 鉴权（可选）：在 `docker-compose.yml` 中取消注释，把宿主机
`licenses.json` 挂进容器并设置 `MCP_LICENSE_FILE`：

```yaml
environment:
  MCP_LICENSE_FILE: /app/licenses/licenses.json
volumes:
  - ./licenses.json:/app/licenses/licenses.json:ro
```

## MCP 客户端接入

分域服务是 Streamable HTTP MCP（JSON-RPC over POST）。以 market 域为例：

```json
{
  "mcpServers": {
    "astock-market": {
      "url": "http://127.0.0.1:50056/mcp"
    }
  }
}
```

鉴权模式下加请求头 `"X-License-Key": "ak_xxx"`。

## 数据源

| 源 | 用途 |
|----|------|
| 东方财富 | 资金流、北向、研报、龙虎榜、人气榜、公告、新闻、个股信息（datacenter + push2 + reportapi，统一节流防封） |
| 新浪 | 日 K 线、财报三表、ETF 期权 |
| 腾讯 | 实时行情、分时 |
| 同花顺 | 涨停揭秘、热榜、题材归因、EPS 一致预期 |
| mootdx（通达信 TCP） | 财务快照、F10（内置服务器列表 + TCP 探测 fallback） |
| 巨潮资讯 | 公告、互动易问答 |

全部为公开接口直连，无 akshare；东财请求走统一 `em_get`（1s 节流 + 会话复用 + 自动重试）。磁盘缓存默认 300s TTL（`DATA_CACHE_DIR` / `DATA_CACHE_TTL` 可调）。

## License 鉴权（可选）

分域服务支持 license key 鉴权 + 每日额度（`mcp_gateway.py`，零外部依赖）：

```bash
# licenses.json: {"keys": {"ak_xxx": {"name": "张三", "daily_quota": 200, "heavy_quota": 3}}}
export MCP_LICENSE_FILE=/path/to/licenses.json
python3 mcp_domain_server.py --domain market --port 50056
```

不配置 `MCP_LICENSE_FILE` 即为开放模式（本地/内网默认）。用量落盘 `.usage-<domain>.json`（可用 `MCP_USAGE_DIR` 改目录）。重负载工具可经 `ASYNC_TOOLS` 登记为异步队列任务（提交返回 job_id，轮询 `GET /jobs/<id>`）。

## 测试

```bash
pytest test_mcp_gateway.py   # license/额度/队列单测（离线）
```

## 可观察性 / Observability

整跑与分域服务均暴露 `GET /metrics`（Prometheus 文本格式，**无鉴权**，只含工具名级聚合，不泄露 license key）：

| 指标 | 类型 | 说明 |
|------|------|------|
| `mcp_tool_calls_total{tool,status}` | counter | 调用次数；status ∈ `ok` / `error` / `rejected_license` / `rejected_quota` / `queued` |
| `mcp_tool_latency_seconds_sum{tool}` / `mcp_tool_latency_seconds_count{tool}` | counter | 延迟累计/次数，平均延迟 = sum / count |
| `mcp_queue_depth` | gauge | 当前排队任务数（分域服务，含 JobQueue 时） |
| `mcp_queue_jobs_total{status}` | counter | 异步任务完成数（`done` / `error`，分域服务） |
| `mcp_uptime_seconds` | gauge | 进程启动至今秒数 |

Prometheus scrape 配置示例：

```yaml
scrape_configs:
  - job_name: astock-data-mcp
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:50052"]   # 整跑；分域模式换成各域端口 50054-50059
```

## 致谢

抽取自 **Athena** — local-first、AI-native 的多 Agent 量化交易系统（5 LLM Agent 管线协作）。本仓为其 py-sidecar 数据层的 A 股子集。

## License

MIT
