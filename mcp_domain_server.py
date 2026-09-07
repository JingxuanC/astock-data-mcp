#!/usr/bin/env python3
"""通用域 MCP 启动器 — 按业务域把 A 股数据工具暴露为独立 MCP 服务。

用法:
    python3 mcp_domain_server.py --domain market       --port 50056
    python3 mcp_domain_server.py --domain sentiment    --port 50055
    python3 mcp_domain_server.py --domain fundamental  --port 50054
    python3 mcp_domain_server.py --domain research     --port 50057
    python3 mcp_domain_server.py --domain corporate    --port 50058
    python3 mcp_domain_server.py --domain options      --port 50059

每个域独立进程/端口，故障隔离：一个域挂了不影响其他。

端点:
    GET  /health        健康检查
    GET  /tools         本域工具列表
    POST /mcp           本域 MCP JSON-RPC（initialize / tools/list / tools/call）
    POST /mcp/<domain>  兼容路径（如 /mcp/factor）
    GET  /jobs/<id>     异步任务状态/结果（重负载工具走队列，见 ASYNC_TOOLS）
    GET  /quota         当前 license key 的额度余量（鉴权模式）
    GET  /queue-stats   队列概况
    GET  /metrics       Prometheus 指标（无鉴权）

鉴权与额度（mcp_gateway.py）：
    环境变量 MCP_LICENSE_FILE 指向 license JSON 时强制鉴权
    （请求头 X-License-Key）；未配置 = 开放模式（本地/内网行为不变）。
    重负载工具提交即入队返回 job_id，客户端轮询 /jobs/<id> 拿结果。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# 复用 sidecar 的工具注册表（HANDLERS: name→callable, TOOLS: name→ToolDef）
from server import HANDLERS, TOOLS  # noqa: F401  — 副作用：注册全部工具

from mcp_gateway import METRICS, JobQueue, LicenseStore, QueueFull, QuotaExceeded

logger = logging.getLogger("domain-mcp")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

# ── 域 → 工具集映射（45 个 A 股工具按业务聚类）──
DOMAIN_TOOLS: dict[str, set[str]] = {
    "market": {
        "get_a_realtime", "get_a_hist", "get_a_intraday", "get_a_indices",
        "get_a_stock_info", "get_a_search", "get_a_time_info",
        "get_a_fund_flow_120d", "get_a_fund_flow_minute",
        "get_a_north_flow", "get_a_north_flow_minute",
        "get_a_news",
    },
    "fundamental": {
        "get_a_financials", "get_a_financial_statements", "get_a_mootdx_finance",
        "get_a_mootdx_f10", "get_a_eps_forecast",
    },
    "research": {
        "get_a_reports", "get_a_industry_reports", "research_reports",
        "download_report_pdf", "get_a_irm_qa", "get_a_announcements",
    },
    "sentiment": {
        "get_a_board_emotion", "get_a_broken_board",
        "get_a_limit_up_pool", "get_a_limit_down_pool",
        "get_a_zt_reason", "get_a_yesterday_zt",
        "get_a_dragon_tiger", "get_a_daily_dragon_tiger",
        "get_a_hot_rank", "get_a_hot_reason", "get_a_em_hot_rank",
        "get_a_hot_concept", "get_a_industry_rank", "get_a_concept_blocks",
    },
    "options": {
        "get_a_option_codes", "get_a_option_greeks", "get_a_option_tquote",
    },
    "corporate": {
        "get_a_dividend", "get_a_holder_num", "get_a_lockup_expiry",
        "get_a_margin", "get_a_block_trade",
    },
}

# 重负载工具：提交后入异步队列执行（返回 job_id 轮询），不占 HTTP 连接。
# 不在清单里的工具维持同步调用。队列/额度语义见 mcp_gateway.py。
# A 股数据工具均为轻量 HTTP 抓取，默认全部同步；如需异步化在此登记。
ASYNC_TOOLS: dict[str, set[str]] = {}


class DomainHandler(BaseHTTPRequestHandler):
    """按域过滤工具的最小 MCP handler。域通过类属性注入（main 里设置）。"""

    domain = "market"
    server_name = "astock-market-mcp"
    license_store: LicenseStore | None = None
    job_queue: JobQueue | None = None

    def log_message(self, fmt, *args):
        logger.debug("HTTP %s", fmt % args)

    @property
    def domain_tools(self):
        return DOMAIN_TOOLS[self.domain]

    def _send(self, code: int, obj: dict):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _tool_schemas(self):
        schemas = [t.to_dict() for t in TOOLS.values() if t.name in self.domain_tools]
        # compute_factors / predict 是硬编码扩展工具（不在 TOOLS 注册表）
        if "compute_factors" in self.domain_tools:
            schemas.append({"name": "compute_factors", "description": "Compute Alpha158 factors from OHLCV data",
                            "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "klines": {"type": "array"}}}})
        if "predict" in self.domain_tools:
            schemas.append({"name": "predict", "description": "ML model prediction from factor values",
                            "inputSchema": {"type": "object", "properties": {"symbol": {"type": "string"}, "factors": {"type": "object"}}}})
        return schemas

    def _license_key(self) -> str:
        return self.headers.get("X-License-Key", "")

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok", "version": "1.0.0", "mode": f"{self.domain}-mcp",
                             "auth": bool(self.license_store and self.license_store.enabled)})
        elif self.path == "/tools":
            self._send(200, {"tools": self._tool_schemas()})
        elif self.path == "/metrics":
            # Prometheus 抓取端点：不要求鉴权（只含工具名级聚合，不泄露 key）
            depth = self.job_queue.stats()["queue_size"] if self.job_queue else None
            body = METRICS.render(queue_depth=depth).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/quota":
            if not (self.license_store and self.license_store.enabled):
                self._send(200, {"mode": "open"})
                return
            ok, info = self.license_store.check(self._license_key())
            if not ok:
                self._send(401, {"error": info})
                return
            self._send(200, self.license_store.quota_of(self._license_key()))
        elif self.path == "/queue-stats":
            self._send(200, self.job_queue.stats() if self.job_queue else {})
        elif self.path.startswith("/jobs/"):
            if not self.job_queue:
                self._send(404, {"error": "queue disabled"})
                return
            job = self.job_queue.get(self.path[len("/jobs/"):], key=self._license_key())
            if job is None:
                self._send(404, {"error": "job not found"})
                return
            self._send(200, job)
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(length) or b"{}")
        except Exception as e:  # noqa: BLE001
            self._send(400, {"error": f"invalid JSON: {e}"})
            return
        if self.path == "/mcp" or self.path == f"/mcp/{self.domain}":
            self._handle_mcp(data)
        else:
            self._send(404, {"error": "not found"})

    def _handle_mcp(self, data):
        mid = data.get("id")
        method = data.get("method", "")
        params = data.get("params") or {}

        if method == "initialize":
            import uuid as _uuid
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Mcp-Session-Id", str(_uuid.uuid4()))
            self.end_headers()
            self.wfile.write(json.dumps({
                "jsonrpc": "2.0", "id": mid,
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": self.server_name, "version": "1.0.0"},
                },
            }).encode())
            return

        if method == "notifications/initialized":
            self._send(200, {"jsonrpc": "2.0", "id": mid, "result": {}})
            return

        if method == "tools/list":
            self._send(200, {"jsonrpc": "2.0", "id": mid,
                             "result": {"tools": self._tool_schemas()}})
            return

        if method == "tools/call":
            tool_name = params.get("name", "")
            tool_args = params.get("arguments", {})
            # 鉴权：license 模式强制校验 key（initialize/tools/list 保持开放便于发现）
            store = self.license_store
            key = self._license_key()
            if store and store.enabled:
                ok, info = store.check(key)
                if not ok:
                    METRICS.inc_call(tool_name, "rejected_license")
                    self._send(200, {"jsonrpc": "2.0", "id": mid,
                                     "error": {"code": -32001, "message": info}})
                    return
            if tool_name not in self.domain_tools or tool_name not in HANDLERS:
                self._send(200, {"jsonrpc": "2.0", "id": mid,
                                 "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}})
                return
            is_async = tool_name in ASYNC_TOOLS.get(self.domain, set()) and self.job_queue
            # 额度：先扣再跑（异步任务失败不退还——成本已发生）
            if store and store.enabled:
                try:
                    store.consume(key, heavy=bool(is_async))
                except QuotaExceeded as e:
                    METRICS.inc_call(tool_name, "rejected_quota")
                    self._send(200, {"jsonrpc": "2.0", "id": mid,
                                     "error": {"code": -32029, "message": str(e)}})
                    return
            # 重负载 → 入队异步执行，返回 job_id 供轮询
            if is_async:
                try:
                    job_id = self.job_queue.submit(tool_name, tool_args, key=key)
                except QueueFull as e:
                    self._send(200, {"jsonrpc": "2.0", "id": mid,
                                     "error": {"code": -32029, "message": str(e)}})
                    return
                # 入队成功记 queued；执行结果（ok/error + latency）由 worker 完成时记
                METRICS.inc_call(tool_name, "queued")
                self._send(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": json.dumps({
                        "job_id": job_id, "status": "queued",
                        "poll": f"/jobs/{job_id}",
                        "note": "重任务已入队，轮询 GET /jobs/<id> 拿结果",
                    }, ensure_ascii=False)}], "isError": False}})
                return
            t0 = time.time()
            try:
                result = HANDLERS[tool_name](**tool_args)
                METRICS.inc_call(tool_name, "ok")
                self._send(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": str(result)}], "isError": False}})
            except Exception as e:  # noqa: BLE001
                METRICS.inc_call(tool_name, "error")
                logger.error("tool call error %s: %s", tool_name, e)
                self._send(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": f"Error: {e}"}], "isError": True}})
            finally:
                METRICS.observe_latency(tool_name, time.time() - t0)
            return

        self._send(200, {"jsonrpc": "2.0", "id": mid,
                         "error": {"code": -32601, "message": f"Unknown method: {method}"}})


def main():
    ap = argparse.ArgumentParser(description="按域暴露 A 股数据工具的独立 MCP 服务")
    ap.add_argument("--domain", required=True, choices=sorted(DOMAIN_TOOLS), help="业务域")
    ap.add_argument("--host", default="127.0.0.1", help="监听地址（默认 127.0.0.1）")
    ap.add_argument("--port", type=int, default=50054, help="监听端口")
    ap.add_argument("--license-file", default=os.environ.get("MCP_LICENSE_FILE", ""),
                    help="license key JSON 路径（env MCP_LICENSE_FILE）；不配置=开放模式")
    ap.add_argument("--workers", type=int, default=int(os.environ.get("MCP_WORKERS", "2")),
                    help="异步任务 worker 数（env MCP_WORKERS，默认 2）")
    ap.add_argument("--queue-size", type=int, default=int(os.environ.get("MCP_QUEUE_SIZE", "50")),
                    help="异步队列上限（env MCP_QUEUE_SIZE，默认 50）")
    args = ap.parse_args()

    DomainHandler.domain = args.domain
    DomainHandler.server_name = f"astock-{args.domain}-mcp"
    DomainHandler.license_store = LicenseStore(args.license_file, domain=args.domain)
    DomainHandler.job_queue = JobQueue(HANDLERS, workers=args.workers, maxsize=args.queue_size)

    server = ThreadingHTTPServer((args.host, args.port), DomainHandler)
    logger.info("%s MCP listening on %s:%d (tools=%d, auth=%s, workers=%d)",
                args.domain, args.host, args.port, len(DOMAIN_TOOLS[args.domain]),
                "on" if DomainHandler.license_store.enabled else "open", args.workers)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("shutting down")
        DomainHandler.job_queue.shutdown()
        server.shutdown()


if __name__ == "__main__":
    main()
