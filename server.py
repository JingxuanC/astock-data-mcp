"""
astock-data-mcp — A股全维数据 MCP 服务（行情/资金流/打板/龙虎榜/财报/研报/期权）

从 Athena 项目 py-sidecar 抽取的 A 股数据子集，45 个工具 / 6 个业务域：
  - market      行情: 实时/K线/分时/指数/搜股/资金流/北向/个股新闻
  - sentiment   情绪: 涨停池/炸板/跌停/昨涨停/涨停揭秘/打板情绪/龙虎榜/热榜/行业排名/板块归属
  - fundamental 基本面: 关键指标/新浪三表/mootdx 财务/F10/EPS 一致预期
  - research    研报: 个股研报/行业研报/研报PDF下载/互动易问答/巨潮公告
  - corporate   公司行动: 分红送转/股东户数/限售解禁/融资融券/大宗交易
  - options     期权: ETF 期权合约/T型报价/希腊字母+IV

数据源：东方财富 / 新浪 / 腾讯 / 同花顺 / mootdx(通达信) / 巨潮资讯 —— 全部直连，零 akshare。

两种跑法：
  python3 server.py --port 50052                              # 整跑：HTTP 数据服务 (/tools /call-tool /mcp)
  python3 mcp_domain_server.py --domain market --port 50056   # 分域 MCP（六域独立进程）
"""

import inspect
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
try:
    from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
except ImportError:
    # Python 3.6 兼容 (ThreadingHTTPServer 是 3.7+)
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from socketserver import ThreadingMixIn

    class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
        pass

from pathlib import Path
from typing import Any, Optional

from research_report import fetch_research_reports
from mcp_gateway import METRICS, LicenseStore, QuotaExceeded

# ── 公共原语：mcp-common 是唯一真源 ─────────────────────────────
# 本文件在仓库里的 mcp_common.py 副本由 mcp-common/sync.py 生成，
# 请勿直接编辑；要改 fail()/类型强制/代码日期归一化，改 canonical 后重新 sync。
from mcp_common import (
    bare_code,
    coerce_args,
    date_arg,
    date_fail,
    eastmoney_secid,
    fail,
    is_empty_result as _is_empty_result,
    normalize_cn_symbol as normalize_symbol,
    normalize_date,
    symbol_fail,
    SYMBOL_HINT,
    tencent_symbol,
)


def coerce_tool_args(tool_name, args):
    """调用 handler 前做轻量类型强制（绑定本服务注册表）。→ (args, None) 或 (None, fail_json)。"""
    return coerce_args(HANDLERS, tool_name, args)


# ── Lazy imports for A-share extensions ──
_requests = None
_pd = None
_mootdx_quotes = None
_mootdx_quotes_loaded = False

def _get_requests():
    global _requests
    if _requests is None:
        import requests as _r
        _requests = _r
    return _requests

def _get_mootdx():
    global _mootdx_quotes, _mootdx_quotes_loaded
    if not _mootdx_quotes_loaded:
        try:
            from mootdx.quotes import Quotes
            _mootdx_quotes = Quotes
        except ImportError:
            _mootdx_quotes = None
        _mootdx_quotes_loaded = True
    return _mootdx_quotes

# ── Eastmoney anti-blocking: global throttle + session reuse ──
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_EM_SESSION = None
_EM_MIN_INTERVAL = 1.0
_em_last_call = [0.0]

def _get_em_session():
    global _EM_SESSION
    if _EM_SESSION is None:
        import requests as _r
        _EM_SESSION = _r.Session()
        _EM_SESSION.headers.update({"User-Agent": _UA})
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            _adapter = HTTPAdapter(max_retries=Retry(
                total=3, connect=3, backoff_factor=0.6,
                status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"]))
            _EM_SESSION.mount("https://", _adapter)
            _EM_SESSION.mount("http://", _adapter)
        except Exception:
            pass
    return _EM_SESSION

def em_get(url: str, params: dict = None, headers: dict = None, timeout: int = 15,
           method: str = "GET", **kwargs):
    """Eastmoney unified request: auto throttle + session reuse + default UA.
    All eastmoney.com APIs must go through this to avoid IP ban.
    method: "GET" (default) or "POST"."""
    import time as _time
    import random as _random
    wait = _EM_MIN_INTERVAL - (_time.time() - _em_last_call[0])
    if wait > 0:
        _time.sleep(wait + _random.uniform(0.1, 0.5))
    try:
        if method.upper() == "POST":
            return _get_em_session().post(url, params=params, headers=headers,
                                          timeout=timeout, **kwargs)
        return _get_em_session().get(url, params=params, headers=headers, timeout=timeout, **kwargs)
    finally:
        _em_last_call[0] = _time.time()

_DATACENTER_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

def eastmoney_datacenter(report_name: str, columns: str = "ALL",
                          filter_str: str = "", page_size: int = 50,
                          sort_columns: str = "", sort_types: str = "-1") -> list:
    """Eastmoney datacenter unified query — dragon_tiger/lockup/margin/block_trade/
    shareholder/dividend all share this endpoint (built-in throttle)."""
    params = {
        "reportName": report_name, "columns": columns,
        "filter": filter_str, "pageNumber": "1", "pageSize": str(page_size),
        "sortColumns": sort_columns, "sortTypes": sort_types,
        "source": "WEB", "client": "WEB",
    }
    r = em_get(_DATACENTER_URL, params=params, timeout=15)
    d = r.json()
    if d.get("result") and d["result"].get("data"):
        return d["result"]["data"]
    return []

# ── mootdx TCP probing client ──
_TDX_SERVERS = [
    ('119.97.185.59', 7709), ('124.70.133.119', 7709), ('116.205.183.150', 7709),
    ('123.60.73.44', 7709),  ('116.205.163.254', 7709), ('121.36.225.169', 7709),
    ('123.60.70.228', 7709), ('124.71.9.153', 7709),    ('110.41.147.114', 7709),
    ('124.71.187.122', 7709),
]

def _probe(ip, port, timeout=2.0):
    import socket as _socket
    try:
        with _socket.create_connection((ip, port), timeout=timeout):
            return True
    except Exception:
        return False

def tdx_client(market='std'):
    """Create mootdx client with TCP probing + 3-level fallback to avoid 0.11.x BESTIP bug."""
    Quotes = _get_mootdx()
    if Quotes is None:
        raise RuntimeError("mootdx not installed. Run: pip install mootdx")
    for ip, port in _TDX_SERVERS:
        if _probe(ip, port):
            return Quotes.factory(market=market, server=(ip, port))
    try:
        return Quotes.factory(market=market, bestip=True)
    except Exception:
        pass
    try:
        return Quotes.factory(market=market)
    except Exception as e:
        raise RuntimeError(
            "All mootdx servers unreachable. Overseas IPs usually timeout on TCP 7709. "
            "Use domestic proxy or update _TDX_SERVERS list. Original error: %s" % e
        )

def _get_pd():
    global _pd
    if _pd is None:
        import pandas as pd
        _pd = pd
    return _pd

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(name)s] %(levelname)s %(message)s")
logger = logging.getLogger("mcp-data")

# ── Cache ──
CACHE_DIR = Path(os.environ.get("DATA_CACHE_DIR", Path(__file__).parent / ".data_cache"))
CACHE_TTL = int(os.environ.get("DATA_CACHE_TTL", "300"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _cache_path(tool_name: str, *args) -> Path:
    key = tool_name + "_" + "_".join(str(a).replace("/", "_")[:40] for a in args)
    return CACHE_DIR / f"{key}.json"


def _cache_get(tool_name: str, *args) -> Optional[Any]:
    p = _cache_path(tool_name, *args)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        if time.time() - data.get("ts", 0) < CACHE_TTL:
            return data.get("value")
    except (json.JSONDecodeError, KeyError):
        pass
    return None


def _cache_set(tool_name: str, value: Any, *args) -> None:
    # 空结果不缓存（调用点也各自加了守卫，这里是最后一道防线）
    if _is_empty_result(value):
        logger.debug("skip caching empty result: %s", tool_name)
        return
    p = _cache_path(tool_name, *args)
    tmp = p.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps({"ts": time.time(), "value": value}))
        tmp.replace(p)  # 原子换名，读者不会看到写了一半的文件
    except OSError:
        pass


# ── Toolkit interface ──
class ToolDef:
    def __init__(self, name: str, description: str, inputSchema: dict):
        self.name = name
        self.description = description
        self.inputSchema = inputSchema
    def to_dict(self):
        return {"name": self.name, "description": self.description, "inputSchema": self.inputSchema}


TOOLS: dict[str, ToolDef] = {}
HANDLERS: dict[str, callable] = {}


def tool(name: str, description: str, properties: dict, required: Optional[list] = None):
    """Decorator to register a tool."""
    def deco(fn):
        TOOLS[name] = ToolDef(name, description, {
            "type": "object",
            "properties": properties,
            # 只有显式声明 required 的参数才进必需列表；缺省 = 全部可选
            # （旧实现把 properties 的全部键都当必填，可选参数被系统性错标：
            #   实测 45 个工具里 27 个工具、33 个可选参数被标成 required）
            "required": required or [],
        })
        HANDLERS[name] = fn
        return fn
    return deco


# ═══════════════════════════════════════════════════════════════
# 14-16. A-share data (Tencent + Sina APIs, zero extra deps)
# ═══════════════════════════════════════════════════════════════

def _tencent_fetch(symbols: list[str]) -> dict[str, dict]:
    import urllib.request
    codes = ",".join(symbols)
    url = f"http://qt.gtimg.cn/q={codes}"
    req = urllib.request.Request(url, headers={"User-Agent": "athena-mcp/1.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read().decode("gbk", errors="replace")
    results = {}
    for line in raw.strip().split("\n"):
        if not line.strip():
            continue
        try:
            parts = line.split("~")
            # parts[0] has the full key like "v_s_sh000001" — extract code from there
            raw_code = parts[2]
            # For index symbols (start with "s_"), preserve the marker + sh/sz prefix
            full_key = line.split("=")[0].strip()  # e.g. "v_s_sh000001"
            if "_s_" in full_key:
                # Index: reconstruct as "s_sh000001"
                idx = full_key.index("_s_")
                code = full_key[idx+1:]  # "s_sh000001"
            else:
                code = raw_code
            results[code] = {
                "symbol": code, "name": parts[1],
                "price": float(parts[3]), "change": float(parts[31]),
                "pct_change": float(parts[32]), "open": float(parts[5]),
                "high": float(parts[33]), "low": float(parts[34]),
                "prev_close": float(parts[4]),
                "volume": int(float(parts[6])) * 100,
                "amount": float(parts[37]) * 10000,
                "turnover": float(parts[38]),
                "pe": float(parts[39]) if parts[39] else 0,
                "market_cap": float(parts[44]) * 1e8,
                "pb": float(parts[46]) if parts[46] else 0,
            }
        except (IndexError, ValueError):
            continue
    return results


def _sina_fetch_klines(symbol: str, count: int) -> list[dict]:
    import urllib.request, json as _json
    # 新浪只认 sh/sz 前缀，且只支持沪深；统一走 normalize_symbol，不再自己判号段
    sym = tencent_symbol(symbol)
    if sym is None:
        raise ValueError("无法识别股票代码 '%s'（%s）" % (symbol, SYMBOL_HINT))
    symbol = sym
    url = ("http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen={count}")
    req = urllib.request.Request(url, headers={"User-Agent": "athena-mcp/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = _json.loads(resp.read().decode("utf-8"))
    return [{"date": r["day"], "open": float(r["open"]), "high": float(r["high"]),
             "low": float(r["low"]), "close": float(r["close"]),
             "volume": int(float(r["volume"]))} for r in data] if isinstance(data, list) else []


@tool("get_a_realtime", "A-share realtime quotes (Tencent). Batch: '600519,000001'. "
      "Code formats accepted: 600519 / sh600519 / SH600519 / 600519.SH. "
      "Returns per stock: symbol, name, price, change, pct_change, open, high, low, "
      "prev_close, volume(股), amount(元), turnover(%), pe, pb, market_cap(元).",
      {"symbol": {"type": "string", "description": "Code or comma-separated codes"}}, required=['symbol'])
def get_a_realtime(symbol: str) -> str:
    import json as _json
    syms = [s.strip() for s in str(symbol).split(",") if s.strip()]
    if not syms:
        return fail("symbol 不能为空", code="invalid_symbol", hint=SYMBOL_HINT)
    full = []
    for s in syms:
        norm = normalize_symbol(s)
        if norm is None:
            return symbol_fail(s)
        full.append(norm[0] + norm[1])
    try:
        data = _tencent_fetch(full)
        if not data:
            # 格式合法但上游无数据：明确区分于"代码写错"，不让 LLM 误判成没行情
            return fail("未获取到 %s 的行情数据" % ",".join(syms), code="no_data",
                        hint="代码可能不存在/已退市/停牌；请先用 get_a_search 确认代码")
        return _json.dumps(list(data.values()), ensure_ascii=False)
    except Exception as e:
        return fail(e)


@tool("get_a_hist", "A-share daily K-line (Sina source). Code formats accepted: "
      "600519 / sh600519 / SH600519 / 600519.SH. start/end accept "
      "2026-01-01 / 20260101 / 2026/01/01. "
      "Returns array of bars: date, open, high, low, close, volume(股).",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "count": {"type": "integer", "description": "Number of bars (default 100)"},
       "start": {"type": "string", "description": "Start date YYYY-MM-DD (optional, range query)"},
       "end": {"type": "string", "description": "End date YYYY-MM-DD (optional, range query)"}}, required=['symbol'])
def get_a_hist(symbol: str, count: int = 100, start: str = None, end: str = None) -> str:
    import json as _json
    t_sym = tencent_symbol(symbol)
    if t_sym is None:
        return symbol_fail(symbol)
    start, _serr = date_arg(start)
    if _serr:
        return _serr
    end, _eerr = date_arg(end)
    if _eerr:
        return _eerr
    cached = _cache_get("get_a_hist", t_sym, str(count), start or "", end or "")
    if cached:
        return cached
    try:
        if start or end:
            # Range query: fetch the max Sina window, then filter by [start, end].
            data = _sina_fetch_klines(t_sym, 1023)
            s, e = (start or "")[:10], (end or "")[:10]
            data = [d for d in data
                    if (not s or d["date"][:10] >= s) and (not e or d["date"][:10] <= e)]
        else:
            data = _sina_fetch_klines(t_sym, count)
        result = _json.dumps(data, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_hist", result, t_sym, str(count), start or "", end or "")
        return result
    except Exception as e:
        msg = str(e)
        if "HTTP Error" in msg or "456" in msg:
            return fail("K 线上游接口不可用（%s，代码 %s）" % (msg, t_sym), code="upstream_error",
                        hint="新浪 K 线接口限流/被墙；可稍后重试，或先用 get_a_realtime / get_a_intraday")
        return fail(e)


@tool("get_a_time_info", "Current time and last A-share trading day.",
      {"market": {"type": "string", "description": "Market: 'cn' (default)"}})
def get_a_time_info(market: str = "cn") -> str:
    import json as _json
    now = datetime.now().astimezone()
    today = now.date()
    wd = today.weekday()
    last_trade = today if wd < 5 else today - timedelta(days=wd - 4)
    return _json.dumps({"iso_format": now.isoformat(), "timestamp": now.timestamp(),
                        "last_trading_day": last_trade.strftime("%Y-%m-%d"), "market": market})


# ═══════════════════════════════════════════════════════════════
# 17. A-share market indices (Tencent)
# ═══════════════════════════════════════════════════════════════

_A_INDEX_CODES = {
    "sh": "sh000001", "sz": "sz399001", "cy": "sz399006",
    "kc50": "sh000688", "hs300": "sh000300", "sz50": "sh000016",
    "zz500": "sh000905", "bj50": "bj899050",
}
# Reverse: returned numeric code → alias
_A_INDEX_REVERSE = {
    "000001": "sh", "399001": "sz", "399006": "cy",
    "000688": "kc50", "000300": "hs300", "000016": "sz50",
    "000905": "zz500", "899050": "bj50",
}

@tool("get_a_indices", "Major A-share index quotes. Use aliases: sh/sz/cy/kc50/hs300/sz50/zz500/bj50. "
      "Returns per index: alias, symbol, name, price, change, pct_change, open, high, low, volume, amount.",
      {"indices": {"type": "string",
                    "description": "Comma-separated index aliases (e.g. 'sh,sz,cy,hs300')"}})
def get_a_indices(indices: str = "sh,sz,cy,hs300") -> str:
    import json as _json
    aliases = [a.strip() for a in indices.split(",") if a.strip()]
    codes = [_A_INDEX_CODES.get(a, a) for a in aliases]
    try:
        data = _tencent_fetch(codes)
        result = []
        for code, d in data.items():
            # Index codes come back as bare numbers (000001 etc) — use reverse map
            d["alias"] = _A_INDEX_REVERSE.get(code, code)
            result.append(d)
        return _json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════════════════════
# 18. Northbound flow (eastmoney push2)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_north_flow", "Northbound capital flow (沪深港通). Returns daily net inflow in CNY. "
      "Returns array (latest snapshot): date, north_sh(沪股通累计净买入,亿元), "
      "north_sz(深股通累计净买入,亿元), north_total(合计,亿元).",
      {"days": {"type": "integer", "description": "Days to look back (default 5, max 60)"}})
def get_a_north_flow(days: int = 5) -> str:
    import json as _json
    days = min(max(days, 1), 60)
    try:
        # 同花顺北向资金 API (EastMoney push2 从服务器不可达, 改用同花顺零鉴权接口)
        # data.hexin.cn 返回当日实时分钟流向: time[], hgt[], sgt[]
        # hgt = 沪股通累计净买入(亿元), sgt = 深股通累计净买入(亿元)
        req = _get_requests().get(
            "https://data.hexin.cn/market/hsgtApi/method/dayChart/",
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36",
                      "Referer": "https://data.hexin.cn/"},
            timeout=10)
        raw = req.json()
        times = raw.get("time") or []
        hgt = raw.get("hgt") or []
        sgt = raw.get("sgt") or []
        # Use latest values as today's net flow
        north_sh = hgt[-1] if hgt else 0
        north_sz = sgt[-1] if sgt else 0
        result = [{
            "date": times[-1] if times else "",
            "north_sh": round(north_sh, 2),
            "north_sz": round(north_sz, 2),
            "north_total": round(north_sh + north_sz, 2),
        }]
        return _json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════════════════════
# 19. Stock search (Tencent smartbox)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_search", "Search A-share stocks by name or code. Returns matching tickers. "
      "Returns array: code, name, market(SH/SZ/BJ), type(如 GP-A / GP-A-KCB).",
      {"query": {"type": "string", "description": "Stock name or code (e.g. '茅台', '600519')"}}, required=['query'])
def get_a_search(query: str) -> str:
    import json as _json, urllib.request, urllib.parse
    try:
        qs = urllib.parse.urlencode({"t": "all", "q": query, "c": "1"})
        url = f"http://smartbox.gtimg.cn/s3/?{qs}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; athena-mcp/1.0)",
        })
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("gbk", errors="replace")
        # 上游格式：v_hint="sh~600519~贵州茅台~gzmt~GP-A"
        #   f0=市场(sh/sz) f1=代码 f2=名称 f3=拼音 f4=类型(GP-A 主板A股 / GP-A-KCB 科创板)
        if not raw or "~" not in raw:
            return "[]"
        results = []
        for line in raw.strip().split("\n"):
            try:
                content = line.split('="', 1)[1].rstrip('";')
                parts = content.split("~")
                if len(parts) >= 4:
                    results.append({
                        "code": parts[1], "name": parts[2],
                        "market": (parts[0] or "").upper(),  # 直接用上游的市场段 sh/sz → SH/SZ
                        "type": parts[4] if len(parts) > 4 else parts[3],
                    })
            except (IndexError, ValueError):
                continue
        return _json.dumps(results, ensure_ascii=False)
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════════════════════
# 20. A-share financials (eastmoney datacenter)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_financials", "A-share key financial metrics: PE, PB, market cap, EPS (derived from Tencent quote). "
      "Code formats accepted: 600519 / sh600519 / 600519.SH. "
      "Returns array(1 item): name, price, pe_ttm, pe_static, pb, eps, bps, roe_approx, "
      "market_cap_yi(亿元), float_market_cap_yi(亿元), turnover_pct, source, note.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "annual": {"type": "boolean", "description": "True=annual, False=latest quarter (default: True)"}}, required=['symbol'])
def get_a_financials(symbol: str, annual: bool = True) -> str:
    import json as _json
    # Normalize symbol（统一走 normalize_symbol：sh/sz/bj + 600519.SH 等写法都收）
    t_sym = tencent_symbol(symbol)
    if t_sym is None:
        return symbol_fail(symbol)
    try:
        # EastMoney datacenter securities API (RPT_DMSK_FN_MAININDICATOR) 已废弃
        # 改用腾讯行情数据派生基本财务指标
        url = f"https://qt.gtimg.cn/q={t_sym}"
        r = _get_requests().get(url, timeout=10)
        raw = r.content.decode("gbk", errors="replace")
        # Parse Tencent quote: v_CODE="field1~field2~..."
        line = raw.strip().split("\n")[0].strip()
        eq = line.find("=")
        if eq < 0:
            return "[]"
        val = line[eq+1:].strip().strip('"; ')
        parts = val.split("~")
        if len(parts) < 53:
            return "[]"

        name = parts[1]
        price = float(parts[3]) if parts[3] else 0
        pe_ttm = float(parts[39]) if parts[39] else 0
        mcap_yi = float(parts[44]) if parts[44] else 0       # 总市值(亿)
        float_mcap_yi = float(parts[45]) if parts[45] else 0  # 流通市值(亿)
        pb = float(parts[46]) if parts[46] else 0
        turnover = float(parts[38]) if parts[38] else 0
        pe_static = float(parts[52]) if parts[52] else 0

        # Derive EPS and BPS from price/PE and price/PB
        eps = round(price / pe_ttm, 2) if pe_ttm > 0 else 0
        bps = round(price / pb, 2) if pb > 0 else 0
        # ROE ≈ PB × (EPS/BPS) ≈ 1/PE × PB... simpler: ROE = PB / PE (approximation)
        roe_approx = round((1.0 / pe_ttm) * pb * 100, 2) if pe_ttm > 0 else 0

        result = [{
            "date": "",
            "name": name,
            "price": price,
            "pe_ttm": pe_ttm,
            "pe_static": pe_static,
            "pb": pb,
            "eps": eps,
            "bps": bps,
            "roe_approx": roe_approx,
            "market_cap_yi": mcap_yi,
            "float_market_cap_yi": float_mcap_yi,
            "turnover_pct": turnover,
            "source": "tencent_quote",
            "note": "PE/PB/market_cap from Tencent; EPS/BPS/ROE derived (not from financial statements)",
        }]
        return _json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════════════════════
# 21. A-share intraday (Tencent minute bars)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_intraday", "A-share intraday minute price/volume. Returns recent minute bars.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"}}, required=['symbol'])
def get_a_intraday(symbol: str) -> str:
    import json as _json, urllib.request
    code = tencent_symbol(symbol)
    if code is None:
        return symbol_fail(symbol)
    try:
        url = f"http://ifzq.gtimg.cn/appstock/app/minute/query?_var=min_data&code={code}"
        req = urllib.request.Request(url, headers={"User-Agent": "athena-mcp/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("gbk", errors="replace")
        # Extract JSON from JS callback
        start = raw.find("{")
        if start < 0:
            return "[]"
        data = _json.loads(raw[start:])
        qt = data.get("data", {}).get(code, {})
        bars = qt.get("data", []) or []
        # Return last 120 bars (2 hours of minute data)
        recent = bars[-120:] if len(bars) > 120 else bars
        result = []
        prev_close = None
        for b in recent:
            if isinstance(b, dict):
                result.append({"t": b.get("t",""), "p": float(b.get("p",0)),
                               "v": int(b.get("v",0)), "a": float(b.get("a",0))})
            elif isinstance(b, list) and len(b) >= 2:
                result.append({"t": str(b[0]), "p": float(b[1])})
        return _json.dumps(result, ensure_ascii=False)
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 22-23. Research reports (Eastmoney, replace akshare)
# ═══════════════════════════════════════════════

REPORT_API = "https://reportapi.eastmoney.com/report/list"
PDF_TPL = "https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf"


@tool("get_a_reports", "Get institutional research reports for an A-share stock (Eastmoney). "
      "Returns title, org, date, rating, EPS forecasts, infoCode for PDF download. "
      "Returns array: title, orgSName(机构), publishDate, emRatingName(评级), "
      "predictThisYearEps/predictNextYearEps, infoCode(PDF下载用).",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "max_pages": {"type": "integer", "description": "Max pages to fetch (default 5)"}}, required=['symbol'])
def get_a_reports(symbol: str, max_pages: int = 5) -> str:
    import json as _json
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_reports", code, str(max_pages))
    if cached:
        return cached
    try:
        all_records = []
        for page in range(1, max_pages + 1):
            params = {
                "industryCode": "*", "pageSize": "100", "industry": "*",
                "rating": "*", "ratingChange": "*",
                "beginTime": "2000-01-01", "endTime": "2030-01-01",
                "pageNo": str(page), "fields": "", "qType": "0",
                "orgCode": "", "code": code, "rcode": "",
                "p": str(page), "pageNum": str(page), "pageNumber": str(page),
            }
            r = em_get(REPORT_API, params=params,
                       headers={"Referer": "https://data.eastmoney.com/"}, timeout=30)
            d = r.json()
            rows = d.get("data") or []
            if not rows:
                break
            all_records.extend(rows)
            if page >= (d.get("TotalPage", 1) or 1):
                break
        result = _json.dumps(all_records, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_reports", result, code, str(max_pages))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_industry_reports", "Get industry research reports (Eastmoney). industry_code='*'=all, "
      "or pass specific code (e.g. '1238'=IT服务Ⅱ). Use '*' first to discover codes.",
      {"industry_code": {"type": "string", "description": "Industry code, '*' for all (default)"},
       "max_pages": {"type": "integer", "description": "Max pages (default 3)"}})
def get_a_industry_reports(industry_code: str = "*", max_pages: int = 3) -> str:
    import json as _json
    cached = _cache_get("get_a_industry_reports", industry_code, str(max_pages))
    if cached:
        return cached
    try:
        all_records = []
        for page in range(1, max_pages + 1):
            params = {
                "industryCode": industry_code, "pageSize": "100", "industry": "*",
                "rating": "*", "ratingChange": "*",
                "beginTime": "2024-01-01", "endTime": "2030-01-01",
                "pageNo": str(page), "fields": "", "qType": "1",
            }
            r = em_get(REPORT_API, params=params,
                       headers={"Referer": "https://data.eastmoney.com/"}, timeout=30)
            d = r.json()
            rows = d.get("data") or []
            if not rows:
                break
            all_records.extend(rows)
            if page >= (d.get("TotalPage", 1) or 1):
                break
        result = _json.dumps(all_records, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_industry_reports", result, industry_code, str(max_pages))
        return result
    except Exception as e:
        return fail(e)


REPORTS_DIR = Path(os.environ.get("REPORTS_DIR", str(CACHE_DIR.parent / "reports")))


@tool("download_report_pdf", "Download a research report PDF by infoCode. Returns saved file path. "
      "Files are saved under a fixed server-side reports dir (REPORTS_DIR env).",
      {"info_code": {"type": "string", "description": "Report infoCode from get_a_reports record"},
       "target_dir": {"type": "string", "description": "保存目录（已废弃：一律存 REPORTS_DIR，参数忽略）"}}, required=['info_code'])
def download_report_pdf(info_code: str, target_dir: str = "./reports") -> str:
    import re as _re
    try:
        # info_code 直接拼进 URL 和文件名，必须消毒（防 URL 注入 + 路径穿越）；
        # target_dir 不再采纳：写盘目录锁定 REPORTS_DIR，杜绝任意路径写
        if not _re.fullmatch(r"[A-Za-z0-9_\-]{1,64}", info_code or ""):
            return '{"error":"invalid info_code"}'
        url = PDF_TPL.format(info_code=info_code)
        r = em_get(url, headers={"Referer": "https://data.eastmoney.com/"}, timeout=60)
        if r.status_code == 200 and len(r.content) >= 1024:
            REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            fpath = REPORTS_DIR / f"H3_{info_code}_1.pdf"
            fpath.write_bytes(r.content)
            return '{"path":"%s","size":%d}' % (str(fpath), len(r.content))
        return '{"error":"Download failed, status=%d size=%d"}' % (r.status_code, len(r.content))
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 24. THS consensus EPS forecast
# ═══════════════════════════════════════════════

@tool("get_a_eps_forecast", "Get institutional consensus EPS forecast from THS (10jqka). "
      "Returns analyst count, min/mean/max EPS per year. 'mean' = consensus. "
      "Warning: analyst_count < 3 = low confidence.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"}}, required=['symbol'])
def get_a_eps_forecast(symbol: str) -> str:
    import json as _json
    from io import StringIO as _StringIO
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_eps_forecast", code)
    if cached:
        return cached
    try:
        url = f"https://basic.10jqka.com.cn/new/{code}/worth.html"
        headers = {
            "User-Agent": _UA,
            "Referer": "https://basic.10jqka.com.cn/",
        }
        r = _get_requests().get(url, headers=headers, timeout=15)
        r.encoding = "gbk"
        dfs = _get_pd().read_html(_StringIO(r.text))
        for df in dfs:
            cols = [str(c) for c in df.columns]
            if any("每股收益" in c or "均值" in c for c in cols):
                result = df.to_json(orient="records", force_ascii=False)
                if not _is_empty_result(result):  # 空结果不缓存
                    _cache_set("get_a_eps_forecast", result, code)
                return result
        result = dfs[0].to_json(orient="records", force_ascii=False) if dfs else "[]"
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_eps_forecast", result, code)
        return result
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 25-32. Signal layer (信号层)
# ═══════════════════════════════════════════════

@tool("get_a_hot_reason", "Get today's strong stocks with reason tags from THS editorial. "
      "Returns name, code, reason (题材归因), change%, turnover, DDX. ~125 stocks, 73ms latency.",
      {"date": {"type": "string", "description": "Date YYYY-MM-DD (default today)"}})
def get_a_hot_reason(date: str = None) -> str:
    import json as _json
    date, derr = date_arg(date, "%Y-%m-%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_hot_reason", date)
    if cached:
        return cached
    try:
        url = (f"http://zx.10jqka.com.cn/event/api/getharden/"
               f"date/{date}/orderby/date/orderway/desc/charset/GBK/")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36"}
        r = _get_requests().get(url, headers=headers, timeout=10)
        data = r.json()
        if data.get("errocode", 0) != 0:
            return fail(data.get("errormsg", "unknown"), code="upstream_error")
        rows = data.get("data") or []
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_hot_reason", result, date)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_north_flow_minute", "Real-time minute-level northbound capital flow (沪深股通). "
      "Returns time series of hgt/sgt cumulative net inflow in 100M CNY. 262 data points per day.",
      {"save_snapshot": {"type": "boolean", "description": "Save closing snapshot to local CSV cache (default False)"}})
def get_a_north_flow_minute(save_snapshot: bool = False) -> str:
    import json as _json
    from pathlib import Path as _Path
    cached = _cache_get("get_a_north_flow_minute", str(save_snapshot))
    if cached:
        return cached
    try:
        HSGT_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36",
            "Host": "data.hexin.cn",
            "Referer": "https://data.hexin.cn/",
        }
        r = _get_requests().get("https://data.hexin.cn/market/hsgtApi/method/dayChart/",
                                headers=HSGT_HEADERS, timeout=10)
        d = r.json()
        times = d.get("time", [])
        hgt = d.get("hgt", [])
        sgt = d.get("sgt", [])
        result = _json.dumps({"times": times, "hgt_yi": hgt, "sgt_yi": sgt}, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_north_flow_minute", result, str(save_snapshot))
        if save_snapshot and not d.get("time"):
            pass  # Market closed, no data to save
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_concept_blocks", "Get all sector/concept/region blocks a stock belongs to (Eastmoney slist). "
      "Returns board names, BK codes, change%, and lead stock. Used for theme attribution.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"}}, required=['symbol'])
def get_a_concept_blocks(symbol: str) -> str:
    import json as _json
    secid = eastmoney_secid(symbol)
    if secid is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_concept_blocks", secid)
    if cached:
        return cached
    try:
        params = {
            "fltt": "2", "invt": "2",
            "secid": secid,
            "spt": "3", "pi": "0", "pz": "200", "po": "1",
            "fields": "f12,f14,f3,f128",
        }
        headers = {"User-Agent": _UA, "Referer": "https://quote.eastmoney.com/"}
        r = em_get("https://push2.eastmoney.com/api/qt/slist/get",
                   params=params, headers=headers, timeout=15)
        d = r.json()
        diff = (d.get("data") or {}).get("diff") or {}
        items = diff.values() if isinstance(diff, dict) else diff
        boards = []
        for it in items:
            boards.append({
                "name": it.get("f14", ""),
                "code": it.get("f12", ""),
                "change_pct": it.get("f3", ""),
                "lead_stock": it.get("f128", ""),
            })
        result = _json.dumps({"total": len(boards), "boards": boards,
                              "concept_tags": [b["name"] for b in boards]}, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_concept_blocks", result, secid)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_fund_flow_minute", "Intraday minute-level fund flow (主力/超大单/大单/中单/小单 net inflow). "
      "Unit: CNY. Use klt=1 for minute, klt=5 for 5-min, klt=101 for daily.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '000858')"},
       "klt": {"type": "integer", "description": "Bar size: 1=minute, 5=5min, 101=daily (default 1)"}}, required=['symbol'])
def get_a_fund_flow_minute(symbol: str, klt: int = 1) -> str:
    import json as _json
    secid = eastmoney_secid(symbol)
    if secid is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_fund_flow_minute", secid, str(klt))
    if cached:
        return cached
    try:
        url = "https://push2.eastmoney.com/api/qt/stock/fflow/kline/get"
        params = {
            "secid": secid, "klt": klt,
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57",
        }
        headers = {"User-Agent": _UA, "Referer": "https://quote.eastmoney.com/",
                   "Origin": "https://quote.eastmoney.com"}
        r = em_get(url, params=params, headers=headers, timeout=10)
        d = r.json()
        rows = []
        for line in d.get("data", {}).get("klines", []):
            parts = line.split(",")
            if len(parts) >= 6:
                rows.append({
                    "time": parts[0],
                    "main_net": float(parts[1]),
                    "small_net": float(parts[2]),
                    "mid_net": float(parts[3]),
                    "large_net": float(parts[4]),
                    "super_net": float(parts[5]),
                })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_fund_flow_minute", result, secid, str(klt))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_dragon_tiger", "Get dragon tiger board (龙虎榜) records for a stock:上榜记录 + 买卖席位 TOP5 + 机构动向.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '002475')"},
       "trade_date": {"type": "string", "description": "Trade date YYYY-MM-DD"},
       "look_back": {"type": "integer", "description": "Look-back days (default 30)"}}, required=['symbol', 'trade_date'])
def get_a_dragon_tiger(symbol: str, trade_date: str, look_back: int = 30) -> str:
    import json as _json
    from datetime import datetime as _dt, timedelta as _td
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    trade_date, derr = date_arg(trade_date, "%Y-%m-%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_dragon_tiger", code, trade_date, str(look_back))
    if cached:
        return cached
    try:
        start = _dt.strptime(trade_date, "%Y-%m-%d") - _td(days=look_back)
        start_str = start.strftime("%Y-%m-%d")
        records = []
        data = eastmoney_datacenter(
            "RPT_DAILYBILLBOARD_DETAILSNEW",
            filter_str=f"(TRADE_DATE>='{start_str}')(TRADE_DATE<='{trade_date}')(SECURITY_CODE=\"{code}\")",
            page_size=50, sort_columns="TRADE_DATE", sort_types="-1",
        )
        for row in data:
            records.append({
                "date": str(row.get("TRADE_DATE", ""))[:10],
                "reason": row.get("EXPLANATION", ""),
                "net_buy_wan": round((row.get("BILLBOARD_NET_AMT") or 0) / 10000, 1),
                "turnover": round(float(row.get("TURNOVERRATE") or 0), 2),
            })
        seats = {"buy": [], "sell": []}
        institution = {"buy_wan": 0, "sell_wan": 0, "net_wan": 0}
        if records:
            latest_date = records[0]["date"]
            buy_data = eastmoney_datacenter(
                "RPT_BILLBOARD_DAILYDETAILSBUY",
                filter_str=f"(TRADE_DATE='{latest_date}')(SECURITY_CODE=\"{code}\")",
                page_size=10, sort_columns="BUY", sort_types="-1",
            )
            for row in buy_data[:5]:
                seats["buy"].append({
                    "name": row.get("OPERATEDEPT_NAME", ""),
                    "buy_wan": round((row.get("BUY") or 0) / 10000, 1),
                    "sell_wan": round((row.get("SELL") or 0) / 10000, 1),
                    "net_wan": round((row.get("NET") or 0) / 10000, 1),
                })
            sell_data = eastmoney_datacenter(
                "RPT_BILLBOARD_DAILYDETAILSSELL",
                filter_str=f"(TRADE_DATE='{latest_date}')(SECURITY_CODE=\"{code}\")",
                page_size=10, sort_columns="SELL", sort_types="-1",
            )
            for row in sell_data[:5]:
                seats["sell"].append({
                    "name": row.get("OPERATEDEPT_NAME", ""),
                    "buy_wan": round((row.get("BUY") or 0) / 10000, 1),
                    "sell_wan": round((row.get("SELL") or 0) / 10000, 1),
                    "net_wan": round((row.get("NET") or 0) / 10000, 1),
                })
            for detail_data in [buy_data, sell_data]:
                for row in detail_data:
                    if str(row.get("OPERATEDEPT_CODE", "")) == "0":
                        institution["buy_wan"] += (row.get("BUY") or 0) / 10000
                        institution["sell_wan"] += (row.get("SELL") or 0) / 10000
            institution["buy_wan"] = round(institution["buy_wan"], 1)
            institution["sell_wan"] = round(institution["sell_wan"], 1)
            institution["net_wan"] = round(institution["buy_wan"] - institution["sell_wan"], 1)
        result = _json.dumps({"records": records, "seats": seats, "institution": institution}, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_dragon_tiger", result, code, trade_date, str(look_back))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_lockup_expiry", "Get lockup expiry calendar (限售解禁): historical + upcoming 90 days. "
      "trade_date accepts 2026-09-14 / 20260914 / 2026/09/14.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '002475')"},
       "trade_date": {"type": "string", "description": "Trade date YYYY-MM-DD"},
       "forward_days": {"type": "integer", "description": "Days to look forward (default 90)"}}, required=['symbol', 'trade_date'])
def get_a_lockup_expiry(symbol: str, trade_date: str, forward_days: int = 90) -> str:
    import json as _json
    from datetime import datetime as _dt, timedelta as _td
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    trade_date, derr = date_arg(trade_date, "%Y-%m-%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_lockup_expiry", code, trade_date, str(forward_days))
    if cached:
        return cached
    try:
        history_data = eastmoney_datacenter(
            "RPT_LIFT_STAGE",
            filter_str=f'(SECURITY_CODE="{code}")',
            page_size=15, sort_columns="FREE_DATE", sort_types="-1",
        )
        history = []
        for row in history_data:
            history.append({
                "date": str(row.get("FREE_DATE", ""))[:10],
                "type": row.get("LIMITED_STOCK_TYPE", ""),
                "shares": row.get("FREE_SHARES_NUM", 0),
                "ratio": row.get("FREE_RATIO", 0),
            })
        end_date = _dt.strptime(trade_date, "%Y-%m-%d") + _td(days=forward_days)
        end_str = end_date.strftime("%Y-%m-%d")
        upcoming_data = eastmoney_datacenter(
            "RPT_LIFT_STAGE",
            filter_str=f'(SECURITY_CODE="{code}")(FREE_DATE>=\'{trade_date}\')(FREE_DATE<=\'{end_str}\')',
            page_size=20, sort_columns="FREE_DATE", sort_types="1",
        )
        upcoming = []
        for row in upcoming_data:
            upcoming.append({
                "date": str(row.get("FREE_DATE", ""))[:10],
                "type": row.get("LIMITED_STOCK_TYPE", ""),
                "shares": row.get("FREE_SHARES_NUM", 0),
                "ratio": row.get("FREE_RATIO", 0),
            })
        result = _json.dumps({"history": history, "upcoming": upcoming}, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_lockup_expiry", result, code, trade_date, str(forward_days))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_industry_rank", "Get sector ranking by change% (全行业涨跌幅排名). Returns top N and bottom N sectors.",
      {"top_n": {"type": "integer", "description": "Number of top/bottom sectors to return (default 20)"}})
def get_a_industry_rank(top_n: int = 20) -> str:
    import json as _json
    cached = _cache_get("get_a_industry_rank", str(top_n))
    if cached:
        return cached
    try:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1", "pz": "100", "po": "1", "np": "1",
            "fltt": "2", "invt": "2",
            "fs": "m:90+t:2",
            "fields": "f2,f3,f4,f12,f13,f14,f104,f105,f128,f136,f140,f141,f207",
        }
        headers = {"User-Agent": _UA}
        r = em_get(url, params=params, headers=headers, timeout=15)
        d = r.json()
        items = d.get("data", {}).get("diff", [])
        if not items:
            return '{"top":[],"bottom":[],"total":0}'
        rows = []
        for i, item in enumerate(items):
            rows.append({
                "rank": i + 1,
                "name": item.get("f14", ""),
                "change_pct": item.get("f3", 0),
                "code": item.get("f12", ""),
                "up_count": item.get("f104", 0),
                "down_count": item.get("f105", 0),
                "leader": item.get("f140", ""),
                "leader_change": item.get("f136", 0),
            })
        result = _json.dumps({"top": rows[:top_n], "bottom": rows[-top_n:],
                              "total": len(rows)}, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_industry_rank", result, str(top_n))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_daily_dragon_tiger", "Get market-wide dragon tiger board for a day (全市场龙虎榜). "
      "Returns all stocks hitting the board with net buy amounts. "
      "trade_date accepts 2026-09-14 / 20260914 / 2026/09/14 (default today). "
      "Returns object: date, total_records, stocks[] with code, name, reason, close, "
      "change_pct, net_buy_wan, buy_wan, sell_wan, turnover_pct.",
      {"trade_date": {"type": "string", "description": "Trade date YYYY-MM-DD (default today)"},
       "min_net_buy_wan": {"type": "number", "description": "Min net buy in 万CNY filter (optional)"}})
def get_a_daily_dragon_tiger(trade_date: str = None, min_net_buy_wan: float = None) -> str:
    import json as _json
    trade_date, derr = date_arg(trade_date, "%Y-%m-%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_daily_dragon_tiger", trade_date, str(min_net_buy_wan or ""))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPT_DAILYBILLBOARD_DETAILSNEW",
            filter_str=f"(TRADE_DATE>='{trade_date}')(TRADE_DATE<='{trade_date}')",
            page_size=500, sort_columns="BILLBOARD_NET_AMT", sort_types="-1",
        )
        if not data:
            return '{"date":"%s","total_records":0,"stocks":[],"note":"无数据（非交易日或盘后未更新）"}' % trade_date
        actual_date = str(data[0].get("TRADE_DATE", ""))[:10] if data else trade_date
        stocks = []
        for row in data:
            net_buy = (row.get("BILLBOARD_NET_AMT") or 0) / 10000
            if min_net_buy_wan is not None and net_buy < min_net_buy_wan:
                continue
            stocks.append({
                "code": row.get("SECURITY_CODE", ""),
                "name": row.get("SECURITY_NAME_ABBR", ""),
                "reason": row.get("EXPLANATION", ""),
                "close": row.get("CLOSE_PRICE") or 0,
                "change_pct": round(float(row.get("CHANGE_RATE") or 0), 2),
                "net_buy_wan": round(net_buy, 1),
                "buy_wan": round((row.get("BILLBOARD_BUY_AMT") or 0) / 10000, 1),
                "sell_wan": round((row.get("BILLBOARD_SELL_AMT") or 0) / 10000, 1),
                "turnover_pct": round(float(row.get("TURNOVERRATE") or 0), 2),
            })
        result = _json.dumps({"date": actual_date, "total_records": len(stocks), "stocks": stocks}, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_daily_dragon_tiger", result, trade_date, str(min_net_buy_wan or ""))
        return result
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 33-37. Capital / Position layer (资金面/筹码层)
# ═══════════════════════════════════════════════

@tool("get_a_margin", "Get margin trading details (融资融券): balance, buy, repay, short balance per day.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 30)"}}, required=['symbol'])
def get_a_margin(symbol: str, page_size: int = 30) -> str:
    import json as _json
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_margin", code, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPTA_WEB_RZRQ_GGMX",
            filter_str=f'(SCODE="{code}")',
            page_size=page_size, sort_columns="DATE", sort_types="-1",
        )
        rows = []
        for row in data:
            rows.append({
                "date": str(row.get("DATE", ""))[:10],
                "rzye": row.get("RZYE", 0),
                "rzmre": row.get("RZMRE", 0),
                "rzche": row.get("RZCHE", 0),
                "rqye": row.get("RQYE", 0),
                "rqmcl": row.get("RQMCL", 0),
                "rqchl": row.get("RQCHL", 0),
                "rzrqye": row.get("RZRQYE", 0),
            })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_margin", result, symbol, str(page_size))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_block_trade", "Get block trade records (大宗交易): price, volume, buyer/seller, premium%.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 20)"}}, required=['symbol'])
def get_a_block_trade(symbol: str, page_size: int = 20) -> str:
    import json as _json
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_block_trade", code, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPT_DATA_BLOCKTRADE",
            filter_str=f'(SECURITY_CODE="{code}")',
            page_size=page_size, sort_columns="TRADE_DATE", sort_types="-1",
        )
        rows = []
        for row in data:
            close = row.get("CLOSE_PRICE") or 0
            deal_price = row.get("DEAL_PRICE") or 0
            premium = ((deal_price / close - 1) * 100) if close else 0
            rows.append({
                "date": str(row.get("TRADE_DATE", ""))[:10],
                "price": deal_price, "close": close,
                "premium_pct": round(premium, 2),
                "vol": row.get("DEAL_VOLUME", 0),
                "amount": row.get("DEAL_AMT", 0),
                "buyer": row.get("BUYER_NAME", ""),
                "seller": row.get("SELLER_NAME", ""),
            })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_block_trade", result, symbol, str(page_size))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_holder_num", "Get shareholder count changes (股东户数变化). Fewer holders = concentration = accumulation signal.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 10)"}}, required=['symbol'])
def get_a_holder_num(symbol: str, page_size: int = 10) -> str:
    import json as _json
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_holder_num", code, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPT_HOLDERNUMLATEST",
            filter_str=f'(SECURITY_CODE="{code}")',
            page_size=page_size, sort_columns="END_DATE", sort_types="-1",
        )
        rows = []
        for row in data:
            rows.append({
                "date": str(row.get("END_DATE", ""))[:10],
                "holder_num": row.get("HOLDER_NUM", 0),
                "change_num": row.get("HOLDER_NUM_CHANGE", 0),
                "change_ratio": row.get("HOLDER_NUM_RATIO", 0),
                "avg_shares": row.get("AVG_FREE_SHARES", 0),
            })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_holder_num", result, code, str(page_size))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_dividend", "Get dividend history (分红送转): bonus per share, transfer ratio, bonus ratio.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 20)"}}, required=['symbol'])
def get_a_dividend(symbol: str, page_size: int = 20) -> str:
    import json as _json
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_dividend", code, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPT_SHAREBONUS_DET",
            filter_str=f'(SECURITY_CODE="{code}")',
            page_size=page_size, sort_columns="EX_DIVIDEND_DATE", sort_types="-1",
        )
        rows = []
        for row in data:
            rows.append({
                "date": str(row.get("EX_DIVIDEND_DATE", ""))[:10],
                "bonus_rmb": row.get("PRETAX_BONUS_RMB", 0),
                "transfer_ratio": row.get("TRANSFER_RATIO", 0),
                "bonus_ratio": row.get("BONUS_RATIO", 0),
                "plan": row.get("ASSIGN_PROGRESS", ""),
            })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_dividend", result, code, str(page_size))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_fund_flow_120d", "Get daily fund flow for last 120 trading days: main/super/large/mid/small net. Unit: CNY. "
      "Returns array(≤120 bars): date, main_net(主力,元), small_net(小单,元), mid_net(中单,元), "
      "large_net(大单,元), super_net(超大单,元).",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"}}, required=['symbol'])
def get_a_fund_flow_120d(symbol: str) -> str:
    import json as _json
    secid = eastmoney_secid(symbol)
    if secid is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_fund_flow_120d", secid)
    if cached:
        return cached
    try:
        url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f7",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f62,f63,f64,f65",
            "lmt": "120",
        }
        headers = {"User-Agent": _UA, "Referer": "https://quote.eastmoney.com/",
                   "Origin": "https://quote.eastmoney.com"}
        r = em_get(url, params=params, headers=headers, timeout=15)
        d = r.json()
        klines = d.get("data", {}).get("klines", [])
        rows = []
        for line in klines:
            parts = line.split(",")
            if len(parts) >= 7:
                rows.append({
                    "date": parts[0],
                    "main_net": float(parts[1]) if parts[1] != "-" else 0,
                    "small_net": float(parts[2]) if parts[2] != "-" else 0,
                    "mid_net": float(parts[3]) if parts[3] != "-" else 0,
                    "large_net": float(parts[4]) if parts[4] != "-" else 0,
                    "super_net": float(parts[5]) if parts[5] != "-" else 0,
                })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_fund_flow_120d", result, secid)
        return result
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 38-39. News layer (新闻层)
# ═══════════════════════════════════════════════

@tool("get_a_news", "Get stock-specific news from Eastmoney (个股新闻). Returns title, content, time, source, URL. "
      "Returns array: title, content(摘要≤200字), time, source(媒体), url.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "page_size": {"type": "integer", "description": "Articles to fetch (default 20)"}}, required=['symbol'])
def get_a_news(symbol: str, page_size: int = 20) -> str:
    import json as _json, re as _re
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_news", code, str(page_size))
    if cached:
        return cached
    try:
        cb = "jQuery_news"
        url = "https://search-api-web.eastmoney.com/search/jsonp"
        inner_params = _json.dumps({
            "uid": "", "keyword": code,
            "type": ["cmsArticleWebOld"],
            "client": "web", "clientType": "web", "clientVersion": "curr",
            "param": {"cmsArticleWebOld": {"searchScope": "default", "sort": "default",
                      "pageIndex": 1, "pageSize": page_size, "preTag": "", "postTag": ""}},
        }, separators=(',', ':'))
        params = {"cb": cb, "param": inner_params}
        headers = {"User-Agent": _UA, "Referer": "https://so.eastmoney.com/"}
        r = em_get(url, params=params, headers=headers, timeout=15)
        text = r.text
        json_str = text[text.index("(") + 1: text.rindex(")")]
        d = _json.loads(json_str)
        articles = d.get("result", {}).get("cmsArticleWebOld", []) or []
        rows = []
        for a in articles:
            rows.append({
                "title": _re.sub(r'<[^>]+>', '', a.get("title", "")),
                "content": _re.sub(r'<[^>]+>', '', a.get("content", ""))[:200],
                "time": a.get("date", ""),
                "source": a.get("mediaName", ""),
                "url": a.get("url", ""),
            })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_news", result, code, str(page_size))
        return result
    except Exception as e:
        return fail(e)

# ═══════════════════════════════════════════════
# 40-43. Fundamental data layer (基础数据层)
# ═══════════════════════════════════════════════

@tool("get_a_mootdx_finance", "Get 37-field quarterly financial snapshot via mootdx: EPS, ROE, BVPS, revenue, profit, etc.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"}}, required=['symbol'])
def get_a_mootdx_finance(symbol: str) -> str:
    import json as _json
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_mootdx_finance", code)
    if cached:
        return cached
    try:
        client = tdx_client()
        fin = client.finance(symbol=code)
        # Convert to serializable dict
        result = _json.dumps(dict(fin) if hasattr(fin, 'items') else str(fin), ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_mootdx_finance", result, code)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_mootdx_f10", "Get company F10 text data (9 categories): 最新提示/公司概况/财务分析/股东研究/股本结构/资本运作/业内点评/行业分析/公司大事.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "category": {"type": "string", "description": "Category: 最新提示/公司概况/财务分析/股东研究/股本结构/资本运作/业内点评/行业分析/公司大事 (default 最新提示)"}}, required=['symbol'])
def get_a_mootdx_f10(symbol: str, category: str = "最新提示") -> str:
    import json as _json
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_mootdx_f10", code, category)
    if cached:
        return cached
    try:
        client = tdx_client()
        text = client.F10(symbol=code, name=category)
        result = _json.dumps({"category": category, "content": text or ""}, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_mootdx_f10", result, code, category)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_stock_info", "Get A-share basic info: industry, total/float shares, market cap, list date (Eastmoney push2). "
      "Returns object: code, name, industry, total_shares(股), float_shares(股), "
      "mcap(元), float_mcap(元), list_date(YYYYMMDD), price.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"}}, required=['symbol'])
def get_a_stock_info(symbol: str) -> str:
    import json as _json
    secid = eastmoney_secid(symbol)
    if secid is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_stock_info", secid)
    if cached:
        return cached
    try:
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "fltt": "2", "invt": "2",
            "fields": "f57,f58,f84,f85,f127,f116,f117,f189,f43",
            "secid": secid,
        }
        headers = {"User-Agent": _UA}
        r = em_get(url, params=params, headers=headers, timeout=10)
        d = r.json().get("data", {})
        info = {
            "code": d.get("f57", ""), "name": d.get("f58", ""),
            "industry": d.get("f127", ""),
            "total_shares": d.get("f84", 0),
            "float_shares": d.get("f85", 0),
            "mcap": d.get("f116", 0),
            "float_mcap": d.get("f117", 0),
            "list_date": str(d.get("f189", "")),
            "price": d.get("f43", 0),
        }
        result = _json.dumps(info, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_stock_info", result, secid)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_financial_statements", "Get Sina financial statements (新浪财报三表): balance sheet, income statement, cash flow.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "report_type": {"type": "string", "description": "'fzb'=balance sheet, 'lrb'=income, 'llb'=cashflow (default 'lrb')"},
       "num": {"type": "integer", "description": "Number of periods (default 8)"}}, required=['symbol'])
def get_a_financial_statements(symbol: str, report_type: str = "lrb", num: int = 8) -> str:
    import json as _json
    norm = normalize_symbol(symbol)
    if norm is None:
        return symbol_fail(symbol)
    market, code = norm
    cached = _cache_get("get_a_financial_statements", market + code, report_type, str(num))
    if cached:
        return cached
    try:
        prefix = market  # 新浪只认 sh/sz（北交所无此源，会返回空）
        paper_code = f"{prefix}{code}"
        url = "https://quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReport2022"
        params = {
            "paperCode": paper_code, "source": report_type,
            "type": "0", "page": "1", "num": str(num),
        }
        headers = {"User-Agent": _UA}
        r = _get_requests().get(url, params=params, headers=headers, timeout=15)
        report_list = r.json().get("result", {}).get("data", {}).get("report_list", {}) or {}
        rows = []
        for period in sorted(report_list.keys(), reverse=True)[:num]:
            obj = report_list[period]
            rec = {"period": f"{period[:4]}-{period[4:6]}-{period[6:8]}"}
            for it in obj.get("data", []) or []:
                title = it.get("item_title", "")
                if not title or it.get("item_value") is None:
                    continue
                rec[title] = it.get("item_value")
                tongbi = it.get("item_tongbi")
                if tongbi not in (None, ""):
                    rec[title + "_yoy"] = tongbi
            rows.append(rec)
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_financial_statements", result, market + code, report_type, str(num))
        return result
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 44. Announcement layer (公告层 - 巨潮)
# ═══════════════════════════════════════════════

_CNINFO_ORGID_MAP = {}

def _cninfo_orgid(code: str, market: str = None) -> str:
    global _CNINFO_ORGID_MAP
    if not _CNINFO_ORGID_MAP:
        try:
            r = _get_requests().get("http://www.cninfo.com.cn/new/data/szse_stock.json",
                                    headers={"User-Agent": _UA}, timeout=15)
            _CNINFO_ORGID_MAP = {s["code"]: s["orgId"]
                                 for s in r.json().get("stockList", [])}
        except Exception:
            pass
    org = _CNINFO_ORGID_MAP.get(code)
    if org:
        return org
    if market is None:
        market = (normalize_symbol(code) or ("sz", code))[0]
    if market == "sh":
        return f"gssh0{code}"
    if market == "bj":
        return f"gsbj0{code}"
    return f"gssz0{code}"


@tool("get_a_announcements", "Get full-text announcements from cninfo (巨潮公告). Returns title, type, date, URL.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 30)"}}, required=['symbol'])
def get_a_announcements(symbol: str, page_size: int = 30) -> str:
    import json as _json
    from datetime import datetime as _dt
    norm = normalize_symbol(symbol)
    if norm is None:
        return symbol_fail(symbol)
    market, code = norm
    cached = _cache_get("get_a_announcements", code, str(page_size))
    if cached:
        return cached
    try:
        url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
        org_id = _cninfo_orgid(code, market)
        payload = {
            "stock": f"{code},{org_id}",
            "tabName": "fulltext", "pageSize": str(page_size), "pageNum": "1",
            "column": "", "category": "", "plate": "", "seDate": "",
            "searchkey": "", "secid": "", "sortName": "", "sortType": "",
            "isHLtitle": "true",
        }
        headers = {
            "User-Agent": _UA,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": "https://www.cninfo.com.cn/new/disclosure",
            "Origin": "https://www.cninfo.com.cn",
        }
        r = _get_requests().post(url, data=payload, headers=headers, timeout=15)
        d = r.json()
        rows = []
        for item in d.get("announcements", []) or []:
            ts = item.get("announcementTime")
            if isinstance(ts, (int, float)):
                date_str = _dt.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            else:
                date_str = str(ts)[:10] if ts else ""
            rows.append({
                "title": item.get("announcementTitle", ""),
                "type": item.get("announcementTypeName", ""),
                "date": date_str,
                "url": f"https://www.cninfo.com.cn/new/disclosure/detail?annoId={item.get('announcementId', '')}",
            })
        result = _json.dumps(rows, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_announcements", result, code, str(page_size))
        return result
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 45-50. Limit-up / Board layer (打板层)
# ═══════════════════════════════════════════════

_ZTB_UT = "7eea3edcaed734bea9cbfc24409ed989"

def _fmt_zt_time(t) -> str:
    s = str(t).zfill(6)
    return f"{s[0:2]}:{s[2:4]}:{s[4:6]}"

def _em_zt_api(endpoint: str, sort: str, date: str) -> list:
    # 东财 push2ex 只认 YYYYMMDD；调用方可能传 2026-09-11 / 2026/09/11，统一归一化
    try:
        date = normalize_date(date, "%Y%m%d") or str(date).replace("-", "")
    except ValueError:
        date = str(date).replace("-", "")
    url = f"https://push2ex.eastmoney.com/{endpoint}"
    params = {"ut": _ZTB_UT, "dpt": "wz.ztzt", "Pageindex": 0,
              "pagesize": 10000, "sort": sort, "date": date}
    headers = {"User-Agent": _UA, "Referer": "https://quote.eastmoney.com/"}
    try:
        r = em_get(url, params=params, headers=headers, timeout=10)
        return (r.json().get("data") or {}).get("pool") or []
    except Exception:
        return []


@tool("get_a_limit_up_pool", "Get today's limit-up pool (涨停池): name, code, price, pct, limit_days, seal time, seal fund, break_times, industry. "
      "date accepts 20260911 / 2026-09-11 / 2026/09/11 (default today). "
      "Returns array: code, name, price, pct, amount, float_cap, turnover, limit_days, "
      "first_seal, last_seal, seal_fund, break_times, industry, zt_stat.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD or YYYY-MM-DD (default today)"}})
def get_a_limit_up_pool(date: str = None) -> str:
    import json as _json
    date, derr = date_arg(date, "%Y%m%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_limit_up_pool", date)
    if cached:
        return cached
    try:
        out = []
        for p in _em_zt_api("getTopicZTPool", "fbt:asc", date):
            out.append({
                "code": p["c"], "name": p["n"], "price": p["p"] / 1000,
                "pct": round(p["zdp"], 2), "amount": p["amount"],
                "float_cap": p["ltsz"], "turnover": round(p["hs"], 2),
                "limit_days": p["lbc"],
                "first_seal": _fmt_zt_time(p["fbt"]),
                "last_seal": _fmt_zt_time(p["lbt"]),
                "seal_fund": p["fund"], "break_times": p["zbc"],
                "industry": p.get("hybk", ""),
                "zt_stat": f"{(p.get('zttj') or {}).get('days','?')}天{(p.get('zttj') or {}).get('ct','?')}板",
            })
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_limit_up_pool", result, date)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_broken_board", "Get today's broken board pool (炸板池): stocks that hit limit-up then opened. "
      "date accepts 20260911 / 2026-09-11 / 2026/09/11 (default today). "
      "Returns array: code, name, price, limit_price, pct, turnover, first_seal, break_times, "
      "amplitude, speed, industry, zt_stat.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD or YYYY-MM-DD (default today)"}})
def get_a_broken_board(date: str = None) -> str:
    import json as _json
    date, derr = date_arg(date, "%Y%m%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_broken_board", date)
    if cached:
        return cached
    try:
        out = []
        for p in _em_zt_api("getTopicZBPool", "fbt:asc", date):
            out.append({
                "code": p["c"], "name": p["n"], "price": p["p"] / 1000,
                "limit_price": p["ztp"] / 1000, "pct": round(p["zdp"], 2),
                "turnover": round(p["hs"], 2),
                "first_seal": _fmt_zt_time(p["fbt"]),
                "break_times": p["zbc"], "amplitude": round(p["zf"], 2),
                "speed": round(p["zs"], 2), "industry": p.get("hybk", ""),
                "zt_stat": f"{(p.get('zttj') or {}).get('days','?')}天{(p.get('zttj') or {}).get('ct','?')}板",
            })
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_broken_board", result, date)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_limit_down_pool", "Get today's limit-down pool (跌停池). "
      "date accepts 20260911 / 2026-09-11 / 2026/09/11 (default today). "
      "Returns array: code, name, price, pct, turnover, pe, seal_fund, last_seal, "
      "board_amount, dt_days, open_times, industry.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD or YYYY-MM-DD (default today)"}})
def get_a_limit_down_pool(date: str = None) -> str:
    import json as _json
    date, derr = date_arg(date, "%Y%m%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_limit_down_pool", date)
    if cached:
        return cached
    try:
        out = []
        for p in _em_zt_api("getTopicDTPool", "fund:asc", date):
            out.append({
                "code": p["c"], "name": p["n"], "price": p["p"] / 1000,
                "pct": round(p["zdp"], 2), "turnover": round(p["hs"], 2),
                "pe": p.get("pe"), "seal_fund": p["fund"],
                "last_seal": _fmt_zt_time(p["lbt"]),
                "board_amount": p.get("fba"), "dt_days": p.get("days"),
                "open_times": p.get("oc"), "industry": p.get("hybk", ""),
            })
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_limit_down_pool", result, date)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_yesterday_zt", "Get yesterday's limit-up performance today (昨涨停今表现). Used to calculate promotion rate. "
      "date accepts 20260911 / 2026-09-11 / 2026/09/11 (default today). "
      "Returns array: code, name, price, pct, turnover, amplitude, speed, y_first_seal, "
      "y_limit_days, industry, zt_stat.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD or YYYY-MM-DD (default today)"}})
def get_a_yesterday_zt(date: str = None) -> str:
    import json as _json
    date, derr = date_arg(date, "%Y%m%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_yesterday_zt", date)
    if cached:
        return cached
    try:
        out = []
        for p in _em_zt_api("getYesterdayZTPool", "zs:desc", date):
            out.append({
                "code": p["c"], "name": p["n"], "price": p["p"] / 1000,
                "pct": round(p["zdp"], 2), "turnover": round(p["hs"], 2),
                "amplitude": round(p["zf"], 2), "speed": round(p["zs"], 2),
                "y_first_seal": _fmt_zt_time(p["yfbt"]),
                "y_limit_days": p["ylbc"], "industry": p.get("hybk", ""),
                "zt_stat": f"{(p.get('zttj') or {}).get('days','?')}天{(p.get('zttj') or {}).get('ct','?')}板",
            })
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_yesterday_zt", result, date)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_zt_reason", "Get limit-up reason/themes from THS (同花顺涨停揭秘): reason tags, board type, seal rate. "
      "date accepts 20260911 / 2026-09-11 / 2026/09/11 (default today). "
      "Returns array: code, name, price, pct, reason(题材), board_type, seal_rate, "
      "break_times, seal_amount, high_days, first_time, is_again.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD or YYYY-MM-DD (default today)"}})
def get_a_zt_reason(date: str = None) -> str:
    import json as _json
    from datetime import datetime as _dt
    date, derr = date_arg(date, "%Y%m%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_zt_reason", date)
    if cached:
        return cached
    try:
        url = "https://data.10jqka.com.cn/dataapi/limit_up/limit_up_pool"
        params = {
            "page": 1, "limit": 200,
            "field": "199112,10,9001,330323,330324,330325,9002,330329,133971,133970,1968584,3475914,9003,9004",
            "filter": "HS,GEM2STAR", "order_field": "330324", "order_type": "0", "date": date,
        }
        r = _get_requests().get(url, params=params, headers={"User-Agent": _UA}, timeout=10)
        info = (r.json().get("data") or {}).get("info", [])
        out = []
        for it in info:
            ft = it.get("first_limit_up_time")
            out.append({
                "code": it.get("code"), "name": it.get("name"),
                "price": it.get("latest"), "pct": it.get("change_rate"),
                "reason": it.get("reason_type", ""),
                "board_type": it.get("limit_up_type", ""),
                "seal_rate": it.get("limit_up_suc_rate"),
                "break_times": it.get("open_num") or 0,
                "seal_amount": it.get("order_amount"),
                "high_days": it.get("high_days", ""),
                "first_time": _dt.fromtimestamp(int(ft)).strftime("%H:%M:%S") if ft else "",
                "is_again": it.get("is_again_limit"),
            })
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_zt_reason", result, date)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_board_emotion", "Calculate limit-up sentiment: break_rate, max_height, ladder (连板梯队), ZT/DT counts. "
      "Break rate >40% = bearish, <20% = bullish. "
      "date accepts 20260911 / 2026-09-11 / 2026/09/11 (default today). "
      "Returns object: date, zt_count, zb_count, dt_count, break_rate(%), max_height, "
      "ladder({连板数: 家数}).",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD or YYYY-MM-DD (default today)"}})
def get_a_board_emotion(date: str = None) -> str:
    import json as _json
    date, derr = date_arg(date, "%Y%m%d", default_today=True)
    if derr:
        return derr
    cached = _cache_get("get_a_board_emotion", date)
    if cached:
        return cached
    try:
        zt, zb, dt = _em_zt_api("getTopicZTPool", "fbt:asc", date), \
                     _em_zt_api("getTopicZBPool", "fbt:asc", date), \
                     _em_zt_api("getTopicDTPool", "fund:asc", date)
        ladder = {}
        for s in zt:
            ladder[s.get("lbc", 1)] = ladder.get(s.get("lbc", 1), 0) + 1
        zt_n, zb_n = len(zt), len(zb)
        sentiment = {
            "date": date, "zt_count": zt_n, "zb_count": zb_n, "dt_count": len(dt),
            "break_rate": round(zb_n / (zt_n + zb_n) * 100, 1) if (zt_n + zb_n) else 0,
            "max_height": max((s.get("lbc", 1) for s in zt), default=0),
            "ladder": dict(sorted(ladder.items())),
        }
        result = _json.dumps(sentiment, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_board_emotion", result, date)
        return result
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 51-53. ETF Options layer (期权层)
# ═══════════════════════════════════════════════

SINA_OPT_HDR = {"Referer": "https://stock.finance.sina.com.cn/", "User-Agent": _UA}

def _opt_f(x):
    try: return float(x)
    except Exception: return x

def _sina_opt_list(param: str) -> list:
    r = _get_requests().get(f"https://hq.sinajs.cn/list={param}", headers=SINA_OPT_HDR, timeout=10)
    r.encoding = "gbk"
    t = r.text
    return t.split('"')[1].split(",") if '"' in t else []


@tool("get_a_option_codes", "Get ETF option contract codes by underlying and month. "
      "underlying: 510050(50ETF)/510300(300ETF)/588000(科创50ETF)/510500(500ETF).",
      {"underlying": {"type": "string", "description": "Underlying ETF code (default '510050')"},
       "call": {"type": "boolean", "description": "True=call options, False=put options (default True)"}})
def get_a_option_codes(underlying: str = "510050", call: bool = True) -> str:
    import json as _json
    cached = _cache_get("get_a_option_codes", underlying, str(call))
    if cached:
        return cached
    try:
        cate = {"510050": "50ETF", "510300": "300ETF",
                "588000": "科创50ETF", "510500": "500ETF"}.get(underlying, "50ETF")
        url = ("https://stock.finance.sina.com.cn/futures/api/openapi.php/"
               f"StockOptionService.getStockName?exchange=null&cate={cate}")
        months = _get_requests().get(url, headers=SINA_OPT_HDR, timeout=10) \
            .json()["result"]["data"]["contractMonth"]
        months = [m.replace("-", "")[2:] for m in months[1:]]
        flag = "OP_UP_" if call else "OP_DOWN_"
        out = {}
        for m in months:
            codes = [c.replace("CON_OP_", "") for c in _sina_opt_list(f"{flag}{underlying}{m}")
                     if c.startswith("CON_OP_")]
            if codes:
                out[m] = codes
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_option_codes", result, underlying, str(call))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_option_tquote", "Get ETF option T-quote: bid/ask, open_interest, strike, greeks reference.",
      {"code": {"type": "string", "description": "Option contract code (e.g. '10005798')"}}, required=['code'])
def get_a_option_tquote(code: str) -> str:
    import json as _json
    cached = _cache_get("get_a_option_tquote", code)
    if cached:
        return cached
    try:
        v = _sina_opt_list(f"CON_OP_{code}")
        if len(v) < 43:
            return "{}"
        quote = {
            "bid_vol": _opt_f(v[0]), "bid": _opt_f(v[1]), "last": _opt_f(v[2]),
            "ask": _opt_f(v[3]), "ask_vol": _opt_f(v[4]), "open_interest": _opt_f(v[5]),
            "pct": _opt_f(v[6]), "strike": _opt_f(v[7]), "prev_close": _opt_f(v[8]),
            "open": _opt_f(v[9]), "limit_up": _opt_f(v[10]), "limit_down": _opt_f(v[11]),
            "name": v[37], "amplitude": _opt_f(v[38]), "high": _opt_f(v[39]),
            "low": _opt_f(v[40]), "volume": _opt_f(v[41]), "amount": _opt_f(v[42]),
        }
        result = _json.dumps(quote, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_option_tquote", result, code)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_option_greeks", "Get ETF option Greeks + IV: delta, gamma, theta, vega, IV (implied volatility). "
      "IV is decimal (0.1735 = 17.35%). Pre-computed by exchange, no local BSM needed.",
      {"code": {"type": "string", "description": "Option contract code (e.g. '10005798')"}}, required=['code'])
def get_a_option_greeks(code: str) -> str:
    import json as _json
    cached = _cache_get("get_a_option_greeks", code)
    if cached:
        return cached
    try:
        raw = _sina_opt_list(f"CON_SO_{code}")
        if len(raw) < 16:
            return "{}"
        v = [raw[0]] + raw[4:]
        greeks = {
            "name": v[0], "volume": _opt_f(v[1]),
            "delta": _opt_f(v[2]), "gamma": _opt_f(v[3]),
            "theta": _opt_f(v[4]), "vega": _opt_f(v[5]),
            "iv": _opt_f(v[6]),
            "high": _opt_f(v[7]), "low": _opt_f(v[8]),
            "trade_code": v[9], "strike": _opt_f(v[10]),
            "last": _opt_f(v[11]), "theory": _opt_f(v[12]),
        }
        result = _json.dumps(greeks, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_option_greeks", result, code)
        return result
    except Exception as e:
        return fail(e)


# ═══════════════════════════════════════════════
# 54-57. Sentiment / Interaction layer (舆情互动层)
# ═══════════════════════════════════════════════

@tool("get_a_irm_qa", "Get investor Q&A from cninfo 互动易: questions investors asked + company replies. "
      "Unique source for company responses to market rumors/events.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '002594')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 30)"}}, required=['symbol'])
def get_a_irm_qa(symbol: str, page_size: int = 30) -> str:
    import json as _json
    from datetime import datetime as _dt
    code = bare_code(symbol)
    if code is None:
        return symbol_fail(symbol)
    cached = _cache_get("get_a_irm_qa", code, str(page_size))
    if cached:
        return cached
    try:
        r1 = _get_requests().post("https://irm.cninfo.com.cn/newircs/index/queryKeyboardInfo",
            data={"keyWord": code}, headers={"User-Agent": _UA}, timeout=10)
        d1 = r1.json().get("data") or []
        if not d1:
            return "[]"
        org_id = d1[0].get("secid")
        params = {"_t": 1, "stockcode": code, "orgId": org_id,
                  "pageSize": page_size, "pageNum": 1,
                  "keyWord": "", "startDay": "", "endDay": ""}
        r2 = _get_requests().post("https://irm.cninfo.com.cn/newircs/company/question",
            params=params, headers={"User-Agent": _UA}, timeout=10)
        rows = r2.json().get("rows") or []
        out = []
        for it in rows:
            pd_ts = it.get("pubDate")
            out.append({
                "code": it.get("stockCode"),
                "company": it.get("companyShortName"),
                "question": it.get("mainContent"),
                "answer": it.get("attachedContent"),
                "answerer": it.get("attachedAuthor"),
                "ask_time": _dt.fromtimestamp(pd_ts / 1000).strftime("%Y-%m-%d %H:%M") if pd_ts else "",
            })
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_irm_qa", result, code, str(page_size))
        return result
    except Exception as e:
        return fail(e)


_EM_HOT_BODY = {"appId": "appId01", "globalId": "786e4c21-70dc-435a-93bb-38"}


@tool("get_a_hot_rank", "Get THS hot stock rank (同花顺热榜): rank, heat value, concepts, rank change. period=hour/day.",
      {"period": {"type": "string", "description": "'hour' or 'day' (default 'hour')"}})
def get_a_hot_rank(period: str = "hour") -> str:
    import json as _json
    cached = _cache_get("get_a_hot_rank", period)
    if cached:
        return cached
    try:
        r = _get_requests().get("https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock",
            params={"stock_type": "a", "type": period, "list_type": "normal"},
            headers={"User-Agent": _UA}, timeout=10)
        lst = (r.json().get("data") or {}).get("stock_list") or []
        out = []
        for it in lst:
            tag = it.get("tag") or {}
            out.append({
                "rank": it.get("order"), "code": it.get("code"), "name": it.get("name"),
                "heat": it.get("rate"), "pct": it.get("rise_and_fall"),
                "rank_chg": it.get("hot_rank_chg"),
                "concepts": tag.get("concept_tag") or [],
                "tag": tag.get("popularity_tag", ""),
            })
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_hot_rank", result, period)
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_em_hot_rank", "Get Eastmoney popularity rank (东财人气榜): rank, name, price, pct, rank change.",
      {"top": {"type": "integer", "description": "Top N stocks (default 50)"}})
def get_a_em_hot_rank(top: int = 50) -> str:
    import json as _json
    cached = _cache_get("get_a_em_hot_rank", str(top))
    if cached:
        return cached
    try:
        r = em_get("https://emappdata.eastmoney.com/stockrank/getAllCurrentList",
            params={**_EM_HOT_BODY, "marketType": "", "pageNo": 1, "pageSize": top},
            headers={"User-Agent": _UA}, timeout=10, method="POST")
        data = r.json().get("data") or []
        if not data:
            return "[]"
        secids = [("0." if it["sc"].startswith("SZ") else "1.") + it["sc"][2:] for it in data]
        u = em_get("https://push2.eastmoney.com/api/qt/ulist.np/get",
            params={"ut": "f057cbcbce2a86e2866ab8877db1d059", "fltt": 2, "invt": 2,
                    "fields": "f14,f3,f12,f2", "secids": ",".join(secids)},
            headers={"User-Agent": _UA, "Referer": "https://quote.eastmoney.com/"}, timeout=10)
        diff = (u.json().get("data") or {}).get("diff") or []
        if isinstance(diff, dict):
            diff = list(diff.values())
        nm = {x["f12"]: (x.get("f14"), x.get("f2"), x.get("f3")) for x in diff}
        out = []
        for it in data:
            code = it["sc"][2:]
            name, price, pct = nm.get(code, ("", None, None))
            out.append({"rank": it["rk"], "code": code, "name": name,
                        "price": price, "pct": pct, "rank_chg": it.get("hisRc")})
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_em_hot_rank", result, str(top))
        return result
    except Exception as e:
        return fail(e)


@tool("get_a_hot_concept", "Get concepts a stock is being traded under right now (东财个股热门概念命中). "
      "Returns concept name, hit count. Shows 'what story is this stock riding'.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '601127')"}}, required=['symbol'])
def get_a_hot_concept(symbol: str) -> str:
    import json as _json
    norm = normalize_symbol(symbol)
    if norm is None:
        return symbol_fail(symbol)
    market, code = norm
    cached = _cache_get("get_a_hot_concept", market + code)
    if cached:
        return cached
    try:
        prefix = market.upper()
        r = em_get("https://emappdata.eastmoney.com/stockrank/getHotStockRankList",
            json={**_EM_HOT_BODY, "srcSecurityCode": prefix + code},
            headers={"User-Agent": _UA}, timeout=10, method="POST")
        data = r.json().get("data") or []
        out = [{"concept": x.get("conceptName"), "bk": x.get("conceptId"),
                "hit": x.get("hitCount")} for x in data]
        result = _json.dumps(out, ensure_ascii=False)
        if not _is_empty_result(result):  # 空结果不缓存
            _cache_set("get_a_hot_concept", result, market + code)
        return result
    except Exception as e:
        return fail(e)


# ── research_reports：Athena 源里是 /research-reports HTTP 路由 + tools/list 硬编码
# 的特例（不在 TOOLS/HANDLERS，MCP tools/call 调不到）。本仓注册为正式工具，
# 保证 tools/list 与 tools/call 一致。
@tool("research_reports", "Fetch institutional research reports for A-share stocks (batch, last 30 days, Eastmoney).",
      {"codes": {"type": "array", "items": {"type": "string"},
                 "description": "Stock codes, e.g. ['600519', '000001']"}},
      required=["codes"])
def research_reports(codes: list) -> str:
    return json.dumps(fetch_research_reports(codes), ensure_ascii=False, default=str)


# ═══════════════════════════════════════════════
# 全市场截面（universe / breadth）
# ═══════════════════════════════════════════════
#
# 教训（2026-09）：东财 clist 会**静默截断分页**——请求 pz=500 实际只回 100 行，
# 接口异常时甚至只回 2 行。旧写法 `if len(diff) < pz: break` 把"被截断的短页"
# 当成"列表到底了"，于是"全市场"退化成 100 只甚至 2 只，而这种退化**不抛异常**，
# 下游（日线增量更新）照常把结果当全市场用，直接产出错误数据。
# 因此这里统一以响应里的 total 为终止条件，并对退化显式报错：宁可报错，
# 也绝不返回一个静默截断的 universe。

_EM_CLIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"
_FS_SH_SZ = "m:1+t:2,m:1+t:23,m:0+t:6,m:0+t:80"   # 沪主板+科创, 深主板+创业
_FS_BJ = "m:0+t:81+s:2048"                          # 北交所
_UNIVERSE_FLOOR = 1000                              # 全市场量级下限（低于此判为接口异常）


class UniverseDegraded(RuntimeError):
    """universe 来源返回的数据不完整（分页被截断 / 502 / 限流 / 异常短）。

    拒绝把这种结果当"全市场"使用——宁可我方报错，也不给下游一个静默截断的 universe。
    """


# 本地代码表（qlib cn_data 的 instruments/all.txt，由 factor-miner 的日更任务维护）
_UNIVERSE_FILE_ENV = "A_UNIVERSE_FILE"
_UNIVERSE_FILE_DEFAULT = "/app/.qlib/qlib_data/cn_data/instruments/all.txt"
_A_CODE_RE = re.compile(r"^(SH|SZ|BJ)(\d{6})$")
# 号段必须**结合市场**判断：沪市 60/68 是个股，而 sh000300 是沪深300指数、
# 51/58 是 ETF；深市 00/30 是个股，00 在沪市却是指数。只按数字前缀判断会把指数混进 universe。
_A_STOCK_RE = {
    "sh": re.compile(r"^(60\d{4}|68\d{4})$"),
    "sz": re.compile(r"^(00\d{4}|30\d{4})$"),
    "bj": re.compile(r"^(43\d{4}|83\d{4}|87\d{4}|88\d{4}|920\d{3})$"),
}
_TENCENT_BATCH = 60


def _local_universe_codes(include_bj: bool) -> list:
    """从本地代码表读全市场代码（→ sh600519 / sz000001 / bj430047）。"""
    path = os.environ.get(_UNIVERSE_FILE_ENV, _UNIVERSE_FILE_DEFAULT)
    try:
        text = Path(path).read_text()
    except OSError as e:
        raise UniverseDegraded(f"本地代码表不可读 {path}: {e}") from e
    out = []
    for line in text.splitlines():
        m = _A_CODE_RE.match(line.split("\t")[0].strip().upper())
        if not m:
            continue
        mkt, code = m.group(1).lower(), m.group(2)
        if not _A_STOCK_RE[mkt].match(code):
            continue  # 指数/ETF 等非个股
        if mkt == "bj" and not include_bj:
            continue
        out.append(mkt + code)
    if len(out) < _UNIVERSE_FLOOR:
        raise UniverseDegraded(f"本地代码表仅 {len(out)} 只 (< {_UNIVERSE_FLOOR})：文件可能被截断")
    return out


def _tencent_universe_rows(include_bj: bool) -> list:
    """本地代码表 + 腾讯批量行情 → 与东财 clist 行**同构**的 dict 列表。

    腾讯是独立于东财的一条路（东财 clist 今天就在返 502/断连）。字段对齐 f-code，
    使上层过滤/输出逻辑两种来源共用；腾讯没有 industry/list_date，留 None 并在
    输出里用 source 标注，绝不假装两种来源等价。
    """
    codes = _local_universe_codes(include_bj)
    by_code = {c[-6:]: c for c in codes}
    rows, seen = [], 0
    for i in range(0, len(codes), _TENCENT_BATCH):
        chunk = codes[i:i + _TENCENT_BATCH]
        try:
            q = _tencent_fetch([c for c in chunk])
        except Exception:  # noqa: BLE001 — 单批失败不炸整体，末尾按覆盖率兜底
            continue
        for k, d in q.items():
            raw = str(d.get("symbol") or k)
            code = raw[-6:]
            sym = by_code.get(code)
            if not sym:
                continue
            rows.append({
                "f12": code, "f13": "1" if sym.startswith("sh") else "0",
                "f14": d.get("name", ""), "f2": d.get("price"),
                "f3": d.get("pct_change"), "f6": d.get("amount"),
                "f8": d.get("turnover"), "f20": d.get("market_cap"),
                "f21": None, "f26": None, "f100": "",
            })
            seen += 1
    if seen < len(codes) * 0.8:
        raise UniverseDegraded(f"腾讯批量行情只覆盖 {seen}/{len(codes)} 只，拒绝当全市场使用")
    return rows


def _load_market_rows(include_bj: bool, fields: str) -> tuple:
    """全市场行 → (rows, source)。

    东财 clist 优先（字段最全），502/断连/截断时回退"本地代码表 + 腾讯行情"。
    两条路都不可用才抛 UniverseDegraded —— 单点故障不该让整个 universe 消失。
    """
    fs = _FS_SH_SZ + ("," + _FS_BJ if include_bj else "")
    em_err = None
    try:
        return em_clist_all(fs, fields), "eastmoney_clist"
    except Exception as e:  # noqa: BLE001 — 502/断连/UniverseDegraded 统一走回退
        em_err = e
    logger.warning("东财 clist 不可用(%s)，回退腾讯批量行情", type(em_err).__name__)
    return _tencent_universe_rows(include_bj), "tencent_qt"


def _clist_page(fs: str, fields: str, page: int, page_size: int = 100) -> tuple:
    """取一页 → (rows, total)。total 由接口给出，是翻页的唯一权威依据。"""
    r = em_get(_EM_CLIST_URL, params={
        "pn": str(page), "pz": str(page_size), "po": "1", "np": "1",
        "fltt": "2", "invt": "2", "fs": fs, "fields": fields,
    }, headers={"User-Agent": _UA}, timeout=20)
    data = (r.json() or {}).get("data") or {}
    return (data.get("diff") or []), int(data.get("total") or 0)


def em_clist_all(fs: str, fields: str, page_size: int = 100, max_pages: int = 90,
                 min_expected: int = None) -> list:
    """按 total 翻完全部页；任何不完整都抛 UniverseDegraded（绝不静默返回短列表）。

    min_expected 走**晚绑定**读取 _UNIVERSE_FLOOR：写成默认值 `=_UNIVERSE_FLOOR`
    会在定义时求值，此后改模块常量（或按环境调阈值、测试里放宽）都不生效。
    """
    if min_expected is None:
        min_expected = _UNIVERSE_FLOOR
    rows: list = []
    total = 0
    for page in range(1, max_pages + 1):
        diff, total = _clist_page(fs, fields, page, page_size)
        total = max(total, 0)
        if not diff:
            break
        rows.extend(diff)
        if total and len(rows) >= total:
            break
        if len(diff) < page_size:
            break  # 不满一页 = 正常到底
    if total and len(rows) < total:
        raise UniverseDegraded(
            f"clist 只取到 {len(rows)}/{total} 条：分页被截断或限流，拒绝当全市场使用")
    if len(rows) < min_expected:
        raise UniverseDegraded(
            f"clist 仅返回 {len(rows)} 条（< {min_expected}）：接口异常，拒绝当全市场使用")
    return rows


def _symbol_of(code: str, market) -> str:
    """东财 f12/f13 → 规范符号 sh600519 / sz000001 / bj430047。"""
    if _is_bj(code, market):
        mkt = "bj"
    else:
        mkt = "sh" if str(market) == "1" else "sz"
    norm = normalize_symbol(mkt + str(code))
    return (norm[0] + norm[1]) if norm else str(code)


def _is_bj(code: str, market) -> bool:
    """北交所号段 43/83/87/88/920；深市只有 00/30 开头，不会误判。"""
    return str(market) == "0" and str(code)[:1] in ("4", "8", "9")


def _num(v, default=None):
    """东财空值是 '-' 字符串；统一转 float，转不了给 default。"""
    if v is None or v == "-" or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


@tool("get_a_market_snapshot",
      "Whole-market breadth snapshot (全市场涨跌家数/分布). Pages the full A-share list "
      "(~5500 stocks) and returns up/down/flat counts, pct-change buckets, median/mean "
      "change, total turnover. Cold call takes ~1 min (eastmoney throttles to 1 req/s) "
      "then cached. Raises instead of returning a truncated market if the list source "
      "is degraded. Returns object: total, up, down, flat, median_pct, mean_pct, "
      "amount_yi, buckets{...}, limit_up_like, limit_down_like.",
      {"include_bj": {"type": "boolean", "description": "Include Beijing Stock Exchange (default false)"}})
def get_a_market_snapshot(include_bj: bool = False) -> str:
    import json as _json
    cached = _cache_get("get_a_market_snapshot", str(include_bj))
    if cached:
        return cached
    fs = _FS_SH_SZ + ("," + _FS_BJ if include_bj else "")
    try:
        rows, source = _load_market_rows(include_bj, "f3,f12,f14,f6")
    except UniverseDegraded as e:
        return fail(str(e), code="degraded_source",
                    hint="东财 clist 与腾讯行情均不可用；稍后重试，"
                         "或改用 get_a_limit_up_pool / get_a_indices")
    except Exception as e:
        return fail(e)
    pcts, amount = [], 0.0
    for it in rows:
        p = _num(it.get("f3"))
        if p is None:
            continue
        pcts.append(p)
        amount += _num(it.get("f6"), 0.0) or 0.0
    if not pcts:
        return '{"total":0,"up":0,"down":0,"flat":0,"note":"no quotes in snapshot"}'
    pcts.sort()
    n = len(pcts)
    up = sum(1 for p in pcts if p > 0)
    down = sum(1 for p in pcts if p < 0)
    edges = ["跌停附近", "跌5%以上", "跌2-5%", "跌0-2%",
             "涨0-2%", "涨2-5%", "涨5-9.5%", "涨停附近(≥9.5%)"]
    buckets = {name: 0 for name in edges}
    for p in pcts:
        if p <= -9.5:
            buckets["跌停附近"] += 1
        elif p <= -5:
            buckets["跌5%以上"] += 1
        elif p < 0:
            buckets["跌0-2%" if p > -2 else "跌2-5%"] += 1
        elif p < 2:
            buckets["涨0-2%"] += 1
        elif p < 5:
            buckets["涨2-5%"] += 1
        elif p < 9.5:
            buckets["涨5-9.5%"] += 1
        else:
            buckets["涨停附近(≥9.5%)"] += 1
    result = _json.dumps({
        "source": source,
        "scanned": len(rows), "total": n, "up": up, "down": down, "flat": n - up - down,
        "median_pct": round(pcts[n // 2], 3), "mean_pct": round(sum(pcts) / n, 3),
        "amount_yi": round(amount / 1e8, 1),
        "limit_up_like": sum(1 for p in pcts if p >= 9.5),
        "limit_down_like": sum(1 for p in pcts if p <= -9.5),
        "buckets": buckets,
        "note": "total 是有有效涨跌幅的只数（停牌股无涨跌幅不进统计）；"
                "limit_up_like 是 |pct|>=9.5% 的计数（含创业/科创 20% 涨停），"
                "精确涨停家数用 get_a_limit_up_pool",
    }, ensure_ascii=False)
    if not _is_empty_result(result):
        _cache_set("get_a_market_snapshot", result, str(include_bj))
    return result


@tool("get_a_trade_universe",
      "Investable A-share universe for daily screening (可交易域). Applies the exclusions "
      "that matter before ranking: ST/退市 risk (name contains ST/退), suspended (no quote), "
      "recent IPOs (default <60 trading-ish days since listing), optionally Beijing exchange "
      "and a min turnover floor. Returns every exclusion count so the filter is auditable, "
      "plus universe[] with symbol/name/price/change/amount/mktcap/industry/list_date. "
      "Raises instead of returning a truncated universe if the list source is degraded.",
      {"exclude_st": {"type": "boolean", "description": "Drop ST/*ST/退市 (default true)"},
       "exclude_new_days": {"type": "integer", "description": "Drop stocks listed less than N days ago (default 60; 0=off)"},
       "include_bj": {"type": "boolean", "description": "Include Beijing Stock Exchange (default false)"},
       "min_amount_wan": {"type": "number", "description": "Min turnover in 万CNY (default 0)"},
       "exclude_suspended": {"type": "boolean", "description": "Drop stocks with no valid quote (default true)"},
       "limit": {"type": "integer", "description": "Max rows to return (0=all)"}})
def get_a_trade_universe(exclude_st: bool = True, exclude_new_days: int = 60,
                         include_bj: bool = False, min_amount_wan: float = 0.0,
                         exclude_suspended: bool = True, limit: int = 0) -> str:
    import json as _json
    import datetime as _dt
    cache_args = (str(exclude_st), str(exclude_new_days), str(include_bj),
                  str(min_amount_wan), str(exclude_suspended), str(limit))
    cached = _cache_get("get_a_trade_universe", *cache_args)
    if cached:
        return cached
    fs = _FS_SH_SZ + ("," + _FS_BJ if include_bj else "")
    try:
        rows, source = _load_market_rows(include_bj, "f12,f13,f14,f2,f3,f6,f8,f20,f21,f26,f100")
    except UniverseDegraded as e:
        return fail(str(e), code="degraded_source",
                    hint="东财 clist 与腾讯行情均不可用；稍后重试。绝不返回静默截断的 universe")
    except Exception as e:
        return fail(e)
    if source != "eastmoney_clist":
        # 腾讯来源没有行业/上市日：显式说明，避免下游把 None 当"该股无行业"
        logger.warning("universe 走腾讯兜底：industry/list_date 不可用")

    cutoff = None
    if exclude_new_days and exclude_new_days > 0:
        cutoff = (_dt.date.today() - _dt.timedelta(days=int(exclude_new_days))).strftime("%Y%m%d")
    excluded = {"st": 0, "suspended": 0, "new_listing": 0, "bj": 0, "low_amount": 0}
    universe = []
    for it in rows:
        code = str(it.get("f12") or "")
        market = it.get("f13")
        name = str(it.get("f14") or "")
        if _is_bj(code, market) and not include_bj:
            excluded["bj"] += 1
            continue
        if exclude_st and ("ST" in name.upper() or "退" in name):
            excluded["st"] += 1
            continue
        price = _num(it.get("f2"))
        if exclude_suspended and (price is None or price <= 0):
            excluded["suspended"] += 1
            continue
        list_date = str(it.get("f26") or "")
        if cutoff and list_date and list_date > cutoff:
            excluded["new_listing"] += 1
            continue
        amount = _num(it.get("f6"), 0.0) or 0.0
        if min_amount_wan and amount / 1e4 < min_amount_wan:
            excluded["low_amount"] += 1
            continue
        mktcap, float_cap = _num(it.get("f20")), _num(it.get("f21"))
        universe.append({
            "symbol": _symbol_of(code, market),
            "code": code,
            "name": name,
            "price": price,
            "change_pct": _num(it.get("f3")),
            "amount_wan": round(amount / 1e4, 1),
            "turnover_pct": _num(it.get("f8")),
            "mktcap_yi": round(mktcap / 1e8, 2) if mktcap else None,
            "float_mktcap_yi": round(float_cap / 1e8, 2) if float_cap else None,
            "industry": it.get("f100") or "",
            "list_date": list_date or None,
        })
    universe.sort(key=lambda x: x["amount_wan"], reverse=True)
    out = universe[:int(limit)] if limit and int(limit) > 0 else universe
    result = _json.dumps({
        "source": source,
        "scanned": len(rows), "passed": len(universe), "returned": len(out),
        "excluded": excluded,
        "notes": [] if source == "eastmoney_clist" else [
            "source=tencent_qt：industry 与 list_date 不可用（该源不提供），mktcap 为总市值"],
        "filters": {"exclude_st": exclude_st, "exclude_new_days": exclude_new_days,
                    "include_bj": include_bj, "min_amount_wan": min_amount_wan,
                    "exclude_suspended": exclude_suspended},
        "universe": out,
    }, ensure_ascii=False)
    if not _is_empty_result(result):
        _cache_set("get_a_trade_universe", result, *cache_args)
    return result


# ═══════════════════════════════════════════════
# HTTP server (unified data tools)
# ═══════════════════════════════════════════════

VERSION = "1.0.0"


class DataHandler(BaseHTTPRequestHandler):
    """HTTP handler: /health /tools /call-tool /mcp (MCP JSON-RPC)."""

    license_store: "LicenseStore | None" = None

    def log_message(self, format, *args):
        logger.debug("HTTP %s", format % args)

    def _license_key(self) -> str:
        return self.headers.get("X-License-Key", "")

    def _check_license(self) -> tuple:
        """返回 (ok, message)。开放模式或未启用一律放行。"""
        store = self.license_store
        if store and store.enabled:
            return store.check(self._license_key())
        return True, ""

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"status": "ok", "version": VERSION, "tools": len(TOOLS),
                             "auth": bool(self.license_store and self.license_store.enabled)})
        elif self.path == "/tools":
            # Return all registered tools for MCP Registry hot-plug
            self._json(200, {"tools": [t.to_dict() for t in TOOLS.values()]})
        elif self.path == "/metrics":
            # Prometheus 抓取端点：不要求鉴权（只含工具名级聚合，不泄露 key）
            body = METRICS.render().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/quota":
            if not (self.license_store and self.license_store.enabled):
                self._json(200, {"auth": "open", "note": "开放模式，无额度限制"})
                return
            ok, info = self._check_license()
            if not ok:
                self._json(401, {"error": info})
                return
            self._json(200, self.license_store.quota_of(self._license_key()))
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        try:
            body = self._read_body()
            data = json.loads(body)

            if self.path == "/mcp":
                self._handle_mcp_jsonrpc(data)
                return

            if self.path == "/call-tool":
                ok, info = self._check_license()
                if not ok:
                    tool_name = data.get("tool", "")
                    METRICS.inc_call(tool_name, "rejected_license")
                    self._json(401, {"error": info})
                    return
                tool_name = data.get("tool", "")
                tool_args = data.get("arguments", {})
                if tool_name not in HANDLERS:
                    self._json(404, {"error": f"unknown tool: {tool_name}"})
                    return
                # 轻量类型强制：days="5" / symbol=600519 这类写法先转换，
                # 转不了就返回指明参数名与期望类型的错误（不再抛 '>' not supported）
                tool_args, arg_err = coerce_tool_args(tool_name, tool_args)
                if arg_err:
                    METRICS.inc_call(tool_name, "error")
                    self._json(200, {"result": arg_err})
                    return
                t0 = time.time()
                try:
                    result = HANDLERS[tool_name](**tool_args)
                except Exception:
                    METRICS.inc_call(tool_name, "error")
                    METRICS.observe_latency(tool_name, time.time() - t0)
                    raise
                METRICS.inc_call(tool_name, "ok")
                METRICS.observe_latency(tool_name, time.time() - t0)
                self._json(200, {"result": result})
            else:
                self._json(404, {"error": "not found"})
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid JSON"})
        except Exception as e:
            logger.error("handler error: %s", e)
            self._json(500, {"error": str(e)})

    # ── MCP JSON-RPC handler (Streamable HTTP) ──
    def _handle_mcp_jsonrpc(self, data):
        mid = data.get("id")
        method = data.get("method", "")
        params = data.get("params") or {}

        if method == "initialize":
            import uuid as _uuid
            session_id = str(_uuid.uuid4())
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Mcp-Session-Id", session_id)
            self.end_headers()
            resp = {
                "jsonrpc": "2.0", "id": mid,
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "astock-data-mcp", "version": VERSION},
                },
            }
            self.wfile.write(json.dumps(resp).encode())
            return

        if method == "notifications/initialized":
            self._json(200, {"jsonrpc": "2.0", "id": mid, "result": {}})
            return

        if method == "tools/list":
            tools_list = [t.to_dict() for t in TOOLS.values()]
            self._json(200, {"jsonrpc": "2.0", "id": mid, "result": {"tools": tools_list}})
            return

        if method == "tools/call":
            tool_name = params.get("name", "")
            tool_args = params.get("arguments", {})
            # 鉴权：license 模式强制校验 key（initialize/tools/list 保持开放便于发现）
            ok, info = self._check_license()
            if not ok:
                METRICS.inc_call(tool_name, "rejected_license")
                self._json(200, {"jsonrpc": "2.0", "id": mid,
                                 "error": {"code": -32001, "message": info}})
                return
            if tool_name not in HANDLERS:
                self._json(200, {"jsonrpc": "2.0", "id": mid,
                                 "error": {"code": -32601, "message": f"Unknown tool: {tool_name}"}})
                return
            # 额度：先扣再跑（失败不退还——成本已发生）
            store = self.license_store
            if store and store.enabled:
                try:
                    store.consume(self._license_key(), heavy=False)
                except QuotaExceeded as e:
                    METRICS.inc_call(tool_name, "rejected_quota")
                    self._json(200, {"jsonrpc": "2.0", "id": mid,
                                     "error": {"code": -32029, "message": str(e)}})
                    return
            # 轻量类型强制（同 /call-tool）：字符串数字、数字代码先转成声明的类型
            tool_args, arg_err = coerce_tool_args(tool_name, tool_args)
            if arg_err:
                METRICS.inc_call(tool_name, "error")
                self._json(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": arg_err}], "isError": True}})
                return
            t0 = time.time()
            try:
                result = HANDLERS[tool_name](**tool_args)
                # handler 以错误 JSON（{"error": ...}）返回失败时，按 MCP 规范置 isError=True，
                # 否则客户端/LLM 会把失败当正常结果继续用。
                is_err = False
                if isinstance(result, str):
                    try:
                        _parsed = json.loads(result)
                        is_err = isinstance(_parsed, dict) and "error" in _parsed
                    except (ValueError, TypeError):
                        is_err = False
                METRICS.inc_call(tool_name, "error" if is_err else "ok")
                self._json(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": str(result)}],
                    "isError": is_err,
                }})
            except Exception as e:
                METRICS.inc_call(tool_name, "error")
                self._json(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": f"Error: {e}"}],
                    "isError": True,
                }})
            finally:
                METRICS.observe_latency(tool_name, time.time() - t0)
            return

        self._json(200, {"jsonrpc": "2.0", "id": mid,
                         "error": {"code": -32601, "message": f"Unknown method: {method}"}})

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length)

    def _json(self, code, data):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())


# ═══════════════════════════════════════════════
# Server launcher
# ═══════════════════════════════════════════════

def serve_http(host: str = "0.0.0.0", port: int = 50052, license_file: str = "") -> None:
    DataHandler.license_store = LicenseStore(license_file, domain="astock")
    server = ThreadingHTTPServer((host, port), DataHandler)
    logger.warning("astock-data-mcp listening on %s:%d (HTTP, tools=%d, auth=%s)",
                   host, port, len(TOOLS),
                   "on" if DataHandler.license_store.enabled else "open")
    server.serve_forever()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="astock-data-mcp: A股全维数据 HTTP/MCP 服务")
    ap.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "50052")))
    ap.add_argument("--license-file", default=os.environ.get("MCP_LICENSE_FILE", ""),
                    help="license key JSON 路径（env MCP_LICENSE_FILE）；不配置=开放模式")
    args = ap.parse_args()
    serve_http(args.host, args.port, args.license_file)
