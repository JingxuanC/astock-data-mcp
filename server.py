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

import json
import logging
import os
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
from mcp_gateway import LicenseStore, QuotaExceeded

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
    p = _cache_path(tool_name, *args)
    try:
        p.write_text(json.dumps({"ts": time.time(), "value": value}))
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
            "required": required or list(properties.keys()),
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
    if not symbol.startswith(("sh", "sz")):
        symbol = ("sh" if symbol.startswith(("60","68","5","9")) else "sz") + symbol
    url = ("http://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           f"CN_MarketData.getKLineData?symbol={symbol}&scale=240&ma=no&datalen={count}")
    req = urllib.request.Request(url, headers={"User-Agent": "athena-mcp/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = _json.loads(resp.read().decode("utf-8"))
    return [{"date": r["day"], "open": float(r["open"]), "high": float(r["high"]),
             "low": float(r["low"]), "close": float(r["close"]),
             "volume": int(float(r["volume"]))} for r in data] if isinstance(data, list) else []


@tool("get_a_realtime", "A-share realtime quotes (Tencent). Batch: '600519,000001'",
      {"symbol": {"type": "string", "description": "Code or comma-separated codes"}})
def get_a_realtime(symbol: str) -> str:
    import json as _json
    syms = [s.strip() for s in symbol.split(",") if s.strip()]
    if not syms:
        return "[]"
    full = [s if s.startswith(("sh","sz")) else ("sh"+s if s.startswith(("60","68","5","9")) else "sz"+s) for s in syms]
    try:
        data = _tencent_fetch(full)
        return _json.dumps(list(data.values()), ensure_ascii=False)
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_hist", "A-share daily K-line (Sina source).",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "count": {"type": "integer", "description": "Number of bars (default 100)"},
       "start": {"type": "string", "description": "Start date YYYY-MM-DD (optional, range query)"},
       "end": {"type": "string", "description": "End date YYYY-MM-DD (optional, range query)"}})
def get_a_hist(symbol: str, count: int = 100, start: str = None, end: str = None) -> str:
    import json as _json
    cached = _cache_get("get_a_hist", symbol, str(count), start or "", end or "")
    if cached:
        return cached
    try:
        if start or end:
            # Range query: fetch the max Sina window, then filter by [start, end].
            data = _sina_fetch_klines(symbol, 1023)
            s, e = (start or "")[:10], (end or "")[:10]
            data = [d for d in data
                    if (not s or d["date"][:10] >= s) and (not e or d["date"][:10] <= e)]
        else:
            data = _sina_fetch_klines(symbol, count)
        result = _json.dumps(data, ensure_ascii=False)
        _cache_set("get_a_hist", result, symbol, str(count), start or "", end or "")
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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

@tool("get_a_indices", "Major A-share index quotes. Use aliases: sh/sz/cy/kc50/hs300/sz50/zz500/bj50",
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
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════════════════════
# 18. Northbound flow (eastmoney push2)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_north_flow", "Northbound capital flow (沪深港通). Returns daily net inflow in CNY.",
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
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════════════════════
# 19. Stock search (Tencent smartbox)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_search", "Search A-share stocks by name or code. Returns matching tickers.",
      {"query": {"type": "string", "description": "Stock name or code (e.g. '茅台', '600519')"}})
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
        # Response format: v_hint="1~茅台~600519~...~0.00~0.00~..."
        if not raw or "~" not in raw:
            return "[]"
        results = []
        for line in raw.strip().split("\n"):
            try:
                content = line.split('="', 1)[1].rstrip('";')
                parts = content.split("~")
                if len(parts) >= 4:
                    results.append({
                        "name": parts[1], "code": parts[2],
                        "market": "SH" if parts[2].startswith(("60","68","5","9")) else "SZ",
                        "type": parts[3],  # GP-A=主板A股, GP-A-KCB=科创板
                    })
            except (IndexError, ValueError):
                continue
        return _json.dumps(results, ensure_ascii=False)
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════════════════════
# 20. A-share financials (eastmoney datacenter)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_financials", "A-share key financial metrics: PE, PB, market cap, EPS (derived from Tencent quote).",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "annual": {"type": "boolean", "description": "True=annual, False=latest quarter (default: True)"}})
def get_a_financials(symbol: str, annual: bool = True) -> str:
    import json as _json
    # Normalize symbol
    sym = symbol.strip()
    if len(sym) == 6 and not sym.startswith(("sh", "sz", "bj")):
        if sym.startswith(("6", "9")):
            sym = "sh" + sym
        elif sym.startswith("8"):
            sym = "bj" + sym
        else:
            sym = "sz" + sym
    try:
        # EastMoney datacenter securities API (RPT_DMSK_FN_MAININDICATOR) 已废弃
        # 改用腾讯行情数据派生基本财务指标
        url = f"https://qt.gtimg.cn/q={sym}"
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
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════════════════════
# 21. A-share intraday (Tencent minute bars)
# ═══════════════════════════════════════════════════════════════

@tool("get_a_intraday", "A-share intraday minute price/volume. Returns recent minute bars.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"}})
def get_a_intraday(symbol: str) -> str:
    import json as _json, urllib.request
    code = symbol if symbol.startswith(("sh","sz")) else (
        "sh" + symbol if symbol.startswith(("60","68","5","9")) else "sz" + symbol)
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
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 22-23. Research reports (Eastmoney, replace akshare)
# ═══════════════════════════════════════════════

REPORT_API = "https://reportapi.eastmoney.com/report/list"
PDF_TPL = "https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf"


@tool("get_a_reports", "Get institutional research reports for an A-share stock (Eastmoney). "
      "Returns title, org, date, rating, EPS forecasts, infoCode for PDF download.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "max_pages": {"type": "integer", "description": "Max pages to fetch (default 5)"}})
def get_a_reports(symbol: str, max_pages: int = 5) -> str:
    import json as _json
    cached = _cache_get("get_a_reports", symbol, str(max_pages))
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
                "orgCode": "", "code": symbol, "rcode": "",
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
        _cache_set("get_a_reports", result, symbol, str(max_pages))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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
        _cache_set("get_a_industry_reports", result, industry_code, str(max_pages))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("download_report_pdf", "Download a research report PDF by infoCode. Returns saved file path.",
      {"info_code": {"type": "string", "description": "Report infoCode from get_a_reports record"},
       "target_dir": {"type": "string", "description": "Save directory (default ./reports)"}})
def download_report_pdf(info_code: str, target_dir: str = "./reports") -> str:
    import re as _re
    from pathlib import Path as _Path
    try:
        url = PDF_TPL.format(info_code=info_code)
        r = em_get(url, headers={"Referer": "https://data.eastmoney.com/"}, timeout=60)
        if r.status_code == 200 and len(r.content) >= 1024:
            target = _Path(target_dir)
            target.mkdir(parents=True, exist_ok=True)
            fpath = target / f"H3_{info_code}_1.pdf"
            fpath.write_bytes(r.content)
            return '{"path":"%s","size":%d}' % (str(fpath), len(r.content))
        return '{"error":"Download failed, status=%d size=%d"}' % (r.status_code, len(r.content))
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 24. THS consensus EPS forecast
# ═══════════════════════════════════════════════

@tool("get_a_eps_forecast", "Get institutional consensus EPS forecast from THS (10jqka). "
      "Returns analyst count, min/mean/max EPS per year. 'mean' = consensus. "
      "Warning: analyst_count < 3 = low confidence.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"}})
def get_a_eps_forecast(symbol: str) -> str:
    import json as _json
    from io import StringIO as _StringIO
    cached = _cache_get("get_a_eps_forecast", symbol)
    if cached:
        return cached
    try:
        url = f"https://basic.10jqka.com.cn/new/{symbol}/worth.html"
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
                _cache_set("get_a_eps_forecast", result, symbol)
                return result
        result = dfs[0].to_json(orient="records", force_ascii=False) if dfs else "[]"
        _cache_set("get_a_eps_forecast", result, symbol)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 25-32. Signal layer (信号层)
# ═══════════════════════════════════════════════

@tool("get_a_hot_reason", "Get today's strong stocks with reason tags from THS editorial. "
      "Returns name, code, reason (题材归因), change%, turnover, DDX. ~125 stocks, 73ms latency.",
      {"date": {"type": "string", "description": "Date YYYY-MM-DD (default today)"}})
def get_a_hot_reason(date: str = None) -> str:
    import json as _json
    from datetime import date as _date
    cached = _cache_get("get_a_hot_reason", date or "today")
    if cached:
        return cached
    if date is None:
        date = _date.today().strftime("%Y-%m-%d")
    try:
        url = (f"http://zx.10jqka.com.cn/event/api/getharden/"
               f"date/{date}/orderby/date/orderway/desc/charset/GBK/")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0 Safari/537.36"}
        r = _get_requests().get(url, headers=headers, timeout=10)
        data = r.json()
        if data.get("errocode", 0) != 0:
            return '{"error":"%s"}' % data.get("errormsg", "unknown")
        rows = data.get("data") or []
        result = _json.dumps(rows, ensure_ascii=False)
        _cache_set("get_a_hot_reason", result, date or "today")
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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
        _cache_set("get_a_north_flow_minute", result, str(save_snapshot))
        if save_snapshot and not d.get("time"):
            pass  # Market closed, no data to save
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_concept_blocks", "Get all sector/concept/region blocks a stock belongs to (Eastmoney slist). "
      "Returns board names, BK codes, change%, and lead stock. Used for theme attribution.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"}})
def get_a_concept_blocks(symbol: str) -> str:
    import json as _json
    cached = _cache_get("get_a_concept_blocks", symbol)
    if cached:
        return cached
    try:
        market_code = 1 if symbol.startswith("6") else 0
        params = {
            "fltt": "2", "invt": "2",
            "secid": f"{market_code}.{symbol}",
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
        _cache_set("get_a_concept_blocks", result, symbol)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_fund_flow_minute", "Intraday minute-level fund flow (主力/超大单/大单/中单/小单 net inflow). "
      "Unit: CNY. Use klt=1 for minute, klt=5 for 5-min, klt=101 for daily.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '000858')"},
       "klt": {"type": "integer", "description": "Bar size: 1=minute, 5=5min, 101=daily (default 1)"}})
def get_a_fund_flow_minute(symbol: str, klt: int = 1) -> str:
    import json as _json
    cached = _cache_get("get_a_fund_flow_minute", symbol, str(klt))
    if cached:
        return cached
    try:
        secid = f"1.{symbol}" if symbol.startswith("6") else f"0.{symbol}"
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
        _cache_set("get_a_fund_flow_minute", result, symbol, str(klt))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_dragon_tiger", "Get dragon tiger board (龙虎榜) records for a stock:上榜记录 + 买卖席位 TOP5 + 机构动向.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '002475')"},
       "trade_date": {"type": "string", "description": "Trade date YYYY-MM-DD"},
       "look_back": {"type": "integer", "description": "Look-back days (default 30)"}})
def get_a_dragon_tiger(symbol: str, trade_date: str, look_back: int = 30) -> str:
    import json as _json
    from datetime import datetime as _dt, timedelta as _td
    cached = _cache_get("get_a_dragon_tiger", symbol, trade_date, str(look_back))
    if cached:
        return cached
    try:
        start = _dt.strptime(trade_date, "%Y-%m-%d") - _td(days=look_back)
        start_str = start.strftime("%Y-%m-%d")
        records = []
        data = eastmoney_datacenter(
            "RPT_DAILYBILLBOARD_DETAILSNEW",
            filter_str=f"(TRADE_DATE>='{start_str}')(TRADE_DATE<='{trade_date}')(SECURITY_CODE=\"{symbol}\")",
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
                filter_str=f"(TRADE_DATE='{latest_date}')(SECURITY_CODE=\"{symbol}\")",
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
                filter_str=f"(TRADE_DATE='{latest_date}')(SECURITY_CODE=\"{symbol}\")",
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
        _cache_set("get_a_dragon_tiger", result, symbol, trade_date, str(look_back))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_lockup_expiry", "Get lockup expiry calendar (限售解禁): historical + upcoming 90 days.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '002475')"},
       "trade_date": {"type": "string", "description": "Trade date YYYY-MM-DD"},
       "forward_days": {"type": "integer", "description": "Days to look forward (default 90)"}})
def get_a_lockup_expiry(symbol: str, trade_date: str, forward_days: int = 90) -> str:
    import json as _json
    from datetime import datetime as _dt, timedelta as _td
    cached = _cache_get("get_a_lockup_expiry", symbol, trade_date, str(forward_days))
    if cached:
        return cached
    try:
        history_data = eastmoney_datacenter(
            "RPT_LIFT_STAGE",
            filter_str=f'(SECURITY_CODE="{symbol}")',
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
            filter_str=f'(SECURITY_CODE="{symbol}")(FREE_DATE>=\'{trade_date}\')(FREE_DATE<=\'{end_str}\')',
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
        _cache_set("get_a_lockup_expiry", result, symbol, trade_date, str(forward_days))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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
        _cache_set("get_a_industry_rank", result, str(top_n))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_daily_dragon_tiger", "Get market-wide dragon tiger board for a day (全市场龙虎榜). "
      "Returns all stocks hitting the board with net buy amounts.",
      {"trade_date": {"type": "string", "description": "Trade date YYYY-MM-DD (default today)"},
       "min_net_buy_wan": {"type": "number", "description": "Min net buy in 万CNY filter (optional)"}})
def get_a_daily_dragon_tiger(trade_date: str = None, min_net_buy_wan: float = None) -> str:
    import json as _json
    from datetime import datetime as _dt
    if trade_date is None:
        trade_date = _dt.now().strftime("%Y-%m-%d")
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
        _cache_set("get_a_daily_dragon_tiger", result, trade_date, str(min_net_buy_wan or ""))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 33-37. Capital / Position layer (资金面/筹码层)
# ═══════════════════════════════════════════════

@tool("get_a_margin", "Get margin trading details (融资融券): balance, buy, repay, short balance per day.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 30)"}})
def get_a_margin(symbol: str, page_size: int = 30) -> str:
    import json as _json
    cached = _cache_get("get_a_margin", symbol, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPTA_WEB_RZRQ_GGMX",
            filter_str=f'(SCODE="{symbol}")',
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
        _cache_set("get_a_margin", result, symbol, str(page_size))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_block_trade", "Get block trade records (大宗交易): price, volume, buyer/seller, premium%.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 20)"}})
def get_a_block_trade(symbol: str, page_size: int = 20) -> str:
    import json as _json
    cached = _cache_get("get_a_block_trade", symbol, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPT_DATA_BLOCKTRADE",
            filter_str=f'(SECURITY_CODE="{symbol}")',
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
        _cache_set("get_a_block_trade", result, symbol, str(page_size))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_holder_num", "Get shareholder count changes (股东户数变化). Fewer holders = concentration = accumulation signal.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 10)"}})
def get_a_holder_num(symbol: str, page_size: int = 10) -> str:
    import json as _json
    cached = _cache_get("get_a_holder_num", symbol, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPT_HOLDERNUMLATEST",
            filter_str=f'(SECURITY_CODE="{symbol}")',
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
        _cache_set("get_a_holder_num", result, symbol, str(page_size))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_dividend", "Get dividend history (分红送转): bonus per share, transfer ratio, bonus ratio.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 20)"}})
def get_a_dividend(symbol: str, page_size: int = 20) -> str:
    import json as _json
    cached = _cache_get("get_a_dividend", symbol, str(page_size))
    if cached:
        return cached
    try:
        data = eastmoney_datacenter(
            "RPT_SHAREBONUS_DET",
            filter_str=f'(SECURITY_CODE="{symbol}")',
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
        _cache_set("get_a_dividend", result, symbol, str(page_size))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_fund_flow_120d", "Get daily fund flow for last 120 trading days: main/super/large/mid/small net. Unit: CNY.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"}})
def get_a_fund_flow_120d(symbol: str) -> str:
    import json as _json
    cached = _cache_get("get_a_fund_flow_120d", symbol)
    if cached:
        return cached
    try:
        market_code = 1 if symbol.startswith("6") else 0
        url = "https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
        params = {
            "secid": f"{market_code}.{symbol}",
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
        _cache_set("get_a_fund_flow_120d", result, symbol)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 38-39. News layer (新闻层)
# ═══════════════════════════════════════════════

@tool("get_a_news", "Get stock-specific news from Eastmoney (个股新闻). Returns title, content, time, source, URL.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "page_size": {"type": "integer", "description": "Articles to fetch (default 20)"}})
def get_a_news(symbol: str, page_size: int = 20) -> str:
    import json as _json, re as _re
    cached = _cache_get("get_a_news", symbol, str(page_size))
    if cached:
        return cached
    try:
        cb = "jQuery_news"
        url = "https://search-api-web.eastmoney.com/search/jsonp"
        inner_params = _json.dumps({
            "uid": "", "keyword": symbol,
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
        _cache_set("get_a_news", result, symbol, str(page_size))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)

# ═══════════════════════════════════════════════
# 40-43. Fundamental data layer (基础数据层)
# ═══════════════════════════════════════════════

@tool("get_a_mootdx_finance", "Get 37-field quarterly financial snapshot via mootdx: EPS, ROE, BVPS, revenue, profit, etc.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"}})
def get_a_mootdx_finance(symbol: str) -> str:
    import json as _json
    cached = _cache_get("get_a_mootdx_finance", symbol)
    if cached:
        return cached
    try:
        client = tdx_client()
        market = 1 if symbol.startswith("6") else 0
        fin = client.finance(symbol=symbol)
        # Convert to serializable dict
        result = _json.dumps(dict(fin) if hasattr(fin, 'items') else str(fin), ensure_ascii=False)
        _cache_set("get_a_mootdx_finance", result, symbol)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_mootdx_f10", "Get company F10 text data (9 categories): 最新提示/公司概况/财务分析/股东研究/股本结构/资本运作/业内点评/行业分析/公司大事.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "category": {"type": "string", "description": "Category: 最新提示/公司概况/财务分析/股东研究/股本结构/资本运作/业内点评/行业分析/公司大事 (default 最新提示)"}})
def get_a_mootdx_f10(symbol: str, category: str = "最新提示") -> str:
    import json as _json
    cached = _cache_get("get_a_mootdx_f10", symbol, category)
    if cached:
        return cached
    try:
        client = tdx_client()
        text = client.F10(symbol=symbol, name=category)
        result = _json.dumps({"category": category, "content": text or ""}, ensure_ascii=False)
        _cache_set("get_a_mootdx_f10", result, symbol, category)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_stock_info", "Get A-share basic info: industry, total/float shares, market cap, list date (Eastmoney push2).",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"}})
def get_a_stock_info(symbol: str) -> str:
    import json as _json
    cached = _cache_get("get_a_stock_info", symbol)
    if cached:
        return cached
    try:
        market_code = 1 if symbol.startswith("6") else 0
        url = "https://push2.eastmoney.com/api/qt/stock/get"
        params = {
            "fltt": "2", "invt": "2",
            "fields": "f57,f58,f84,f85,f127,f116,f117,f189,f43",
            "secid": f"{market_code}.{symbol}",
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
        _cache_set("get_a_stock_info", result, symbol)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_financial_statements", "Get Sina financial statements (新浪财报三表): balance sheet, income statement, cash flow.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '600519')"},
       "report_type": {"type": "string", "description": "'fzb'=balance sheet, 'lrb'=income, 'llb'=cashflow (default 'lrb')"},
       "num": {"type": "integer", "description": "Number of periods (default 8)"}})
def get_a_financial_statements(symbol: str, report_type: str = "lrb", num: int = 8) -> str:
    import json as _json
    cached = _cache_get("get_a_financial_statements", symbol, report_type, str(num))
    if cached:
        return cached
    try:
        prefix = "sh" if symbol.startswith("6") else "sz"
        paper_code = f"{prefix}{symbol}"
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
        _cache_set("get_a_financial_statements", result, symbol, report_type, str(num))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 44. Announcement layer (公告层 - 巨潮)
# ═══════════════════════════════════════════════

_CNINFO_ORGID_MAP = {}

def _cninfo_orgid(code: str) -> str:
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
    if code.startswith("6"):
        return f"gssh0{code}"
    elif code.startswith("8") or code.startswith("4"):
        return f"gsbj0{code}"
    return f"gssz0{code}"


@tool("get_a_announcements", "Get full-text announcements from cninfo (巨潮公告). Returns title, type, date, URL.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '688017')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 30)"}})
def get_a_announcements(symbol: str, page_size: int = 30) -> str:
    import json as _json
    from datetime import datetime as _dt
    cached = _cache_get("get_a_announcements", symbol, str(page_size))
    if cached:
        return cached
    try:
        url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
        org_id = _cninfo_orgid(symbol)
        payload = {
            "stock": f"{symbol},{org_id}",
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
        _cache_set("get_a_announcements", result, symbol, str(page_size))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 45-50. Limit-up / Board layer (打板层)
# ═══════════════════════════════════════════════

_ZTB_UT = "7eea3edcaed734bea9cbfc24409ed989"

def _fmt_zt_time(t) -> str:
    s = str(t).zfill(6)
    return f"{s[0:2]}:{s[2:4]}:{s[4:6]}"

def _em_zt_api(endpoint: str, sort: str, date: str) -> list:
    date = str(date).replace("-", "")  # 东财 push2ex 要 YYYYMMDD；调用方(如 SentimentAnalyst)传 YYYY-MM-DD 需归一化，否则 data:null
    url = f"https://push2ex.eastmoney.com/{endpoint}"
    params = {"ut": _ZTB_UT, "dpt": "wz.ztzt", "Pageindex": 0,
              "pagesize": 10000, "sort": sort, "date": date}
    headers = {"User-Agent": _UA, "Referer": "https://quote.eastmoney.com/"}
    try:
        r = em_get(url, params=params, headers=headers, timeout=10)
        return (r.json().get("data") or {}).get("pool") or []
    except Exception:
        return []


@tool("get_a_limit_up_pool", "Get today's limit-up pool (涨停池): name, code, price, pct, limit_days, seal time, seal fund, break_times, industry.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD (e.g. '20260626')"}})
def get_a_limit_up_pool(date: str) -> str:
    import json as _json
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
        _cache_set("get_a_limit_up_pool", result, date)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_broken_board", "Get today's broken board pool (炸板池): stocks that hit limit-up then opened.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD (e.g. '20260626')"}})
def get_a_broken_board(date: str) -> str:
    import json as _json
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
        _cache_set("get_a_broken_board", result, date)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_limit_down_pool", "Get today's limit-down pool (跌停池).",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD (e.g. '20260626')"}})
def get_a_limit_down_pool(date: str) -> str:
    import json as _json
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
        _cache_set("get_a_limit_down_pool", result, date)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_yesterday_zt", "Get yesterday's limit-up performance today (昨涨停今表现). Used to calculate promotion rate.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD (e.g. '20260626')"}})
def get_a_yesterday_zt(date: str) -> str:
    import json as _json
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
        _cache_set("get_a_yesterday_zt", result, date)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_zt_reason", "Get limit-up reason/themes from THS (同花顺涨停揭秘): reason tags, board type, seal rate.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD (e.g. '20260626')"}})
def get_a_zt_reason(date: str) -> str:
    import json as _json
    from datetime import datetime as _dt
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
        _cache_set("get_a_zt_reason", result, date)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_board_emotion", "Calculate limit-up sentiment: break_rate, max_height, ladder (连板梯队), ZT/DT counts. "
      "Break rate >40% = bearish, <20% = bullish.",
      {"date": {"type": "string", "description": "Trade date YYYYMMDD (e.g. '20260626')"}})
def get_a_board_emotion(date: str) -> str:
    import json as _json
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
        _cache_set("get_a_board_emotion", result, date)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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
        _cache_set("get_a_option_codes", result, underlying, str(call))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_option_tquote", "Get ETF option T-quote: bid/ask, open_interest, strike, greeks reference.",
      {"code": {"type": "string", "description": "Option contract code (e.g. '10005798')"}})
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
        _cache_set("get_a_option_tquote", result, code)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_option_greeks", "Get ETF option Greeks + IV: delta, gamma, theta, vega, IV (implied volatility). "
      "IV is decimal (0.1735 = 17.35%). Pre-computed by exchange, no local BSM needed.",
      {"code": {"type": "string", "description": "Option contract code (e.g. '10005798')"}})
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
        _cache_set("get_a_option_greeks", result, code)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


# ═══════════════════════════════════════════════
# 54-57. Sentiment / Interaction layer (舆情互动层)
# ═══════════════════════════════════════════════

@tool("get_a_irm_qa", "Get investor Q&A from cninfo 互动易: questions investors asked + company replies. "
      "Unique source for company responses to market rumors/events.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '002594')"},
       "page_size": {"type": "integer", "description": "Records to fetch (default 30)"}})
def get_a_irm_qa(symbol: str, page_size: int = 30) -> str:
    import json as _json
    from datetime import datetime as _dt
    cached = _cache_get("get_a_irm_qa", symbol, str(page_size))
    if cached:
        return cached
    try:
        r1 = _get_requests().post("https://irm.cninfo.com.cn/newircs/index/queryKeyboardInfo",
            data={"keyWord": symbol}, headers={"User-Agent": _UA}, timeout=10)
        d1 = r1.json().get("data") or []
        if not d1:
            return "[]"
        org_id = d1[0].get("secid")
        params = {"_t": 1, "stockcode": symbol, "orgId": org_id,
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
        _cache_set("get_a_irm_qa", result, symbol, str(page_size))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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
        _cache_set("get_a_hot_rank", result, period)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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
        _cache_set("get_a_em_hot_rank", result, str(top))
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


@tool("get_a_hot_concept", "Get concepts a stock is being traded under right now (东财个股热门概念命中). "
      "Returns concept name, hit count. Shows 'what story is this stock riding'.",
      {"symbol": {"type": "string", "description": "Stock code (e.g. '601127')"}})
def get_a_hot_concept(symbol: str) -> str:
    import json as _json
    cached = _cache_get("get_a_hot_concept", symbol)
    if cached:
        return cached
    try:
        prefix = "SH" if symbol.startswith("6") else "SZ"
        r = em_get("https://emappdata.eastmoney.com/stockrank/getHotStockRankList",
            json={**_EM_HOT_BODY, "srcSecurityCode": prefix + symbol},
            headers={"User-Agent": _UA}, timeout=10, method="POST")
        data = r.json().get("data") or []
        out = [{"concept": x.get("conceptName"), "bk": x.get("conceptId"),
                "hit": x.get("hitCount")} for x in data]
        result = _json.dumps(out, ensure_ascii=False)
        _cache_set("get_a_hot_concept", result, symbol)
        return result
    except Exception as e:
        return '{"error":"%s"}' % str(e)


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
                    self._json(401, {"error": info})
                    return
                tool_name = data.get("tool", "")
                tool_args = data.get("arguments", {})
                if tool_name not in HANDLERS:
                    self._json(404, {"error": f"unknown tool: {tool_name}"})
                    return
                result = HANDLERS[tool_name](**tool_args)
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
                    self._json(200, {"jsonrpc": "2.0", "id": mid,
                                     "error": {"code": -32029, "message": str(e)}})
                    return
            try:
                result = HANDLERS[tool_name](**tool_args)
                self._json(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": str(result)}],
                    "isError": False,
                }})
            except Exception as e:
                self._json(200, {"jsonrpc": "2.0", "id": mid, "result": {
                    "content": [{"type": "text", "text": f"Error: {e}"}],
                    "isError": True,
                }})
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
