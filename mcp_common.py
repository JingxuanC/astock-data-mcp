# =======================================================================
# 自动生成 —— 请勿直接编辑本文件
#
#   源    : mcp-common/mcp_common.py   (mcp-suite 仓库)
#   指纹  : sha256:d7f18017861997a552e8eb94a26a206c3892363553a66f891fb3998ddf0f0e8f
#   同步  : python3 mcp-common/sync.py
#   校验  : python3 mcp-common/sync.py --check      (CI 门禁)
#
# 直接修改本文件会在下次 sync 时被覆盖，且 --check 会失败。要改公共函数，
# 请改 canonical 源文件后重新 sync。
# =======================================================================

"""mcp_common — mcp-suite 各 MCP 服务的共享原语（唯一真源）。

为什么存在
──────────
astock-data / global-data / factor-miner / causal / kronos 五个服务是从同一份
模板复制出来的，``fail()``、参数类型强制、A 股代码/日期归一化、空结果判定、
日志初始化这些逻辑在各仓各写一份。后果是**同一类 bug 要修 N 遍**，而且修不全：

* ``required or list(properties.keys())`` 的 falsy 陷阱在 5 个服务里各出现一次，
  线上导致「声明 required=[] 的工具被标成全部必填」；
* ``'{"error":"%s"}' % str(e)`` 未转义，异常信息带引号时产出非法 JSON；
* 类型强制只在 astock/global 有，causal/kronos/factor-miner 直接 ``**args`` 解包，
  LLM 把 ``count="5"`` 传进来就在 ``min(max(...))`` 处抛 TypeError；
* A 股代码识别规则在 astock 内有 3 套并存，北交所 8xx 号段判错。

本模块是这些原语的唯一真源。各服务的副本由 ``mcp-common/sync.py`` 从本文件
生成（带源指纹），``sync.py --check`` 可在 CI 里校验副本是否漂移。

设计约束
────────
* **零第三方依赖**：只用标准库。各服务镜像里不一定装了同一套包。
* **不导入任何服务模块**：``coerce_args`` 显式接收 ``handlers`` 映射，
  避免循环导入，也便于单测。
* **行为与各服务现行实现一致**：本模块由现有实现归并而成（取各仓最完整的
  版本），替换后调用点无需改动。

用法
────
::

    from mcp_common import fail, coerce_args, normalize_cn_symbol, setup_logging

    logger = setup_logging("astock-data-mcp")

    def coerce_tool_args(tool_name, args):
        return coerce_args(HANDLERS, tool_name, args)   # 绑定本服务的注册表
"""

from __future__ import annotations

import inspect
import json
import logging
import re
from datetime import datetime
from typing import Any, Optional

__version__ = "1.0.0"

__all__ = [
    # 错误出口
    "fail", "ok",
    # 空结果 / 失败判定
    "is_empty_result", "tool_failed",
    # schema
    "input_schema",
    # 参数类型强制
    "coerce_args", "make_coercer", "arg_fail",
    # A 股代码
    "normalize_cn_symbol", "symbol_fail", "bare_code", "tencent_symbol",
    "eastmoney_secid", "SYMBOL_HINT",
    # 日期
    "normalize_date", "date_fail", "date_arg", "DATE_HINT",
    # 日志
    "setup_logging",
]


# ═══════════════════════════════════════════════════════════════
# 1. 错误出口
# ═══════════════════════════════════════════════════════════════

def fail(msg: Any, code: str = "error", hint: str = "") -> str:
    """统一错误出口：保证合法 JSON，并让 LLM 能据此纠正。

    旧写法用 ``%s`` 直接拼异常字符串、未做转义，异常信息里带引号时
    （例如 ``KeyError:'data'``）会产出非法 JSON，客户端解析直接失败。
    这里统一走 ``json.dumps``。

    Args:
        msg: 面向调用方的错误描述。
        code: 机器可读的错误类别（``error`` / ``invalid_symbol`` / …）。
        hint: 可操作的修复建议（怎么写才对）。

    Returns:
        形如 ``{"error": "...", "code": "...", "hint": "..."}`` 的 JSON 串；
        ``hint`` 为空时省略该字段。
    """
    payload = {"error": str(msg), "code": code}
    if hint:
        payload["hint"] = hint
    return json.dumps(payload, ensure_ascii=False)


def ok(payload: Any) -> str:
    """成功出口：统一 ``json.dumps(ensure_ascii=False)``，中文不转义。"""
    return json.dumps(payload, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
# 2. 空结果 / 失败判定
# ═══════════════════════════════════════════════════════════════

# 只描述"这层结构本身"的元数据键：它们的存在不证明拿到了业务数据
_META_KEYS = {"date", "total", "total_records", "count", "category", "code",
              "symbol", "source", "note", "market", "ts"}


def is_empty_result(value: Any) -> bool:
    """空结果判定：None / 空容器 / 空 JSON 串（``[]``、``{}``）/ 全空字段都算空。

    上游限流或空响应常返回 ``[]``/``{}``，若照常写缓存，TTL 内所有调用方都会
    拿到假"无数据"。注意 ``json.dumps([]) == "[]"`` 是**非空字符串**，所以
    不能只判断字符串真值，必须反解 JSON 再判空。
    """
    if value is None:
        return True
    if isinstance(value, str):
        s = value.strip()
        if not s or s in ("[]", "{}", "null", "None"):
            return True
        try:
            return is_empty_result(json.loads(s))
        except (ValueError, TypeError):
            return False  # 非 JSON 文本（如纯文本行情）视为有内容
    if isinstance(value, bool):
        return not value
    if isinstance(value, (int, float)):
        return value == 0
    if isinstance(value, (list, tuple, set)):
        return len(value) == 0 or all(is_empty_result(v) for v in value)
    if isinstance(value, dict):
        if not value:
            return True
        data_vals = [v for k, v in value.items() if k not in _META_KEYS]
        if not data_vals:
            return False  # 只有元数据字段，不足以判定为空
        return all(is_empty_result(v) for v in data_vals)
    return False


def tool_failed(result: Any) -> bool:
    """工具层失败判定 → 服务层据此置 ``isError=True``。

    handler 内部普遍吞掉异常并返回错误文本，因此服务层需要识别失败结果，
    才能像异常路径一样置 ``isError``：

    * 统一错误出口 ``fail()`` 产出的 JSON 对象（含 ``"error"`` 键）
    * 声称是 JSON 却解析失败（非法 JSON 输出 = 工具损坏）
    * 文本错误约定 ``"ERROR: ..."`` 前缀

    ``NO_DATA`` / ``NO_MATCH`` / ``"No ... found"`` 是**合法空结果**，不算失败。
    """
    text = str(result).lstrip()
    if text.startswith("ERROR:"):
        return True
    if text.startswith("{"):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            return True
        return isinstance(obj, dict) and "error" in obj
    return False


# ═══════════════════════════════════════════════════════════════
# 3. schema 生成（含 required falsy 陷阱的正确写法）
# ═══════════════════════════════════════════════════════════════

def input_schema(properties: dict, required: Optional[list] = None,
                 description: str = "") -> dict:
    """生成 MCP ``inputSchema``。

    **务必用本函数或 ``required or []``**：``required or list(properties)``
    在 ``required=[]``（显式声明"无必填参数"）时会被 ``or`` 短路成
    "全部必填"，线上已因此产生过真实的工具调用失败。
    """
    schema = {"type": "object", "properties": properties,
              "required": list(required or [])}
    if description:
        schema["description"] = description
    return schema


# ═══════════════════════════════════════════════════════════════
# 4. 参数类型强制
#
# 调用方是 LLM Agent（背后是不会写提示词的散户），把数字传成字符串
# （``lookback_days="60"``）或显式传 null 是常态。直接 ``HANDLERS[name](**args)``
# 解包会在 min/max 比较或切片处抛
# "'>' not supported between instances of 'str' and 'int'"，LLM 无法据此纠正。
# 这里按 handler 签名注解做轻量转换；转不了就返回点明参数名与期望类型的错误。
# ═══════════════════════════════════════════════════════════════

_ANN_NAME = {int: "integer", float: "number", bool: "boolean",
             str: "string", list: "array", dict: "object"}
_BOOL_TRUE = {"true", "1", "yes", "y", "on"}
_BOOL_FALSE = {"false", "0", "no", "n", "off"}
_EXAMPLE = {"integer": "5", "number": "5.0", "boolean": "true",
            "string": "'600519'", "array": "['600519']", "object": "{}"}


def _ann_type(ann: Any) -> Optional[type]:
    """注解 → 真实类型。

    有些服务用了 ``from __future__ import annotations``，注解是**字符串**
    （``'int'``）；另一些仓是真实类型对象。两种都要认。
    """
    if ann is inspect.Parameter.empty or ann is None:
        return None
    if isinstance(ann, str):
        return {"int": int, "float": float, "bool": bool, "str": str,
                "list": list, "dict": dict}.get(ann.strip())
    return ann


def arg_fail(name: str, expected: str, value: Any) -> str:
    """参数类型错误 → 点名参数、期望类型、实际值，并给一个可照抄的示例。"""
    return fail("参数 '%s' 类型错误：期望 %s，实际收到 %r（%s）"
                % (name, expected, value, type(value).__name__),
                code="invalid_argument",
                hint="参数 '%s' 请传 %s 类型，例如 %s=%s"
                     % (name, expected, name, _EXAMPLE.get(expected, "值")))


def coerce_args(handlers: dict, tool_name: str, args: Any):
    """调用 handler 前做轻量类型强制 → ``(新参数 dict, None)`` 或 ``(None, fail_json)``。

    Args:
        handlers: 该服务的 ``{tool_name: callable}`` 注册表。
        tool_name: 工具名（未注册则原样放行，交给上层报"工具不存在"）。
        args: 原始参数 dict。

    Returns:
        ``(args, None)`` 正常；``(None, err_json)`` 参数不可用。
    """
    fn = handlers.get(tool_name)
    if fn is None or not isinstance(args, dict):
        return args, None
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return args, None
    params = sig.parameters
    out = dict(args)

    # 未声明的参数：明确报错（旧实现直接解包 → TypeError: unexpected keyword）
    if not any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        unknown = [k for k in out if k not in params]
        if unknown:
            return None, fail(
                "工具 %s 不支持参数 %s" % (tool_name, ", ".join("'%s'" % u for u in unknown)),
                code="unknown_argument",
                hint="支持的参数: " + (", ".join(params) or "（无）"))

    for name, val in list(out.items()):
        if name not in params:
            continue
        p = params[name]
        if p.kind in (inspect.Parameter.VAR_KEYWORD, inspect.Parameter.VAR_POSITIONAL):
            continue
        ann = _ann_type(p.annotation)
        if ann is None:
            continue

        if val is None:
            # 显式传 null：有默认值就当没传（旧实现会崩在 min(max(days,1),60)）
            if p.default is not inspect.Parameter.empty:
                out.pop(name, None)
            else:
                return None, fail("参数 '%s' 不能为 null" % name, code="invalid_argument",
                                  hint="参数 '%s' 是必填项，请提供 %s 类型的值"
                                       % (name, _ANN_NAME.get(ann, "正确")))
            continue

        if ann is int:
            if isinstance(val, bool):
                out[name] = int(val)
            elif isinstance(val, int):
                continue
            elif isinstance(val, float):
                if float(val).is_integer():
                    out[name] = int(val)
                else:
                    return None, arg_fail(name, "integer", val)
            elif isinstance(val, str):
                try:
                    out[name] = int(val.strip())
                except ValueError:
                    try:
                        f = float(val.strip())
                    except ValueError:
                        return None, arg_fail(name, "integer", val)
                    if not f.is_integer():
                        return None, arg_fail(name, "integer", val)
                    out[name] = int(f)
            else:
                return None, arg_fail(name, "integer", val)

        elif ann is float:
            if isinstance(val, bool):
                out[name] = float(val)
            elif isinstance(val, (int, float)):
                out[name] = float(val)
            elif isinstance(val, str):
                try:
                    out[name] = float(val.strip())
                except ValueError:
                    return None, arg_fail(name, "number", val)
            else:
                return None, arg_fail(name, "number", val)

        elif ann is bool:
            if isinstance(val, bool):
                continue
            if isinstance(val, int) and val in (0, 1):
                out[name] = bool(val)
            elif isinstance(val, str) and val.strip().lower() in _BOOL_TRUE | _BOOL_FALSE:
                out[name] = val.strip().lower() in _BOOL_TRUE
            else:
                return None, arg_fail(name, "boolean", val)

        elif ann is str:
            if isinstance(val, str):
                continue
            if isinstance(val, bool):
                out[name] = str(val)
            elif isinstance(val, int):
                out[name] = str(val)
            elif isinstance(val, float):
                # JSON 里 600519.0 会被解析成 float，还原成 "600519" 而不是 "600519.0"
                out[name] = str(int(val)) if val.is_integer() else str(val)
            else:
                return None, arg_fail(name, "string", val)

        elif ann is list:
            if isinstance(val, list):
                continue
            if isinstance(val, str):
                try:
                    parsed = json.loads(val)
                except (ValueError, TypeError):
                    parsed = [x.strip() for x in val.split(",") if x.strip()]
                if isinstance(parsed, list):
                    out[name] = parsed
                    continue
            return None, arg_fail(name, "array", val)

        elif ann is dict:
            if isinstance(val, dict):
                continue
            if isinstance(val, str):
                try:
                    parsed = json.loads(val)
                except (ValueError, TypeError):
                    parsed = None
                if isinstance(parsed, dict):
                    out[name] = parsed
                    continue
            return None, arg_fail(name, "object", val)

    return out, None


def make_coercer(handlers: dict):
    """→ ``coerce_tool_args(tool_name, args)``，已绑定本服务的注册表。

    各服务保留原有二参函数名，调用点便无需改动::

        from mcp_common import make_coercer
        coerce_tool_args = make_coercer(HANDLERS)
    """
    def coerce_tool_args(tool_name: str, args: Any):
        return coerce_args(handlers, tool_name, args)
    coerce_tool_args.__doc__ = coerce_args.__doc__
    return coerce_tool_args


# ═══════════════════════════════════════════════════════════════
# 5. A 股代码 / 日期归一化（唯一真源）
#
# 同一个业务概念在旧实现里有多套互不兼容的写法，且遇到不认识的值一律静默
# 返回 []，LLM 分不清"代码写错了"和"确实没数据"。非法输入一律返回可操作的
# JSON 错误。
# ═══════════════════════════════════════════════════════════════

# 代码 → 市场 的识别规则表（前缀匹配，2 位优先于 1 位）
#   sh 沪市: 60x 主板 / 68x 科创板 / 90x B股 / 11x 可转债 / 5xxxxx ETF·LOF
#   sz 深市: 00x 主板 / 30x 创业板 / 12x 可转债 / 15x·16x·18x 基金 / 20x B股
#   bj 北交所: 43x / 83x / 87x / 92x
_SYMBOL_PREFIX_MARKET = (
    ("60", "sh"), ("68", "sh"), ("90", "sh"), ("11", "sh"), ("5", "sh"),
    ("00", "sz"), ("30", "sz"), ("12", "sz"), ("15", "sz"),
    ("16", "sz"), ("18", "sz"), ("20", "sz"),
    ("43", "bj"), ("83", "bj"), ("87", "bj"), ("92", "bj"),
)
# 显式市场标记（前缀或后缀）→ 市场；SS=上交所老写法，BSE/BJS=北交所
_SYMBOL_MARKET_TAG = {
    "sh": "sh", "ss": "sh", "shse": "sh",
    "sz": "sz", "szse": "sz",
    "bj": "bj", "bse": "bj", "bjs": "bj",
}
# 600519 / sh600519 / SH600519 / 600519.SH / 600519.SS / sz000001 / 430047.BJ
_SYMBOL_RE = re.compile(
    r"^(?:(sh|sz|bj)[.\-_]?)?(\d{6})(?:[.\-_]?(sh|ss|shse|sz|szse|bj|bse|bjs))?$",
    re.IGNORECASE)

SYMBOL_HINT = ("支持 600519 / sh600519 / SH600519 / 600519.SH 等写法；"
                "沪 60/68/5xxxxx/90x、深 00/30/12x/15x/16x/18x/20x、北 43/83/87/92")


def normalize_cn_symbol(s: Any) -> Optional[tuple]:
    """任意常见写法的 A 股代码 → 规范 ``(market, code)``，无法识别返回 ``None``。

    显式市场标记（``sh600519`` / ``600519.SH``）**优先于**按号段推断，
    这样 ``sh000001``（上证指数）与 ``sz000001``（平安银行）都能正确区分。
    """
    if s is None:
        return None
    raw = str(s).strip()
    if not raw:
        return None
    m = _SYMBOL_RE.match(raw)
    if not m:
        return None
    pre_tag, code, suf_tag = m.group(1), m.group(2), m.group(3)
    tag = (pre_tag or suf_tag or "").lower()
    if tag:
        market = _SYMBOL_MARKET_TAG.get(tag)
        if market is None:
            return None
        return (market, code)
    for prefix, market in _SYMBOL_PREFIX_MARKET:
        if code.startswith(prefix):
            return (market, code)
    return None


def symbol_fail(s: Any) -> str:
    """无法识别代码时的可操作错误（**禁止静默返回 []**）。"""
    return fail("无法识别股票代码 '%s'" % (s,), code="invalid_symbol", hint=SYMBOL_HINT)


def bare_code(s: Any) -> Optional[str]:
    """→ 6 位裸代码（东财 datacenter / 巨潮 / 同花顺等只要裸代码的接口用）。"""
    norm = normalize_cn_symbol(s)
    return norm[1] if norm else None


def tencent_symbol(s: Any) -> Optional[str]:
    """→ 腾讯行情写法 ``sh600519`` / ``sz000001`` / ``bj430047``。"""
    norm = normalize_cn_symbol(s)
    return (norm[0] + norm[1]) if norm else None


def eastmoney_secid(s: Any) -> Optional[str]:
    """→ 东财 secid（``1.`` = 沪，``0.`` = 深/北）。"""
    norm = normalize_cn_symbol(s)
    if not norm:
        return None
    return ("1." if norm[0] == "sh" else "0.") + norm[1]


# ── 日期：YYYYMMDD / YYYY-MM-DD / YYYY/MM/DD / YYYY.MM.DD 全兼容 ──
_DATE_FORMATS = ("%Y-%m-%d", "%Y%m%d", "%Y/%m/%d", "%Y.%m.%d")
DATE_HINT = "支持 20260626 / 2026-06-26 / 2026/06/26 三种写法"


def normalize_date(value: Any, fmt: str = "%Y-%m-%d") -> Optional[str]:
    """归一化日期字符串；``None``/空 → ``None``；无法识别 → ``ValueError``。"""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for f in _DATE_FORMATS:
        try:
            return datetime.strptime(s, f).strftime(fmt)
        except ValueError:
            continue
    raise ValueError("无法识别日期 '%s'" % (value,))


def date_fail(value: Any) -> str:
    """日期非法的可操作错误。"""
    return fail("无法识别日期 '%s'" % (value,), code="invalid_date", hint=DATE_HINT)


def date_arg(value: Any, fmt: str = "%Y-%m-%d", default_today: bool = False):
    """→ ``(归一化日期, None)`` 或 ``(None, fail_json)``。

    ``default_today=True`` 时 ``None``/空 取今天（与"涨停揭秘""龙虎榜"等
    只接受当天日期的接口历史行为一致）。
    """
    try:
        out = normalize_date(value, fmt)
    except ValueError:
        return None, date_fail(value)
    if out is None and default_today:
        out = datetime.now().strftime(fmt)
    return out, None


# ═══════════════════════════════════════════════════════════════
# 6. 日志
# ═══════════════════════════════════════════════════════════════

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def setup_logging(name: str, level: Optional[int] = None) -> logging.Logger:
    """统一日志初始化。

    两个服务曾在 import 阶段就 ``logging.basicConfig``，早于 server 初始化，
    导致 uvicorn/gateway 后续无法再配置 handler。这里只在**尚未配置**时设置，
    并把级别交给调用方（默认取 ``LOG_LEVEL`` 环境变量，回退 INFO）。
    """
    import os
    if level is None:
        level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level, format=_LOG_FORMAT)
    else:
        root.setLevel(level)
    return logging.getLogger(name)
