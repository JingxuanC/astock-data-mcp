#!/usr/bin/env python3
"""全市场截面工具单测：clist 分页 / 退化拒绝 / 可交易域过滤。

不联网：em_get 用 mock 顶掉。重点锁住 2026-09 那次事故的教训——
东财 clist 会静默截断分页（pz 被钳到 100，异常时只回 2 行），
`if len(diff) < pz: break` 会把短页当"列表到底了"，让"全市场"退化成 2 只。
"""

import json

import pytest

import server as S


class _Resp:
    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


def _page(codes, total, extra=None):
    """构造一页 clist 响应。codes 为 f12 列表。"""
    diff = []
    for c in codes:
        row = {"f12": c}
        if extra:
            row.update(extra(c))
        diff.append(row)
    return _Resp({"data": {"total": total, "diff": diff}})


def _patch_pages(monkeypatch, pages, total):
    """pages: {pn: [codes...]}；按 pn 返回对应页，越界返回空页。"""
    calls = []

    def fake_em_get(url, params=None, headers=None, timeout=None, **kw):
        pn = int(params["pn"])
        calls.append(pn)
        codes = pages.get(pn, [])
        return _page(codes, total)

    monkeypatch.setattr(S, "em_get", fake_em_get)
    return calls


# ── em_clist_all：按 total 翻页 ──

def test_clist_pages_until_total(monkeypatch):
    pages = {1: [f"60{i:04d}" for i in range(100)],
             2: [f"60{i:04d}" for i in range(100, 200)],
             3: [f"60{i:04d}" for i in range(200, 260)]}
    calls = _patch_pages(monkeypatch, pages, total=260)
    rows = S.em_clist_all("m:1+t:2", "f12", min_expected=1)
    assert len(rows) == 260
    assert calls == [1, 2, 3], "必须翻到 total 为止"


def test_clist_full_page_then_empty_page_ends(monkeypatch):
    """最后一页刚好满页 → 再取一页拿到空页结束（不能靠 len< pz 判断）。"""
    pages = {1: [f"60{i:04d}" for i in range(100)], 2: []}
    calls = _patch_pages(monkeypatch, pages, total=100)
    rows = S.em_clist_all("m:1+t:2", "f12", min_expected=1)
    assert len(rows) == 100
    assert calls == [1], "第 1 页已满足 total，不该再请求"


def test_clist_truncated_page_raises(monkeypatch):
    """接口只回 2 行却声称 total=5500（事故原形）→ 必须报错，绝不返回 2 只。"""
    _patch_pages(monkeypatch, {1: ["000001", "000002"]}, total=5500)
    with pytest.raises(S.UniverseDegraded, match="只取到 2/5500"):
        S.em_clist_all("m:0+t:6", "f12", min_expected=100)


def test_clist_below_floor_raises(monkeypatch):
    """接口正常但总量远低于全市场量级（被限流成小列表）→ 报错。"""
    _patch_pages(monkeypatch, {1: [f"60{i:04d}" for i in range(100)]}, total=100)
    with pytest.raises(S.UniverseDegraded, match="仅返回 100 条"):
        S.em_clist_all("m:1+t:2", "f12", min_expected=1000)


def test_clist_max_pages_guard(monkeypatch):
    """total 异常巨大时分页有硬上限，不会死循环。"""
    pages = {pn: [f"60{i:04d}" for i in range(100)] for pn in range(1, 6)}
    calls = _patch_pages(monkeypatch, pages, total=10**9)
    with pytest.raises(S.UniverseDegraded):
        S.em_clist_all("m:1+t:2", "f12", max_pages=4, min_expected=1)
    assert calls == [1, 2, 3, 4]


# ── get_a_market_snapshot ──

def _snapshot_rows(changes):
    def extra(c):
        return {"f3": changes[c], "f6": 10000000.0}  # 每只 1000 万元
    return {1: list(changes.keys())}, extra


def test_market_snapshot_breadth(monkeypatch, tmp_path):
    S.CACHE_DIR = tmp_path
    changes = {"600000": 10.0, "600001": -10.0, "600002": 0.0,
               "600003": 3.0, "600004": -4.0, "600005": 6.0, "600006": 1.0}
    pages, extra = _snapshot_rows(changes)
    monkeypatch.setattr(S, "em_get", lambda url, params=None, **kw: _page(
        pages[int(params["pn"])], total=len(changes), extra=extra))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    out = json.loads(S.get_a_market_snapshot())
    assert out["scanned"] == 7 and out["total"] == 7
    assert out["up"] == 4 and out["down"] == 2 and out["flat"] == 1
    assert out["limit_up_like"] == 1 and out["limit_down_like"] == 1
    assert out["buckets"]["涨停附近(≥9.5%)"] == 1
    assert out["buckets"]["跌停附近"] == 1
    assert out["buckets"]["涨2-5%"] == 1 and out["buckets"]["涨5-9.5%"] == 1
    assert out["amount_yi"] == pytest.approx(0.7, rel=1e-3)


def test_market_snapshot_degrades_loudly(monkeypatch, tmp_path):
    """退化时必须回错 JSON（handler 会据此置 isError），而不是短市场。"""
    S.CACHE_DIR = tmp_path
    monkeypatch.setattr(S, "em_get", lambda url, params=None, **kw: _page(
        ["000001", "000002"], total=5500))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    out = json.loads(S.get_a_market_snapshot())
    assert "error" in out and out.get("code") == "degraded_source"


# ── get_a_trade_universe ──

def _universe_rows():
    """f2 价, f3 涨跌, f6 成交额, f8 换手, f20 总市值, f21 流通, f26 上市日, f100 行业"""
    return {
        "600519": {"f13": "1", "f14": "贵州茅台", "f2": 1700.0, "f3": 1.0, "f6": 5e9,
                   "f8": 0.5, "f20": 2.1e12, "f21": 2.1e12, "f26": "20010827", "f100": "白酒"},
        "600001": {"f13": "1", "f14": "ST小明", "f2": 3.0, "f3": -5.0, "f6": 1e7,
                   "f26": "20000101", "f100": "钢铁"},
        "600002": {"f13": "1", "f14": "退市王", "f2": 0.5, "f3": 0.0, "f6": 1e6,
                   "f26": "20000101", "f100": "综合"},
        "600003": {"f13": "1", "f14": "停牌股", "f2": "-", "f3": "-", "f6": "-",
                   "f26": "20000101", "f100": "综合"},
        "600004": {"f13": "1", "f14": "次新股", "f2": 20.0, "f3": 2.0, "f6": 8e8,
                   "f26": "20990101", "f100": "电子"},
        "000001": {"f13": "0", "f14": "平安银行", "f2": 12.0, "f3": -1.0, "f6": 2e9,
                   "f26": "19910403", "f100": "银行"},
        "430047": {"f13": "0", "f14": "北交股", "f2": 8.0, "f3": 3.0, "f6": 5e6,
                   "f26": "20210101", "f100": "机械"},
        "000002": {"f13": "0", "f14": "小成交", "f2": 5.0, "f3": 0.5, "f6": 1e5,
                   "f26": "19910129", "f100": "地产"},
    }


def _patch_universe(monkeypatch, tmp_path, **kwargs):
    S.CACHE_DIR = tmp_path
    rows = _universe_rows()
    codes = list(rows.keys())

    def extra(c):
        return rows[c]

    monkeypatch.setattr(S, "em_get", lambda url, params=None, **kw: _page(
        [c for i, c in enumerate(codes) if i // 100 == int(params["pn"]) - 1],
        total=100, extra=extra))


def test_universe_filters_and_is_auditable(monkeypatch, tmp_path):
    S.CACHE_DIR = tmp_path
    rows = _universe_rows()
    codes = list(rows.keys())

    def fake_em_get(url, params=None, **kw):
        return _page(codes, total=len(codes), extra=lambda c: rows[c])

    # min_expected 走默认 1000 会拒绝小样本，这里把下限调低以测过滤逻辑
    monkeypatch.setattr(S, "em_get", fake_em_get)
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    out = json.loads(S.get_a_trade_universe(exclude_new_days=60))
    assert out["scanned"] == 8
    assert out["excluded"] == {"st": 2, "suspended": 1, "new_listing": 1,
                               "bj": 1, "low_amount": 0}
    syms = [u["symbol"] for u in out["universe"]]
    assert syms == ["sh600519", "sz000001", "sz000002"], "只留可交易的三只"
    top = out["universe"][0]
    assert top["name"] == "贵州茅台" and top["industry"] == "白酒"
    assert top["mktcap_yi"] == pytest.approx(21000.0)
    assert top["list_date"] == "20010827"
    assert out["universe"][-1]["symbol"] == "sz000002", "按成交额降序"


def test_universe_min_amount_and_bj_switch(monkeypatch, tmp_path):
    S.CACHE_DIR = tmp_path
    rows = _universe_rows()
    codes = list(rows.keys())
    monkeypatch.setattr(S, "em_get", lambda url, params=None, **kw: _page(
        codes, total=len(codes), extra=lambda c: rows[c]))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    out = json.loads(S.get_a_trade_universe(exclude_new_days=0, include_bj=True,
                                            min_amount_wan=100.0))
    syms = {u["symbol"] for u in out["universe"]}
    assert "bj430047" in syms, "显式开启北交所后应包含 bj 前缀"
    assert "sz000002" not in syms, "成交额 1e5 元=10 万 < 100 万被剔除"
    assert out["excluded"]["low_amount"] == 1


def test_universe_degrades_loudly(monkeypatch, tmp_path):
    S.CACHE_DIR = tmp_path
    monkeypatch.setattr(S, "em_get", lambda url, params=None, **kw: _page(
        ["000001", "000002"], total=5500))
    out = json.loads(S.get_a_trade_universe())
    assert "error" in out and out.get("code") == "degraded_source"
    assert "universe" not in out, "退化时绝不能给出一个短 universe"


def test_symbol_prefixes():
    assert S._symbol_of("600519", "1") == "sh600519"
    assert S._symbol_of("000001", "0") == "sz000001"
    assert S._symbol_of("300750", "0") == "sz300750"
    assert S._symbol_of("430047", "0") == "bj430047"
    assert S._symbol_of("920002", "0") == "bj920002"


# ── 腾讯兜底：东财 clist 502/断连时仍要给出全市场 ──

def _write_universe_file(tmp_path, lines):
    f = tmp_path / "all.txt"
    f.write_text("".join(lines))
    return f


def test_local_universe_codes_parses_and_filters(tmp_path, monkeypatch):
    f = _write_universe_file(tmp_path, [
        "SH600519\t2001-08-27\t2026-09-11\n",
        "SZ000001\t1991-04-03\t2026-09-11\n",
        "bj430047\t2021-01-01\t2026-09-11\n",
        "SH000300\t2005-01-04\t2026-09-11\n",   # 指数，非个股
        "SZ399300\t2005-01-04\t2026-09-11\n",   # 指数
    ])
    monkeypatch.setenv("A_UNIVERSE_FILE", str(f))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    assert S._local_universe_codes(False) == ["sh600519", "sz000001"]
    assert S._local_universe_codes(True) == ["sh600519", "sz000001", "bj430047"]


def test_local_universe_file_short_raises(tmp_path, monkeypatch):
    f = _write_universe_file(tmp_path, ["SH600519\t2001-08-27\t2026-09-11\n"])
    monkeypatch.setenv("A_UNIVERSE_FILE", str(f))
    with pytest.raises(S.UniverseDegraded, match="本地代码表仅 1 只"):
        S._local_universe_codes(False)


def test_local_universe_file_missing_raises(monkeypatch):
    monkeypatch.setenv("A_UNIVERSE_FILE", "/nonexistent/all.txt")
    with pytest.raises(S.UniverseDegraded, match="不可读"):
        S._local_universe_codes(False)


def test_tencent_universe_rows_maps_fields(tmp_path, monkeypatch):
    f = _write_universe_file(tmp_path, [
        "SH600519\t2001-08-27\t2026-09-11\n",
        "SZ000001\t1991-04-03\t2026-09-11\n",
    ])
    monkeypatch.setenv("A_UNIVERSE_FILE", str(f))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    monkeypatch.setattr(S, "_tencent_fetch", lambda syms: {
        "600519": {"symbol": "600519", "name": "贵州茅台", "price": 1700.0,
                   "pct_change": 1.2, "amount": 5e9, "turnover": 0.5, "market_cap": 2.1e12},
        "000001": {"symbol": "000001", "name": "平安银行", "price": 12.0,
                   "pct_change": -0.8, "amount": 2e9, "turnover": 0.9, "market_cap": 2.3e11},
    })
    rows = S._tencent_universe_rows(False)
    assert len(rows) == 2
    r = {x["f12"]: x for x in rows}
    assert r["600519"]["f13"] == "1" and r["600519"]["f14"] == "贵州茅台"
    assert r["000001"]["f13"] == "0" and r["000001"]["f20"] == 2.3e11
    assert r["600519"]["f100"] == "" and r["600519"]["f26"] is None


def test_tencent_universe_coverage_guard(tmp_path, monkeypatch):
    """腾讯只回一半 → 拒绝当全市场（跨来源同样不许静默变短）。"""
    f = _write_universe_file(tmp_path, [
        f"SH6005{i:02d}\t2001-08-27\t2026-09-11\n" for i in range(10)
    ])
    monkeypatch.setenv("A_UNIVERSE_FILE", str(f))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    monkeypatch.setattr(S, "_tencent_fetch", lambda syms: {
        "600500": {"symbol": "600500", "name": "x", "price": 1.0, "pct_change": 0.0,
                   "amount": 1e6, "turnover": 0.1, "market_cap": 1e9},
    })
    with pytest.raises(S.UniverseDegraded, match="只覆盖 1/10"):
        S._tencent_universe_rows(False)


def test_snapshot_falls_back_to_tencent(tmp_path, monkeypatch):
    """东财 clist 抛 502 → 快照仍给出结果，并标注 source=tencent_qt。"""
    S.CACHE_DIR = tmp_path
    f = _write_universe_file(tmp_path, [
        "SH600519\t2001-08-27\t2026-09-11\n",
        "SZ000001\t1991-04-03\t2026-09-11\n",
    ])
    monkeypatch.setenv("A_UNIVERSE_FILE", str(f))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    monkeypatch.setattr(S, "em_clist_all",
                        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("502 Bad Gateway")))
    monkeypatch.setattr(S, "_tencent_fetch", lambda syms: {
        "600519": {"symbol": "600519", "name": "贵州茅台", "price": 1700.0,
                   "pct_change": 3.0, "amount": 5e9, "turnover": 0.5, "market_cap": 2.1e12},
        "000001": {"symbol": "000001", "name": "平安银行", "price": 12.0,
                   "pct_change": -1.0, "amount": 2e9, "turnover": 0.9, "market_cap": 2.3e11},
    })
    out = json.loads(S.get_a_market_snapshot())
    assert out["source"] == "tencent_qt"
    assert out["total"] == 2 and out["up"] == 1 and out["down"] == 1


def test_universe_falls_back_and_reports_notes(tmp_path, monkeypatch):
    S.CACHE_DIR = tmp_path
    f = _write_universe_file(tmp_path, [
        "SH600519\t2001-08-27\t2026-09-11\n",
        "SZ000001\t1991-04-03\t2026-09-11\n",
    ])
    monkeypatch.setenv("A_UNIVERSE_FILE", str(f))
    monkeypatch.setattr(S, "_UNIVERSE_FLOOR", 1)
    monkeypatch.setattr(S, "em_clist_all",
                        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("502")))
    monkeypatch.setattr(S, "_tencent_fetch", lambda syms: {
        "600519": {"symbol": "600519", "name": "贵州茅台", "price": 1700.0,
                   "pct_change": 1.0, "amount": 5e9, "turnover": 0.5, "market_cap": 2.1e12},
        "000001": {"symbol": "000001", "name": "平安银行", "price": 12.0,
                   "pct_change": -1.0, "amount": 2e9, "turnover": 0.9, "market_cap": 2.3e11},
    })
    out = json.loads(S.get_a_trade_universe(exclude_new_days=0))
    assert out["source"] == "tencent_qt" and out["passed"] == 2
    assert out["notes"], "兜底来源必须显式说明 industry/list_date 不可用"
    assert {u["symbol"] for u in out["universe"]} == {"sh600519", "sz000001"}
