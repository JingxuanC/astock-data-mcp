#!/usr/bin/env python3
"""mcp_gateway（license 鉴权 / 额度 / 异步队列）的单元测试。"""

import json
import threading
import time

import pytest

from mcp_gateway import JobQueue, LicenseStore, QueueFull, QuotaExceeded


@pytest.fixture()
def license_file(tmp_path):
    f = tmp_path / "licenses.json"
    f.write_text(json.dumps({"keys": {
        "ak_alice": {"name": "alice", "daily_quota": 3, "heavy_quota": 1},
        "ak_bob": {"name": "bob"},  # 无额度限制
    }}))
    return str(f)


# ── LicenseStore ──

def test_open_mode_without_file():
    st = LicenseStore("", domain="t")
    assert not st.enabled
    ok, _ = st.check("whatever")
    assert ok
    assert st.consume("whatever", heavy=True) == {"mode": "open"}


def test_invalid_key_rejected(license_file):
    st = LicenseStore(license_file, domain="t")
    assert st.enabled
    ok, msg = st.check("ak_nobody")
    assert not ok and "license" in msg
    ok, name = st.check("ak_alice")
    assert ok and name == "alice"


def test_daily_quota_enforced(license_file):
    st = LicenseStore(license_file, domain="t")
    for _ in range(3):
        st.consume("ak_alice", heavy=False)
    with pytest.raises(QuotaExceeded):
        st.consume("ak_alice", heavy=False)
    # bob 无限制
    for _ in range(10):
        st.consume("ak_bob", heavy=True)


def test_heavy_quota_separate(license_file):
    st = LicenseStore(license_file, domain="t")
    st.consume("ak_alice", heavy=True)  # heavy_quota=1
    with pytest.raises(QuotaExceeded, match="重负载"):
        st.consume("ak_alice", heavy=True)
    # 轻调用仍可用（daily_quota=3 还剩）
    st.consume("ak_alice", heavy=False)


def test_usage_persisted_and_reload(license_file):
    st = LicenseStore(license_file, domain="t")
    st.consume("ak_alice", heavy=True)
    # 新实例从 usage 文件恢复计数
    st2 = LicenseStore(license_file, domain="t")
    q = st2.quota_of("ak_alice")
    assert q["calls_today"] == 1 and q["heavy_today"] == 1
    assert q["calls_left"] == 2 and q["heavy_left"] == 0


def test_license_hot_reload(license_file):
    st = LicenseStore(license_file, domain="t")
    ok, _ = st.check("ak_carol")
    assert not ok
    # 账号服务签发新 key（改文件）→ 免重启生效
    import os
    data = json.loads(open(license_file).read())
    data["keys"]["ak_carol"] = {"name": "carol", "daily_quota": 5}
    with open(license_file, "w") as f:
        json.dump(data, f)
    os.utime(license_file, (time.time() + 2, time.time() + 2))  # 确保 mtime 变化
    st.reload()
    ok, name = st.check("ak_carol")
    assert ok and name == "carol"


# ── JobQueue ──

def _double(x=0):
    return str(x * 2)


def _boom(**_):
    raise ValueError("炸了")


def test_job_done():
    q = JobQueue({"double": _double}, workers=1)
    jid = q.submit("double", {"x": 21}, key="k1")
    deadline = time.time() + 5
    while time.time() < deadline:
        job = q.get(jid)
        if job["status"] == "done":
            break
        time.sleep(0.05)
    job = q.get(jid)
    assert job["status"] == "done" and job["result"] == "42"
    assert job["elapsed_sec"] >= 0
    q.shutdown()


def test_job_error():
    q = JobQueue({"boom": _boom}, workers=1)
    jid = q.submit("boom", {}, key="k1")
    deadline = time.time() + 5
    while time.time() < deadline:
        if q.get(jid)["status"] == "error":
            break
        time.sleep(0.05)
    job = q.get(jid)
    assert job["status"] == "error" and "炸了" in job["error"]
    q.shutdown()


def test_per_key_concurrency_gate():
    blocker = threading.Event()

    def slow(**_):
        blocker.wait(2)
        return "ok"

    q = JobQueue({"slow": slow}, workers=2)
    q.submit("slow", {}, key="k1")
    with pytest.raises(QueueFull, match="已有一个"):
        q.submit("slow", {}, key="k1")
    # 别的 key 不受影响
    q.submit("slow", {}, key="k2")
    blocker.set()
    q.shutdown()


def test_queue_full_rejected():
    blocker = threading.Event()

    def slow(**_):
        blocker.wait(2)
        return "ok"

    q = JobQueue({"slow": slow}, workers=1, maxsize=1)
    # worker 是否已取走第一个任务存在竞争 → 持续提交直到队列拒收，
    # 断言 3 次内必满（worker 1 + 队列 1 = 容量 2）
    raised = False
    for _ in range(3):
        try:
            q.submit("slow", {}, key="")
        except QueueFull as e:
            assert "已满" in str(e)
            raised = True
            break
    assert raised, "队列满时应拒绝提交"
    blocker.set()
    q.shutdown()


def test_job_ownership_isolation():
    q = JobQueue({"double": _double}, workers=1)
    jid = q.submit("double", {"x": 1}, key="alice")
    assert q.get(jid, key="alice") is not None
    assert q.get(jid, key="bob") is None  # bob 看不到 alice 的任务
    q.shutdown()
