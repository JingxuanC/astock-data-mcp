#!/usr/bin/env python3
"""域 MCP 服务的鉴权 + 额度 + 异步任务队列。

设计目标（内测 50 人规模，刻意保持零外部依赖）：

- LicenseStore：license key 鉴权 + 每日额度。key 清单是挂载进来的 JSON 文件
  （账号/License 服务签发后写入即可，域服务定时 reload 或重启生效）。
  未配置文件 → 开放模式（本地开发/内网部署行为不变）。
- JobQueue：重负载工具（回测/因子执行）异步化。提交即返回 job_id，
  客户端轮询 GET /jobs/<id>。线程池 + 队列上限 + 结果 TTL，
  防止多用户并发把 CPU 打爆。

都不引入 redis/DB：usage 落 JSON 文件（原子写），队列是纯内存
（进程重启丢队列是可接受的——任务幂等，客户端重新提交即可）。
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
from pathlib import Path

logger = logging.getLogger("mcp-gateway")


# ═══════════════ License 鉴权 + 额度 ═══════════════

class QuotaExceeded(Exception):
    """当日额度用尽。message 面向用户，含剩余信息。"""


class LicenseStore:
    """license key 清单 + 每日用量计数。

    key 文件格式（MCP_LICENSE_FILE 指向）::

        {"keys": {"ak_xxx": {"name": "张三", "daily_quota": 200, "heavy_quota": 3}}}

    daily_quota：当日全部 tools/call 次数上限；heavy_quota：当日重负载
    任务（回测类）上限。0/缺省 = 不限。usage 落盘到同目录 .usage-<domain>.json。
    """

    def __init__(self, license_file: str = "", domain: str = "factor",
                 usage_dir: str = ""):
        self._file = Path(license_file).expanduser() if license_file else None
        # usage 落盘目录：MCP_USAGE_DIR > 参数 > license 文件同目录
        # （license 常以 ro 挂载进容器，不能往同目录写）
        base = (Path(usage_dir) if usage_dir else
                Path(os.environ.get("MCP_USAGE_DIR", "")) if os.environ.get("MCP_USAGE_DIR")
                else self._file.parent if self._file else None)
        self._usage_file = base / f".usage-{domain}.json" if base else None
        self._mtime = 0.0
        self._keys: dict[str, dict] = {}
        self._mu = threading.Lock()
        self._usage: dict[str, dict[str, dict[str, int]]] = {}  # date → key → counters
        if self._file:
            self.reload(force=True)
            self._load_usage()

    @property
    def enabled(self) -> bool:
        """配置了 key 文件且至少有一个 key → 强制鉴权；否则开放模式。"""
        return self._file is not None and len(self._keys) > 0

    def reload(self, force: bool = False):
        """license 文件 mtime 变了才重读（账号服务签发新 key 后免重启生效）。"""
        if not self._file or not self._file.exists():
            return
        mtime = self._file.stat().st_mtime
        if not force and mtime == self._mtime:
            return
        try:
            data = json.loads(self._file.read_text())
            keys = data.get("keys", {})
            with self._mu:
                self._keys = keys
                self._mtime = mtime
            logger.info("license keys reloaded: %d keys", len(keys))
        except Exception as e:  # noqa: BLE001
            logger.error("license file reload failed: %s", e)

    def check(self, key: str) -> tuple[bool, str]:
        """鉴权。开放模式一律放行；否则 key 必须在清单里。"""
        if not self.enabled:
            return True, ""
        self.reload()
        with self._mu:
            info = self._keys.get(key)
        if info is None:
            return False, "invalid or missing license key（请求头 X-License-Key）"
        return True, info.get("name", "")

    def consume(self, key: str, heavy: bool) -> dict:
        """扣一次额度，返回用量快照。超额抛 QuotaExceeded。"""
        if not self.enabled:
            return {"mode": "open"}
        self.reload()
        today = time.strftime("%Y-%m-%d")
        with self._mu:
            info = self._keys.get(key, {})
            bucket = self._usage.setdefault(today, {}).setdefault(
                key, {"calls": 0, "heavy": 0}
            )
            calls_left = self._left(info.get("daily_quota", 0), bucket["calls"])
            heavy_left = self._left(info.get("heavy_quota", 0), bucket["heavy"])
            if calls_left is not None and calls_left <= 0:
                raise QuotaExceeded(
                    f"当日调用额度已用完（{info.get('daily_quota')} 次/日），明天重置或联系管理员"
                )
            if heavy and heavy_left is not None and heavy_left <= 0:
                raise QuotaExceeded(
                    f"当日重负载任务（回测/挖掘）额度已用完（{info.get('heavy_quota')} 次/日）"
                )
            bucket["calls"] += 1
            if heavy:
                bucket["heavy"] += 1
            snapshot = {
                "calls_today": bucket["calls"],
                "heavy_today": bucket["heavy"],
                "calls_left": self._left(info.get("daily_quota", 0), bucket["calls"]),
                "heavy_left": self._left(info.get("heavy_quota", 0), bucket["heavy"]),
            }
        self._save_usage()
        return snapshot

    @staticmethod
    def _left(quota: int, used: int):
        return None if quota <= 0 else quota - used

    def quota_of(self, key: str) -> dict:
        """查询剩余额度（不扣减）。"""
        if not self.enabled:
            return {"mode": "open"}
        self.reload()
        today = time.strftime("%Y-%m-%d")
        with self._mu:
            info = self._keys.get(key, {})
            bucket = self._usage.get(today, {}).get(key, {"calls": 0, "heavy": 0})
            return {
                "name": info.get("name", ""),
                "calls_today": bucket["calls"],
                "heavy_today": bucket["heavy"],
                "calls_left": self._left(info.get("daily_quota", 0), bucket["calls"]),
                "heavy_left": self._left(info.get("heavy_quota", 0), bucket["heavy"]),
            }

    def _load_usage(self):
        if not self._usage_file or not self._usage_file.exists():
            return
        try:
            with self._mu:
                self._usage = json.loads(self._usage_file.read_text())
        except Exception as e:  # noqa: BLE001
            logger.warn("usage file load failed, reset: %s", e)

    def _save_usage(self):
        if not self._usage_file:
            return
        try:
            # 只保留近 3 天，防文件膨胀
            with self._mu:
                days = sorted(self._usage)[-3:]
                slim = {d: self._usage[d] for d in days}
                self._usage = slim
            tmp = self._usage_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(slim, ensure_ascii=False))
            os.replace(tmp, self._usage_file)
        except Exception as e:  # noqa: BLE001
            logger.warn("usage save failed: %s", e)


# ═══════════════ 异步任务队列 ═══════════════

class QueueFull(Exception):
    """队列已满，客户端应稍后重试。"""


class JobQueue:
    """有界队列 + 固定 worker 线程池执行重负载工具。

    - 队列满 → 提交即拒（QueueFull），不让请求堆积
    - 结果保留 ttl_sec 供轮询，过期清掉
    - 每 key 同时只允许 1 个重任务在跑/排队（防单用户霸占总线）
    """

    def __init__(self, handlers: dict, workers: int = 2, maxsize: int = 50,
                 ttl_sec: int = 3600):
        self._handlers = handlers
        self._q: queue.Queue[str] = queue.Queue(maxsize=maxsize)
        self._jobs: dict[str, dict] = {}
        self._ttl = ttl_sec
        self._mu = threading.Lock()
        self._stop = threading.Event()
        self._threads = [
            threading.Thread(target=self._worker, daemon=True, name=f"jobworker-{i}")
            for i in range(max(1, workers))
        ]
        for t in self._threads:
            t.start()
        threading.Thread(target=self._reaper, daemon=True, name="jobreaper").start()

    def submit(self, tool: str, args: dict, key: str = "") -> str:
        # 单 key 并发闸：已有 queued/running 的重任务 → 拒绝
        with self._mu:
            for j in self._jobs.values():
                if j["key"] == key and key and j["status"] in ("queued", "running"):
                    raise QueueFull("你已有一个重任务在执行/排队中，等它跑完再提交")
        job_id = uuid.uuid4().hex[:12]
        with self._mu:
            self._jobs[job_id] = {
                "id": job_id, "tool": tool, "key": key, "status": "queued",
                "created_at": time.time(), "finished_at": None,
                "result": None, "error": None,
            }
        try:
            self._q.put_nowait((job_id, tool, args))
        except queue.Full:
            with self._mu:
                del self._jobs[job_id]
            raise QueueFull("任务队列已满，请稍后重试")
        return job_id

    def get(self, job_id: str, key: str = "") -> dict | None:
        with self._mu:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            if key and job["key"] and job["key"] != key:
                return None  # 看不到别人的任务
            out = dict(job)
        if out["status"] in ("done", "error") and out["finished_at"]:
            out["elapsed_sec"] = round(out["finished_at"] - out["created_at"], 1)
        return out

    def stats(self) -> dict:
        with self._mu:
            by_status = {}
            for j in self._jobs.values():
                by_status[j["status"]] = by_status.get(j["status"], 0) + 1
        return {"workers": len(self._threads), "queue_size": self._q.qsize(),
                "jobs": by_status}

    def _worker(self):
        while not self._stop.is_set():
            try:
                job_id, tool, args = self._q.get(timeout=1)
            except queue.Empty:
                continue
            with self._mu:
                if job_id not in self._jobs:
                    continue
                self._jobs[job_id]["status"] = "running"
            try:
                result = self._handlers[tool](**args)
                with self._mu:
                    self._jobs[job_id].update(status="done", result=str(result),
                                              finished_at=time.time())
            except Exception as e:  # noqa: BLE001
                logger.error("job %s (%s) failed: %s", job_id, tool, e)
                with self._mu:
                    self._jobs[job_id].update(status="error", error=str(e),
                                              finished_at=time.time())
            finally:
                self._q.task_done()

    def _reaper(self):
        while not self._stop.is_set():
            time.sleep(300)
            cutoff = time.time() - self._ttl
            with self._mu:
                dead = [jid for jid, j in self._jobs.items()
                        if j["status"] in ("done", "error")
                        and (j["finished_at"] or 0) < cutoff]
                for jid in dead:
                    del self._jobs[jid]
            if dead:
                logger.info("reaped %d expired jobs", len(dead))

    def shutdown(self):
        self._stop.set()
