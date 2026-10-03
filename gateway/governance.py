"""重放与结果复用治理：按调用方隔离的两类有界记录。

- ReplayStore：调用标识（caller_id + request_id）结论重放，保留期内承诺一致性。
- ReuseStore：成功结论按 (caller, model, version, prompt, timeout_ms) 复用。

容量与保留策略按调用方隔离、有界、可配置；非法配置在构造阶段直接拒绝。
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import time
from dataclasses import dataclass
from typing import Callable

from .errors import (
    IdempotencyConflictError,
    InvalidGovernanceConfigError,
    RecordCapacityError,
)


@dataclass(frozen=True)
class GovernanceConfig:
    """重放/复用治理配置（全部有界；非法取值在加载阶段拒绝）。"""

    replay_ttl_seconds: float = 300.0
    replay_max_per_caller: int = 128
    reuse_ttl_seconds: float = 300.0
    reuse_max_per_caller: int = 256

    def __post_init__(self) -> None:
        def _ttl(name: str, value: float) -> None:
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise InvalidGovernanceConfigError(
                    f"{name} must be a positive finite number of seconds, got {value!r}",
                    detail={"field": name, "value": value},
                )

        def _cap(name: str, value: int) -> None:
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise InvalidGovernanceConfigError(
                    f"{name} must be a positive integer, got {value!r}",
                    detail={"field": name, "value": value},
                )

        _ttl("replay_ttl_seconds", self.replay_ttl_seconds)
        _cap("replay_max_per_caller", self.replay_max_per_caller)
        _ttl("reuse_ttl_seconds", self.reuse_ttl_seconds)
        _cap("reuse_max_per_caller", self.reuse_max_per_caller)


@dataclass
class StoredConclusion:
    """一次成功执行的完整结论快照（重放承诺与复用共用）。"""

    request_id: str
    model: str
    version: str
    backend: str
    output: str
    attempts: int
    fallback_from: list[str]
    created_at: float
    expires_at: float
    fingerprint: str = ""


@dataclass
class GovernanceCounters:
    """治理层计数：真实执行之外的各类命中、拒绝、过期与淘汰。"""

    replay_hits: int = 0
    replay_conflicts: int = 0
    replay_capacity_rejects: int = 0
    replay_expired: int = 0
    replay_evicted: int = 0
    reuse_hits: int = 0
    reuse_capacity_rejects: int = 0
    reuse_expired: int = 0
    reuse_evicted: int = 0

    def as_dict(self) -> dict:
        return {
            "replay_hits": self.replay_hits,
            "replay_conflicts": self.replay_conflicts,
            "replay_capacity_rejects": self.replay_capacity_rejects,
            "replay_expired": self.replay_expired,
            "replay_evicted": self.replay_evicted,
            "reuse_hits": self.reuse_hits,
            "reuse_capacity_rejects": self.reuse_capacity_rejects,
            "reuse_expired": self.reuse_expired,
            "reuse_evicted": self.reuse_evicted,
        }


class ReplayStore:
    """调用标识结论存储 + 同标识并发合并（同一标识只真正执行一次）。

    状态机（每个 (caller_id, request_id)）：
    - 不存在 -> leader：登记 in-flight，执行一次；follower：等待同一 future。
    - in-flight 且内容一致 -> follower 等待；内容不一致 -> conflict 拒绝。
    - 已提交且未过期 -> 一律返回历史结论（replay hit），不检查指纹一致性冲突：
      重试必须返回首次承诺；首次内容即权威。
    - 已提交但过期 -> 按未命中处理（惰性清除），键可被重新占用。
    容量：先清过期；仍满时**不淘汰**在保留期内的承诺，快速拒绝新请求。
    """

    def __init__(
        self,
        config: GovernanceConfig,
        *,
        clock: Callable[[], float] | None = None,
        counters: GovernanceCounters | None = None,
    ) -> None:
        self._cfg = config
        self._clock = clock or time.monotonic
        self.counters = counters or GovernanceCounters()
        self._ttl = config.replay_ttl_seconds
        # caller -> request_id -> StoredConclusion（dict 保持插入顺序，便于确定淘汰扫描）
        self._records: dict[str, dict[str, StoredConclusion]] = {}
        # caller -> request_id -> (model, fingerprint, asyncio.Future[StoredConclusion])
        self._inflight: dict[str, dict[str, tuple[str, str, asyncio.Future]]] = {}
        self._lock = asyncio.Lock()

    def _purge_expired_locked(self, caller_id: str) -> int:
        """删除该调用方所有已过期的已提交结论；返回删除条数。"""
        now = self._clock()
        bucket = self._records.get(caller_id)
        if not bucket:
            return 0
        dead = [rid for rid, c in bucket.items() if c.expires_at <= now]
        for rid in dead:
            del bucket[rid]
        return len(dead)

    async def lookup_committed(
        self,
        caller_id: str,
        request_id: str,
        model: str | None = None,
        fingerprint: str | None = None,
    ) -> StoredConclusion | None:
        async with self._lock:
            bucket = self._records.get(caller_id)
            if not bucket:
                return None
            c = bucket.get(request_id)
            if c is None:
                return None
            if c.expires_at <= self._clock():
                del bucket[request_id]
                self.counters.replay_expired += 1
                return None
            if model is not None and (c.model != model or c.fingerprint != fingerprint):
                self.counters.replay_conflicts += 1
                raise IdempotencyConflictError(
                    f"request_id {request_id!r} is already bound to a different model "
                    f"or request content for caller {caller_id!r}",
                    detail={
                        "caller_id": caller_id,
                        "request_id": request_id,
                        "stored_model": c.model,
                        "incoming_model": model,
                        "phase": "committed",
                    },
                )
            self.counters.replay_hits += 1
            return c

    async def check_or_join(
        self,
        caller_id: str,
        request_id: str,
        model: str,
        fingerprint: str,
    ) -> tuple[str, asyncio.Future[StoredConclusion] | None]:
        """登记或加入一次执行。

        返回 ("leader", None) 表示调用方应真正执行；
        返回 ("follower", future) 表示等待 leader 的同一份结论；
        冲突抛 IdempotencyConflictError；容量不足抛 RecordCapacityError。
        """
        async with self._lock:
            inflight = self._inflight.setdefault(caller_id, {})
            existing = inflight.get(request_id)
            if existing is not None:
                ex_model, ex_fp, fut = existing
                if ex_model != model or ex_fp != fingerprint:
                    self.counters.replay_conflicts += 1
                    raise IdempotencyConflictError(
                        f"request_id {request_id!r} already in flight with a different "
                        f"model or request content for caller {caller_id!r}",
                        detail={
                            "caller_id": caller_id,
                            "request_id": request_id,
                            "stored_model": ex_model,
                            "incoming_model": model,
                            "phase": "inflight",
                        },
                    )
                self.counters.replay_hits += 1
                return "follower", fut

            bucket = self._records.setdefault(caller_id, {})
            prior = bucket.get(request_id)
            live_future: asyncio.Future[StoredConclusion] | None = None
            if prior is not None:
                if prior.expires_at <= self._clock():
                    del bucket[request_id]
                    self.counters.replay_expired += 1
                    prior = None
                else:
                    # 已提交承诺与新内容不一致：冲突拒绝，绝不覆盖旧结论。
                    if prior.model != model or prior.fingerprint != fingerprint:
                        self.counters.replay_conflicts += 1
                        raise IdempotencyConflictError(
                            f"request_id {request_id!r} is already bound to a different "
                            f"model or request content for caller {caller_id!r}",
                            detail={
                                "caller_id": caller_id,
                                "request_id": request_id,
                                "stored_model": prior.model,
                                "incoming_model": model,
                                "phase": "committed",
                            },
                        )
                    # 存活且内容一致：直接作为 follower 取回历史结论（存储层自洽保证，
                    # 网关快路径通常已先行返回）。
                    loop = asyncio.get_running_loop()
                    live_future = loop.create_future()
                    live_future.set_result(prior)
                    self.counters.replay_hits += 1
                    return "follower", live_future

            if prior is None:
                # 容量同时计入已提交结论与在途预留，保证提交后不会越界；
                # 已过期的提交先回收，仍满则不淘汰在保留期内的承诺，快速拒绝。
                used = len(bucket) + len(inflight)
                if used >= self._cfg.replay_max_per_caller:
                    purged = self._purge_expired_locked(caller_id)
                    self.counters.replay_expired += purged
                    if len(bucket) + len(inflight) >= self._cfg.replay_max_per_caller:
                        self.counters.replay_capacity_rejects += 1
                        raise RecordCapacityError(
                            f"replay record capacity {self._cfg.replay_max_per_caller} reached "
                            f"for caller {caller_id!r}",
                            detail={
                                "caller_id": caller_id,
                                "scope": "replay",
                                "capacity": self._cfg.replay_max_per_caller,
                                "committed": len(bucket),
                                "inflight": len(inflight),
                            },
                        )

            loop = asyncio.get_running_loop()
            fut: asyncio.Future[StoredConclusion] = loop.create_future()
            inflight[request_id] = (model, fingerprint, fut)
            return "leader", None

    async def commit(self, caller_id: str, request_id: str, conclusion: StoredConclusion) -> None:
        now = self._clock()
        conclusion.created_at = now
        conclusion.expires_at = now + self._ttl
        async with self._lock:
            bucket = self._records.setdefault(caller_id, {})
            if request_id in bucket:
                del bucket[request_id]
            bucket[request_id] = conclusion
            entry = self._inflight.get(caller_id, {}).pop(request_id, None)
        if entry is not None and not entry[2].done():
            entry[2].set_result(conclusion)

    async def fail_or_cancel(
        self, caller_id: str, request_id: str, exc: BaseException | None
    ) -> None:
        """leader 失败/取消：释放标识，不占结论；唤醒 follower 重放同一异常。"""
        async with self._lock:
            entry = self._inflight.get(caller_id, {}).pop(request_id, None)
        if entry is not None and not entry[2].done():
            if exc is None:
                exc = asyncio.CancelledError()
            entry[2].set_exception(exc)
            try:
                entry[2].exception()  # 标记已取回，避免“未检索异常”告警
            except (asyncio.CancelledError, Exception):
                pass

    def record_count(self, caller_id: str | None = None) -> int:
        if caller_id is not None:
            return len(self._records.get(caller_id, {}))
        return sum(len(b) for b in self._records.values())

    def inflight_count(self, caller_id: str | None = None) -> int:
        if caller_id is not None:
            return len(self._inflight.get(caller_id, {}))
        return sum(len(b) for b in self._inflight.values())

    def set_retention(self, ttl_seconds: float | None) -> None:
        """缩短保留期只影响此后新提交的结论；既有承诺不变。None 恢复配置值。"""
        if ttl_seconds is not None:
            if not isinstance(ttl_seconds, (int, float)) or not math.isfinite(ttl_seconds) \
                    or ttl_seconds <= 0:
                raise InvalidGovernanceConfigError(
                    f"retention must be a positive finite number of seconds, got {ttl_seconds!r}",
                    detail={"field": "replay_ttl_seconds", "value": ttl_seconds},
                )
            self._ttl = float(ttl_seconds)
        else:
            self._ttl = self._cfg.replay_ttl_seconds

    @property
    def retention_seconds(self) -> float:
        return self._ttl

    async def purge(self, *, model: str | None = None, version: str | None = None) -> int:
        """重放承诺不可被定向清理改写：始终返回 0（保留期内结论必须稳定）。"""
        return 0


class ReuseStore:
    """成功结论的跨请求复用缓存。

    键作用域为调用方：(model, version, prompt, timeout_ms)；不同调用方、
    不同模型别名、不同实际版本之间天然隔离。
    - 过期项惰性删除，按未命中处理，并计入 reuse_expired。
    - 容量满时允许淘汰：先清过期，仍满则按插入顺序（FIFO，最旧优先）淘汰，
      计入 reuse_evicted；复用记录不做承诺，因此不会产生容量快速拒绝。
    """

    def __init__(
        self,
        config: GovernanceConfig,
        *,
        clock: Callable[[], float] | None = None,
        counters: GovernanceCounters | None = None,
    ) -> None:
        self._cfg = config
        self._clock = clock or time.monotonic
        self.counters = counters or GovernanceCounters()
        self._ttl = config.reuse_ttl_seconds
        # caller -> reuse_key -> StoredConclusion（dict 插入顺序 = 最旧 -> 最新）
        self._records: dict[str, dict[str, StoredConclusion]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def make_key(model: str, version: str, prompt: str, timeout_ms: int | None) -> str:
        raw = "\x1f".join([model, version, prompt, str(timeout_ms)])
        return hashlib.sha256(raw.encode()).hexdigest()

    async def get(self, caller_id: str, key: str) -> StoredConclusion | None:
        async with self._lock:
            bucket = self._records.get(caller_id)
            if not bucket:
                return None
            c = bucket.get(key)
            if c is None:
                return None
            if c.expires_at <= self._clock():
                del bucket[key]
                self.counters.reuse_expired += 1
                return None
            self.counters.reuse_hits += 1
            return c

    async def put(self, caller_id: str, key: str, conclusion: StoredConclusion) -> None:
        async with self._lock:
            bucket = self._records.setdefault(caller_id, {})
            now = self._clock()
            conclusion.created_at = now
            conclusion.expires_at = now + self._ttl
            if key in bucket:
                del bucket[key]
                bucket[key] = conclusion
                return
            # 先清过期项
            dead = [k for k, v in bucket.items() if v.expires_at <= now]
            for k in dead:
                del bucket[k]
            self.counters.reuse_expired += len(dead)
            # 仍满则按 FIFO（dict 首项最旧）确定淘汰
            while len(bucket) >= self._cfg.reuse_max_per_caller:
                oldest = next(iter(bucket))
                del bucket[oldest]
                self.counters.reuse_evicted += 1
            bucket[key] = conclusion

    def record_count(self, caller_id: str | None = None) -> int:
        if caller_id is not None:
            return len(self._records.get(caller_id, {}))
        return sum(len(b) for b in self._records.values())

    def set_retention(self, ttl_seconds: float | None) -> None:
        """缩短保留期只影响此后写入的记录；既有记录不变。None 恢复配置值。"""
        if ttl_seconds is not None:
            if not isinstance(ttl_seconds, (int, float)) or not math.isfinite(ttl_seconds) \
                    or ttl_seconds <= 0:
                raise InvalidGovernanceConfigError(
                    f"retention must be a positive finite number of seconds, got {ttl_seconds!r}",
                    detail={"field": "reuse_ttl_seconds", "value": ttl_seconds},
                )
            self._ttl = float(ttl_seconds)
        else:
            self._ttl = self._cfg.reuse_ttl_seconds

    @property
    def retention_seconds(self) -> float:
        return self._ttl

    async def purge(self, *, model: str | None = None, version: str | None = None) -> int:
        """按模型别名和/或实际版本定向清理复用记录；只影响之后的新请求。

        正在执行的请求不读取本存储，因此不受影响；重放承诺在 ReplayStore 中，
        亦不受影响。
        """
        if model is None and version is None:
            return 0
        removed = 0
        async with self._lock:
            for bucket in self._records.values():
                dead = [
                    k for k, c in bucket.items()
                    if (model is None or c.model == model)
                    and (version is None or c.version == version)
                ]
                for k in dead:
                    del bucket[k]
                    removed += 1
        return removed
