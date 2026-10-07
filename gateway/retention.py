"""重放（调用标识幂等）与结果复用的保留存储。

两类记录均按调用方隔离、有界且可配置：

- 调用标识记录（idempotency）：(caller, request_id) -> 已成功的结论。
  保留期内的承诺不可被挤掉；容量满时快速拒绝新标识；首次未成功不占标识。
- 复用记录（reuse）：(caller, model, version, prompt) -> 已成功的结论。
  过期按未命中处理；容量满时按确定顺序（最久未使用，键名字典序兜底）淘汰。

并发同标识：首个到达者成为 leader 真正执行，其余等待其结论。
- leader 成功：等待者共享同一份成功结论（计重放命中）。
- leader 终态失败（后端失败/准入拒绝等）：等待者共享同一个失败结果，
  全批只执行一次、不分叉；标识随即释放，之后的新请求按当时路由重新执行。
- leader 被取消：标识释放，等待者重新认领执行（取消不形成任何结论）。
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Callable

from .errors import (
    IdempotencyConflictError,
    IdempotencyStoreFullError,
    InvalidRetentionConfigError,
)


@dataclass(frozen=True)
class RetentionConfig:
    """保留与容量策略；构造时校验，非法配置在加载阶段直接拒绝。"""

    idempotency_ttl_seconds: float = 300.0
    idempotency_max_entries_per_caller: int = 1000
    reuse_ttl_seconds: float = 60.0
    reuse_max_entries_per_caller: int = 1000

    def __post_init__(self) -> None:
        problems = []
        if not math.isfinite(self.idempotency_ttl_seconds) or self.idempotency_ttl_seconds <= 0:
            problems.append(f"idempotency_ttl_seconds={self.idempotency_ttl_seconds!r} 必须为正有限值")
        if not math.isfinite(self.reuse_ttl_seconds) or self.reuse_ttl_seconds <= 0:
            problems.append(f"reuse_ttl_seconds={self.reuse_ttl_seconds!r} 必须为正有限值")
        if self.idempotency_max_entries_per_caller < 1:
            problems.append("idempotency_max_entries_per_caller 必须 >= 1")
        if self.reuse_max_entries_per_caller < 1:
            problems.append("reuse_max_entries_per_caller 必须 >= 1")
        if problems:
            raise InvalidRetentionConfigError(
                "invalid retention config: " + "; ".join(problems),
                detail={"problems": problems},
            )


@dataclass(frozen=True)
class Conclusion:
    """一次成功结论的快照：重放与复用都必须原样返回这些字段。"""

    model: str
    version: str
    backend: str
    output: str
    attempts: int
    fallback_from: tuple[str, ...]
    cached: bool
    reused: bool = False


@dataclass
class RetentionStats:
    replay_hits: int = 0       # 重放命中（含并发 follower 共享 leader 结论）
    reuse_hits: int = 0        # 跨请求复用命中
    conflict_rejected: int = 0  # 同标识不同内容冲突拒绝
    store_full_rejected: int = 0  # 调用标识容量满快速拒绝
    replay_expired: int = 0    # 调用标识记录过期
    reuse_expired: int = 0     # 复用记录过期
    reuse_evicted: int = 0     # 复用记录被淘汰


@dataclass
class _IdemRecord:
    fingerprint: tuple[str, str]  # (model, prompt) —— 同标识不同内容按冲突拒绝
    created_at: float
    conclusion: Conclusion | None = None  # None 表示在途；成功后写入
    error: BaseException | None = None    # 终态失败（后端失败/准入拒绝等）；取消不设置
    done: asyncio.Event | None = None     # 在途完成信号（成功或失败都置位）


@dataclass
class _ReuseRecord:
    conclusion: Conclusion
    created_at: float
    last_access: float


@dataclass
class Claim:
    """begin() 的返回：completed=直接重放；in_flight=等待他人；claimed=自己执行。"""

    status: str  # "completed" | "in_flight" | "claimed"
    conclusion: Conclusion | None = None
    record: _IdemRecord | None = None


class RetentionStore:
    """调用标识记录 + 复用记录的统一存储（容量与保留期按调用方隔离）。"""

    def __init__(
        self,
        config: RetentionConfig | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._config = config or RetentionConfig()
        self._clock = clock or time.monotonic
        self.stats = RetentionStats()
        self._idem: dict[str, dict[str, _IdemRecord]] = {}
        self._reuse: dict[str, dict[tuple[str, str, str], _ReuseRecord]] = {}
        self._reuse_ttl = self._config.reuse_ttl_seconds

    # ---------- 调用标识（重放） ----------

    def begin(self, caller: str, request_id: str, fingerprint: tuple[str, str]) -> Claim:
        """查询或认领一个调用标识。冲突抛 IdempotencyConflictError，
        容量满抛 IdempotencyStoreFullError。"""
        self._expire_idem(caller)
        records = self._idem.setdefault(caller, {})
        rec = records.get(request_id)
        if rec is not None:
            if rec.fingerprint != fingerprint:
                self.stats.conflict_rejected += 1
                raise IdempotencyConflictError(
                    f"request_id {request_id!r} already used with different model/prompt",
                    detail={
                        "caller_id": caller,
                        "request_id": request_id,
                        "expected": rec.fingerprint,
                        "got": fingerprint,
                    },
                )
            if rec.conclusion is not None:
                self.stats.replay_hits += 1
                return Claim("completed", conclusion=rec.conclusion, record=rec)
            return Claim("in_flight", record=rec)

        if len(records) >= self._config.idempotency_max_entries_per_caller:
            self.stats.store_full_rejected += 1
            raise IdempotencyStoreFullError(
                f"idempotency store full for caller {caller!r} "
                f"({self._config.idempotency_max_entries_per_caller} entries within retention)",
                detail={
                    "caller_id": caller,
                    "max_entries": self._config.idempotency_max_entries_per_caller,
                },
            )
        rec = _IdemRecord(fingerprint=fingerprint, created_at=self._clock(), done=asyncio.Event())
        records[request_id] = rec
        return Claim("claimed", record=rec)

    async def wait_for(self, rec: _IdemRecord) -> tuple[Conclusion | None, BaseException | None]:
        """等待在途记录完成。

        返回 (conclusion, error)，二者互斥：
        - (结论, None)：leader 成功，共享结论（计一次重放命中）；
        - (None, 异常)：leader 终态失败，调用方应原样抛出该异常（不计重放）；
        - (None, None)：leader 被取消、标识已释放，调用方可重新认领执行。
        """
        assert rec.done is not None
        await rec.done.wait()
        if rec.conclusion is not None:
            self.stats.replay_hits += 1
            return rec.conclusion, None
        return None, rec.error

    def complete(self, caller: str, request_id: str, rec: _IdemRecord, conclusion: Conclusion) -> None:
        """leader 成功：写入结论并唤醒等待者。承诺自此生效，保留期内不可挤掉。"""
        rec.conclusion = conclusion
        if rec.done is not None:
            rec.done.set()

    def fail(self, caller: str, request_id: str, rec: _IdemRecord, error: BaseException) -> None:
        """leader 终态失败：把失败结果交给同批等待者后释放标识。

        先把异常挂到记录上唤醒等待者（保证它们共享同一个失败、不会重新执行），
        再从存储中删除记录（失败不占标识）：之后到达/重试的请求按当时路由重新执行。
        """
        rec.error = error
        if rec.done is not None:
            rec.done.set()
        records = self._idem.get(caller)
        if records is not None and records.get(request_id) is rec:
            del records[request_id]

    def abort(self, caller: str, request_id: str, rec: _IdemRecord) -> None:
        """leader 被取消：释放标识（不占位、不留结论），唤醒等待者去重新认领执行。"""
        records = self._idem.get(caller)
        if records is not None and records.get(request_id) is rec:
            del records[request_id]
        if rec.done is not None:
            rec.done.set()

    def _expire_idem(self, caller: str) -> None:
        records = self._idem.get(caller)
        if not records:
            return
        now = self._clock()
        ttl = self._config.idempotency_ttl_seconds
        stale = [
            rid for rid, rec in records.items()
            if rec.conclusion is not None and now - rec.created_at >= ttl
        ]
        for rid in stale:
            del records[rid]
            self.stats.replay_expired += 1

    # ---------- 结果复用 ----------

    def lookup_reuse(self, caller: str, model: str, version: str, prompt: str) -> Conclusion | None:
        """命中返回结论并计 reuse_hits；过期按未命中处理并计 reuse_expired。"""
        entries = self._reuse.get(caller)
        if not entries:
            return None
        key = (model, version, prompt)
        rec = entries.get(key)
        if rec is None:
            return None
        now = self._clock()
        if now - rec.created_at >= self._reuse_ttl:
            del entries[key]
            self.stats.reuse_expired += 1
            return None
        rec.last_access = now
        self.stats.reuse_hits += 1
        return rec.conclusion

    def store_reuse(self, caller: str, model: str, version: str, prompt: str,
                    conclusion: Conclusion) -> None:
        """写入复用记录；超容量时按（最久未访问, 键名）确定顺序淘汰。"""
        self._expire_reuse(caller)
        entries = self._reuse.setdefault(caller, {})
        key = (model, version, prompt)
        now = self._clock()
        entries[key] = _ReuseRecord(conclusion=conclusion, created_at=now, last_access=now)
        while len(entries) > self._config.reuse_max_entries_per_caller:
            victim = min(entries, key=lambda k: (entries[k].last_access, k))
            del entries[victim]
            self.stats.reuse_evicted += 1

    def purge_reuse(self, *, model: str | None = None, version: str | None = None) -> int:
        """按模型/版本定向清理复用记录，返回清理条数。

        只影响之后的新请求；不动调用标识记录（保留期内的承诺不可改写），
        也不影响正在执行的请求（在途记录不在此结构中）。
        """
        removed = 0
        for entries in self._reuse.values():
            stale = [
                k for k in entries
                if (model is None or k[0] == model) and (version is None or k[1] == version)
            ]
            for k in stale:
                del entries[k]
                removed += 1
        return removed

    def shorten_reuse_ttl(self, seconds: float) -> None:
        """缩短复用保留期；只作用于复用记录，不改写已承诺的调用标识结论。"""
        if not math.isfinite(seconds) or seconds <= 0:
            raise InvalidRetentionConfigError(
                f"reuse ttl must be positive finite, got {seconds!r}",
                detail={"reuse_ttl_seconds": seconds},
            )
        self._reuse_ttl = seconds

    def _expire_reuse(self, caller: str) -> None:
        entries = self._reuse.get(caller)
        if not entries:
            return
        now = self._clock()
        stale = [k for k, rec in entries.items() if now - rec.created_at >= self._reuse_ttl]
        for k in stale:
            del entries[k]
            self.stats.reuse_expired += 1

    # ---------- 状态查询 ----------

    def record_counts(self) -> dict:
        """当前记录数（按调用方隔离展示）。"""
        return {
            "idempotency": {c: len(r) for c, r in self._idem.items() if r},
            "reuse": {c: len(e) for c, e in self._reuse.items() if e},
        }

    def snapshot(self) -> dict:
        """状态查询：计数 + 当前记录数 + 生效的保留策略。"""
        return {
            "counters": {
                "replay_hits": self.stats.replay_hits,
                "reuse_hits": self.stats.reuse_hits,
                "conflict_rejected": self.stats.conflict_rejected,
                "store_full_rejected": self.stats.store_full_rejected,
                "replay_expired": self.stats.replay_expired,
                "reuse_expired": self.stats.reuse_expired,
                "reuse_evicted": self.stats.reuse_evicted,
            },
            "records": self.record_counts(),
            "policy": {
                "idempotency_ttl_seconds": self._config.idempotency_ttl_seconds,
                "idempotency_max_entries_per_caller": self._config.idempotency_max_entries_per_caller,
                "reuse_ttl_seconds": self._reuse_ttl,
                "reuse_max_entries_per_caller": self._config.reuse_max_entries_per_caller,
            },
        }
