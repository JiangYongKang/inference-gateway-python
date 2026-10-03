"""推理网关编排：重放 -> 配额 -> 路由 -> 复用 -> 在途合并 -> 后端调用与回退。

计费/统计语义：
- 配额在准入时计一次；重试与回退在同一准入内，不重复计费。
- 调用标识重放（含并发 follower）不再扣额度、不重复计入后端成功、不触发后端调用。
- 复用命中仍按正常准入扣一次额度，但不计真实后端执行。
- 请求被取消（含客户端超时）时退回配额，且不计入成功/失败统计。
- 配额/并发/冲突/容量拒绝计入 rejected；后端全部失败计入 failed。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

from .backends import InferenceBackend
from .concurrency import ConcurrencyLimiter
from .errors import (
    AllBackendsFailedError,
    BackendExecutionError,
    BackendTimeoutError,
    ConcurrencyLimitError,
    GatewayError,
    IdempotencyConflictError,
    NoAvailableBackendError,
    QuotaExceededError,
    RecordCapacityError,
)
from .governance import (
    GovernanceConfig,
    GovernanceCounters,
    ReplayStore,
    ReuseStore,
    StoredConclusion,
)
from .models import InferRequest, InferResponse
from .quotas import QuotaLimiter
from .registry import ModelRegistry, RouteDecision

_log = logging.getLogger("gateway.request")


@dataclass
class Stats:
    success: int = 0
    failed: int = 0
    rejected: int = 0
    cancelled: int = 0
    per_backend: dict[str, int] = field(default_factory=dict)
    # 治理口径新增（只新增字段）：
    backend_executions: int = 0  # 真实后端执行且成功（不含在途合并 follower / 重放 / 复用）
    replay_hits: int = 0         # 调用标识历史结论重放命中（含并发 follower）
    reuse_hits: int = 0          # 跨请求复用命中


class InferenceGateway:
    def __init__(
        self,
        registry: ModelRegistry,
        backends: dict[str, InferenceBackend],
        quota: QuotaLimiter,
        concurrency: ConcurrencyLimiter,
        governance: GovernanceConfig | None = None,
        *,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._registry = registry
        self._backends = dict(backends)
        self._quota = quota
        self._concurrency = concurrency
        self._clock = clock or time.monotonic
        self.stats = Stats()
        if governance is not None:
            self._gov_config: GovernanceConfig | None = governance
            self._gov_counters = GovernanceCounters()
            self._replay = ReplayStore(governance, clock=self._clock, counters=self._gov_counters)
            self._reuse = ReuseStore(governance, clock=self._clock, counters=self._gov_counters)
        else:
            self._gov_config = None
            self._gov_counters = GovernanceCounters()
            self._replay = None
            self._reuse = None

    # ----- 治理运维面 -----

    @property
    def replay_store(self) -> ReplayStore | None:
        return self._replay

    @property
    def reuse_store(self) -> ReuseStore | None:
        return self._reuse

    def governance_snapshot(self) -> dict:
        """治理计数与当前记录数（含在途合并数）。"""
        return {
            "counters": self._gov_counters.as_dict(),
            "replay_records": self._replay.record_count() if self._replay else 0,
            "replay_inflight": self._replay.inflight_count() if self._replay else 0,
            "reuse_records": self._reuse.record_count() if self._reuse else 0,
            "per_caller": {
                "replay_records": {
                    c: self._replay.record_count(c) for c in self._all_replay_callers()
                },
                "replay_inflight": {
                    c: self._replay.inflight_count(c) for c in self._all_replay_callers()
                },
                "reuse_records": {
                    c: self._reuse.record_count(c) for c in self._all_reuse_callers()
                },
            },
        }

    def _all_replay_callers(self) -> list[str]:
        if self._replay is None:
            return []
        seen = set(self._replay._records)
        seen.update(self._replay._inflight)
        return sorted(seen)

    def _all_reuse_callers(self) -> list[str]:
        return list(self._reuse._records) if self._reuse else []

    async def purge_reuse(
        self, *, model: str | None = None, version: str | None = None
    ) -> int:
        if self._reuse is None:
            return 0
        return await self._reuse.purge(model=model, version=version)

    # ----- 请求面 -----

    @staticmethod
    def _fingerprint(request: InferRequest) -> str:
        raw = "\x1f".join([request.prompt, str(request.timeout_ms)])
        return hashlib.sha256(raw.encode()).hexdigest()

    @staticmethod
    def _response_from_conclusion(
        request: InferRequest,
        c: StoredConclusion,
        *,
        replayed: bool = False,
        reuse_hit: bool = False,
    ) -> InferResponse:
        return InferResponse(
            request_id=request.request_id or c.request_id or uuid.uuid4().hex,
            model=c.model,
            version=c.version,
            backend=c.backend,
            output=c.output,
            cached=False,  # 复用/重放不是“在途合并”，cached 语义保持不变
            attempts=c.attempts,
            fallback_from=list(c.fallback_from),
            replayed=replayed,
            reuse_hit=reuse_hit,
            reused_from=c.request_id if reuse_hit else None,
        )

    async def infer(self, request: InferRequest) -> InferResponse:
        rid = request.request_id
        replay_on = self._replay is not None and rid is not None

        # 1) 重放快路径：已提交的历史结论。不扣额度、不执行后端、不重复计后端成功。
        if replay_on:
            try:
                committed = await self._replay.lookup_committed(
                    request.caller_id, rid, request.model, self._fingerprint(request),
                )
            except IdempotencyConflictError as e:
                self.stats.rejected += 1
                _log.info(
                    "input=%s dispatch=replay-fastpath judgement=%s detail=%s",
                    request.model_dump(), e.code, e.detail,
                )
                raise
            if committed is not None:
                self.stats.replay_hits += 1
                self.stats.success += 1
                resp = self._response_from_conclusion(request, committed, replayed=True)
                _log.info(
                    "input=%s dispatch=replay version=%s backend=%s judgement=historical-conclusion",
                    request.model_dump(), resp.version, resp.backend,
                )
                return resp

        # 2) 在途登记（冲突与容量在此快速拒绝，均发生在扣额度之前）
        reserved = False
        join_future = None
        if replay_on:
            try:
                role, join_future = await self._replay.check_or_join(
                    request.caller_id, rid, request.model, self._fingerprint(request),
                )
            except (IdempotencyConflictError, RecordCapacityError) as e:
                self.stats.rejected += 1
                _log.info(
                    "input=%s dispatch=replay-register judgement=%s detail=%s",
                    request.model_dump(), e.code, e.detail,
                )
                raise
            reserved = True
            if role == "follower":
                return await self._serve_follower(request, join_future)

        # 3) leader 路径：配额准入
        quota_held = False
        completed = False
        try:
            try:
                self._quota.acquire(request.caller_id)
            except QuotaExceededError as e:
                if reserved:
                    await self._replay.fail_or_cancel(request.caller_id, rid, e)
                self.stats.rejected += 1
                raise
            quota_held = True

            # 4) 按当时配置解析路由（重放不命中的重试也在此刻重新路由）
            decision = self._registry.resolve(request.model, request.caller_id)

            # 5) 复用查找：同 caller + 同模型别名 + 同实际版本 + 同内容
            if self._reuse is not None:
                key = ReuseStore.make_key(
                    request.model, decision.version, request.prompt, request.timeout_ms,
                )
                hit = await self._reuse.get(request.caller_id, key)
                if hit is not None:
                    if reserved:
                        # 首次调用即命中复用，也为该调用标识建立同内容的重放承诺
                        await self._replay.commit(request.caller_id, rid,
                                                  self._clone_conclusion(hit, rid))
                    self.stats.reuse_hits += 1
                    self.stats.success += 1
                    completed = True  # 复用按正常准入计费，额度不退
                    resp = self._response_from_conclusion(request, hit, reuse_hit=True)
                    _log.info(
                        "input=%s dispatch=reuse version=%s backend=%s reused_from=%s "
                        "judgement=conclusion-reused-no-backend-call",
                        request.model_dump(), resp.version, resp.backend,
                        resp.reused_from,
                    )
                    return resp

            # 6) 真实执行（内含回退链与在途合并）
            resp = await self._execute_backends(request, decision)
            conclusion = StoredConclusion(
                request_id=rid or resp.request_id,
                model=resp.model,
                version=resp.version,
                backend=resp.backend,
                output=resp.output,
                attempts=resp.attempts,
                fallback_from=list(resp.fallback_from),
                created_at=0.0,
                expires_at=0.0,
                fingerprint=self._fingerprint(request),
            )
            if self._reuse is not None:
                await self._reuse.put(request.caller_id, key, self._clone_conclusion(conclusion))
            if reserved:
                await self._replay.commit(request.caller_id, rid,
                                          self._clone_conclusion(conclusion, rid))
            completed = True
            return resp

        except asyncio.CancelledError:
            if reserved:
                await self._replay.fail_or_cancel(request.caller_id, rid, None)
            self.stats.cancelled += 1
            raise
        except ConcurrencyLimitError as e:
            if reserved:
                await self._replay.fail_or_cancel(request.caller_id, rid, e)
            self.stats.rejected += 1
            completed = True  # 拒绝即终态，配额不退（请求未被取消）
            raise
        except (NoAvailableBackendError, AllBackendsFailedError) as e:
            if reserved:
                await self._replay.fail_or_cancel(request.caller_id, rid, e)
            self.stats.failed += 1
            completed = True  # 与旧版一致：后端失败不退配额
            raise
        except GatewayError as e:
            # unknown_model / invalid_route_config 等解析期错误：释放标识、退配额
            if reserved:
                await self._replay.fail_or_cancel(request.caller_id, rid, e)
            raise
        finally:
            if not completed and quota_held:
                self._quota.refund(request.caller_id)

    async def _serve_follower(
        self, request: InferRequest, join_future: asyncio.Future
    ) -> InferResponse:
        """并发同标识 follower：不扣额度、不执行后端，等待 leader 的同一份结论。"""
        try:
            conclusion = await asyncio.shield(join_future)
        except asyncio.CancelledError:
            self.stats.cancelled += 1
            raise
        self.stats.replay_hits += 1
        self.stats.success += 1
        resp = self._response_from_conclusion(request, conclusion, replayed=True)
        _log.info(
            "input=%s dispatch=replay-concurrent version=%s backend=%s "
            "judgement=shared-leader-conclusion",
            request.model_dump(), resp.version, resp.backend,
        )
        return resp

    @staticmethod
    def _clone_conclusion(c: StoredConclusion, request_id: str | None = None) -> StoredConclusion:
        return StoredConclusion(
            request_id=request_id if request_id is not None else c.request_id,
            model=c.model,
            version=c.version,
            backend=c.backend,
            output=c.output,
            attempts=c.attempts,
            fallback_from=list(c.fallback_from),
            created_at=0.0,
            expires_at=0.0,
            fingerprint=c.fingerprint,
        )

    async def _execute_backends(
        self, request: InferRequest, decision: RouteDecision
    ) -> InferResponse:
        attempts = 0
        fallback_from: list[str] = []
        last_err: GatewayError | None = None
        missing = 0

        for backend_name in decision.backends:
            backend = self._backends.get(backend_name)
            if backend is None:
                missing += 1
                fallback_from.append(f"{backend_name}(unregistered)")
                continue
            attempts += 1
            coalesce_key = f"{decision.version}|{backend_name}|{request.prompt}|{request.timeout_ms}"
            try:
                output, shared = await self._concurrency.execute(
                    coalesce_key,
                    lambda b=backend: b.infer(
                        request.prompt,
                        version=decision.version,
                        timeout_ms=request.timeout_ms,
                    ),
                )
            except (BackendTimeoutError, BackendExecutionError) as e:
                last_err = e
                fallback_from.append(backend_name)
                continue

            if not shared:
                # 只有真正发起后端执行的 leader 才计后端成功
                self.stats.backend_executions += 1
                self.stats.per_backend[backend_name] = (
                    self.stats.per_backend.get(backend_name, 0) + 1
                )
            self.stats.success += 1
            resp = InferResponse(
                request_id=request.request_id or uuid.uuid4().hex,
                model=request.model,
                version=decision.version,
                backend=backend_name,
                output=output,
                cached=shared,
                attempts=attempts,
                fallback_from=list(fallback_from),
            )
            _log.info(
                "input=%s dispatch=execute shared=%s version=%s backend=%s "
                "fallback_from=%s judgement=backend-success",
                request.model_dump(), shared, decision.version, backend_name, fallback_from,
            )
            return resp

        if missing == len(decision.backends):
            raise NoAvailableBackendError(
                f"no registered backend for alias {request.model!r} version {decision.version}",
                detail={"version": decision.version, "tried": fallback_from},
            )
        raise AllBackendsFailedError(
            f"all backends failed for {request.model!r} version {decision.version}",
            detail={"version": decision.version, "tried": fallback_from,
                    "last_error": last_err.code if last_err else None},
        )
