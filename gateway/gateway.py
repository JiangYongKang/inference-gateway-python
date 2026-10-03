"""推理网关编排：重放/复用判定 -> 配额 -> 路由 -> 并发控制 -> 后端调用与回退。

计费/统计语义：
- 配额在准入时计一次；重试与回退在同一准入内，不重复计费。
- 请求被取消（含客户端超时）时退回配额，且不计入成功/失败统计。
- 配额拒绝与并发拒绝计入 rejected；后端全部失败计入 failed。
- 重放命中（同调用标识的历史结论）不扣配额、不计后端成功、不触发后端调用。
- 复用命中（同调用方+模型+实际版本+内容）正常准入扣配额，但不计后端成功。

重放/复用语义（见 retention.py）：
- 同调用方同 request_id 首次成功后，保留期内重放返回同一份结论并标记 replayed。
- 并发同标识只真正执行一次；首次失败不占标识，重试按当时路由重新执行。
- 复用只在同调用方、同模型、同实际版本、同内容之间生效；版本变化自然未命中。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field

from .backends import InferenceBackend
from .concurrency import ConcurrencyLimiter
from .errors import (
    AllBackendsFailedError,
    BackendExecutionError,
    BackendTimeoutError,
    ConcurrencyLimitError,
    GatewayError,
    NoAvailableBackendError,
    QuotaExceededError,
)
from .models import InferRequest, InferResponse
from .quotas import QuotaLimiter
from .registry import ModelRegistry
from .retention import Conclusion, RetentionStore

log = logging.getLogger("gateway")


@dataclass
class Stats:
    success: int = 0   # 真实后端执行成功（不含复用/重放）
    failed: int = 0
    rejected: int = 0
    cancelled: int = 0
    per_backend: dict[str, int] = field(default_factory=dict)


class InferenceGateway:
    def __init__(
        self,
        registry: ModelRegistry,
        backends: dict[str, InferenceBackend],
        quota: QuotaLimiter,
        concurrency: ConcurrencyLimiter,
        retention: RetentionStore | None = None,
    ) -> None:
        self._registry = registry
        self._backends = dict(backends)
        self._quota = quota
        self._concurrency = concurrency
        self._retention = retention
        self.stats = Stats()

    async def infer(self, request: InferRequest) -> InferResponse:
        if self._retention is not None and request.request_id:
            return await self._infer_idempotent(request)
        return await self._execute(request)

    # ---------- 重放路径 ----------

    async def _infer_idempotent(self, request: InferRequest) -> InferResponse:
        store = self._retention
        assert store is not None and request.request_id
        fingerprint = (request.model, request.prompt)

        while True:
            claim = store.begin(request.caller_id, request.request_id, fingerprint)
            if claim.status == "completed":
                log.info(
                    "replay hit caller=%s request_id=%s version=%s backend=%s",
                    request.caller_id, request.request_id,
                    claim.conclusion.version, claim.conclusion.backend,
                )
                return self._replay_response(request, claim.conclusion)
            if claim.status == "in_flight":
                conclusion = await store.wait_for(claim.record)
                if conclusion is not None:
                    log.info(
                        "replay hit (concurrent leader) caller=%s request_id=%s version=%s",
                        request.caller_id, request.request_id, conclusion.version,
                    )
                    return self._replay_response(request, conclusion)
                continue  # leader 失败：标识已释放，按当时路由重新认领执行

            # claimed：本次由自己真正执行
            rec = claim.record
            try:
                response = await self._execute(request)
            except BaseException:
                store.abort(request.caller_id, request.request_id, rec)
                raise
            store.complete(
                request.caller_id, request.request_id, rec,
                self._conclusion_of(request, response),
            )
            return response

    @staticmethod
    def _replay_response(request: InferRequest, c: Conclusion) -> InferResponse:
        return InferResponse(
            request_id=request.request_id,
            model=c.model,
            version=c.version,
            backend=c.backend,
            output=c.output,
            cached=c.cached,
            attempts=c.attempts,
            fallback_from=list(c.fallback_from),
            replayed=True,
            reused=c.reused,
        )

    @staticmethod
    def _conclusion_of(request: InferRequest, r: InferResponse) -> Conclusion:
        return Conclusion(
            model=r.model,
            version=r.version,
            backend=r.backend,
            output=r.output,
            attempts=r.attempts,
            fallback_from=tuple(r.fallback_from),
            cached=r.cached,
            reused=r.reused,
        )

    # ---------- 真实执行路径 ----------

    async def _execute(self, request: InferRequest) -> InferResponse:
        try:
            self._quota.acquire(request.caller_id)
        except QuotaExceededError:
            self.stats.rejected += 1
            raise

        completed = False
        try:
            decision = self._registry.resolve(request.model, request.caller_id)

            # 结果复用：同调用方+模型+实际版本+内容，且仍在保留窗口内
            if self._retention is not None:
                hit = self._retention.lookup_reuse(
                    request.caller_id, request.model, decision.version, request.prompt)
                if hit is not None:
                    completed = True  # 正常准入：配额不退
                    log.info(
                        "reuse hit caller=%s model=%s version=%s backend=%s",
                        request.caller_id, request.model, hit.version, hit.backend,
                    )
                    return InferResponse(
                        request_id=request.request_id or uuid.uuid4().hex,
                        model=request.model,
                        version=hit.version,
                        backend=hit.backend,
                        output=hit.output,
                        cached=hit.cached,
                        attempts=hit.attempts,
                        fallback_from=list(hit.fallback_from),
                        reused=True,
                    )

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
                except ConcurrencyLimitError:
                    self.stats.rejected += 1
                    completed = True  # 拒绝即终态，配额不退（请求未被取消）
                    raise

                self.stats.success += 1
                self.stats.per_backend[backend_name] = (
                    self.stats.per_backend.get(backend_name, 0) + 1
                )
                completed = True
                response = InferResponse(
                    request_id=request.request_id or uuid.uuid4().hex,
                    model=request.model,
                    version=decision.version,
                    backend=backend_name,
                    output=output,
                    cached=shared,
                    attempts=attempts,
                    fallback_from=list(fallback_from),
                )
                if self._retention is not None:
                    self._retention.store_reuse(
                        request.caller_id, request.model, decision.version,
                        request.prompt, self._conclusion_of(request, response),
                    )
                log.info(
                    "real execution caller=%s model=%s version=%s backend=%s attempts=%d",
                    request.caller_id, request.model, decision.version, backend_name, attempts,
                )
                return response

            completed = True
            self.stats.failed += 1
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
        except asyncio.CancelledError:
            self.stats.cancelled += 1
            raise
        finally:
            if not completed:
                self._quota.refund(request.caller_id)

    # ---------- 治理接口 ----------

    def purge_reuse(self, *, model: str | None = None, version: str | None = None) -> int:
        """按模型/版本定向清理复用记录；不影响在途请求与已承诺的调用标识结论。"""
        if self._retention is None:
            return 0
        removed = self._retention.purge_reuse(model=model, version=version)
        log.info("reuse purged model=%s version=%s removed=%d", model, version, removed)
        return removed

    def shorten_reuse_ttl(self, seconds: float) -> None:
        if self._retention is not None:
            self._retention.shorten_reuse_ttl(seconds)

    def retention_stats(self) -> dict:
        """重放/复用计数与当前记录数。"""
        if self._retention is None:
            return {}
        return self._retention.snapshot()
