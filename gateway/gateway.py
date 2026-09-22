"""推理网关编排：配额 -> 路由 -> 并发控制 -> 后端调用与回退。

计费/统计语义：
- 配额在准入时计一次；重试与回退在同一准入内，不重复计费。
- 请求被取消（含客户端超时）时退回配额，且不计入成功/失败统计。
- 配额拒绝与并发拒绝计入 rejected；后端全部失败计入 failed。
"""

from __future__ import annotations

import asyncio
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


@dataclass
class Stats:
    success: int = 0
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
    ) -> None:
        self._registry = registry
        self._backends = dict(backends)
        self._quota = quota
        self._concurrency = concurrency
        self.stats = Stats()

    async def infer(self, request: InferRequest) -> InferResponse:
        try:
            self._quota.acquire(request.caller_id)
        except QuotaExceededError:
            self.stats.rejected += 1
            raise

        completed = False
        try:
            decision = self._registry.resolve(request.model, request.caller_id)

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
                return InferResponse(
                    request_id=request.request_id or uuid.uuid4().hex,
                    model=request.model,
                    version=decision.version,
                    backend=backend_name,
                    output=output,
                    cached=shared,
                    attempts=attempts,
                    fallback_from=list(fallback_from),
                )

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
