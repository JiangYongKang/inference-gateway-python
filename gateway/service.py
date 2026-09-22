"""Request orchestration.

Pipeline per request::

  resolve alias -> version (Router)
  idempotency ledger (request_id: unique execution / replay)
  per-caller quota reservation (refunded on reject/cancel-before-work)
  global concurrency slot + bounded queue (fast reject when full)
  result cache / singleflight coalescing (same alias+version+input)
  version backend list, then stable-version fallback on timeout/error

Guarantees implemented here:

* the four *reject* reasons stay distinct (version not found / no backend /
  illegal canary / quota vs concurrency vs queue);
* a fallback chain performs at most one backend call per listed backend and
  charges/counts a logical request exactly once;
* cancelled/timed-out callers release concurrency + refund quota and never
  write the cache or success counters;
* every stage emits a JSON log line (input, selection basis, verdict).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable

from gateway.backend import BackendRegistry
from gateway.cache import ResultCache
from gateway.concurrency import ConcurrencyLimiter
from gateway.errors import (AllBackendsFailedError, GatewayError,
                            QuotaExceededError, RequestCancelledError,
                            VersionNotFoundError)
from gateway.idempotency import IdempotencyLedger, State
from gateway.models import (BackendAttempt, GatewayConfig, InferenceRequest,
                            InferenceResult)
from gateway.quota import QuotaManager
from gateway.router import Router
from gateway.singleflight import SingleFlight
from gateway.telemetry import Metrics, configure_logging, log_event

DEFAULT_TIMEOUT = 5.0


class InferenceService:
    def __init__(self,
                 config: GatewayConfig,
                 registry: BackendRegistry,
                 metrics: Metrics | None = None,
                 clock: Callable[[], float] | None = None,
                 log: logging.Logger | None = None,
                 cache_ttl: float = 300.0,
                 attempt_timeout: float = DEFAULT_TIMEOUT) -> None:
        self._config = config
        self._registry = registry
        self.metrics = metrics or Metrics()
        self._clock = clock or time.monotonic
        self.log = log or configure_logging()
        self._router = Router(backend_available=registry.available)
        self._quotas = QuotaManager(config.quotas, clock=self._clock)
        self._limiter = ConcurrencyLimiter(config.max_concurrency,
                                           config.max_queue)
        self._ledger = IdempotencyLedger()
        self._singleflight = SingleFlight()
        self._cache = ResultCache(ttl_seconds=cache_ttl, clock=self._clock)
        self._attempt_timeout = attempt_timeout

    # -- test/admin helpers -------------------------------------------------
    @property
    def ledger(self) -> IdempotencyLedger:
        return self._ledger

    @property
    def cache(self) -> ResultCache:
        return self._cache

    def quota_usage(self, caller: str) -> int:
        return self._quotas.usage(caller)

    def limiter_snapshot(self) -> tuple[int, int]:
        return self._limiter.active, self._limiter.in_queue

    # -- main entry ---------------------------------------------------------
    async def infer(self, request: InferenceRequest):
        log_event(self.log, "request_received", request_id=request.request_id,
                  caller=request.caller, alias=request.alias,
                  group=request.group, input=request.input)

        # 1) route: alias -> version (distinct error classes on bad input)
        alias_cfg = self._config.aliases.get(request.alias)
        if alias_cfg is None:
            self._reject(request, "alias_not_registered")
            raise VersionNotFoundError(
                f"alias not registered: {request.alias}",
                details={"alias": request.alias})
        version = self._router.resolve(alias_cfg, request.caller,
                                       request.group)
        log_event(self.log, "version_selected", request_id=request.request_id,
                  caller=request.caller, alias=request.alias,
                  version=version,
                  basis=(f"group={request.group}" if request.group
                         else "canary/weights"))

        # 2) idempotency ledger
        role = await self._ledger.begin(request.request_id)
        if role == "replay":
            log_event(self.log, "idempotent_replay",
                      request_id=request.request_id, role=role)
            return self._present_replay(request)
        if role == "replay_error":
            log_event(self.log, "idempotent_replay",
                      request_id=request.request_id, role=role)
            # Replay the original verdict exactly: no re-charge, no rerun,
            # and never a contradictory success for the same request id.
            try:
                self._ledger.get_outcome(request.request_id)
            except GatewayError as exc:
                raise
        if role == "follower":
            try:
                result = await self._ledger.wait_outcome(request.request_id)
            except asyncio.CancelledError:
                raise
            log_event(self.log, "idempotent_follower_result",
                      request_id=request.request_id,
                      version=result.version, backend=result.backend)
            return self._as_coalesced(result)

        # unique leader from here on; a leader that gives up (client cancel /
        # timeout) must still terminate the ledger entry so the same request
        # id cannot hang forever in "running" state.
        try:
            return await self._lead(request, alias_cfg, version)
        except asyncio.CancelledError:
            self._ledger.fail(request.request_id,
                              RequestCancelledError(
                                  "leader cancelled before completion",
                                  details={"request_id": request.request_id}))
            raise

    # -- unique leader path -------------------------------------------------
    async def _lead(self, request, alias_cfg, version):
        """Leader path: quota reservation -> bounded slot -> execution.

        Quota is refunded exactly when the caller departs *before* the backend
        chain runs (fast reject / queued cancellation / in-flight cancel). A
        chain that actually ran keeps the unit (success or failure).
        """
        if not self._quotas.try_acquire(request.caller):
            err = QuotaExceededError(
                f"caller quota exhausted: {request.caller}",
                details={"caller": request.caller,
                         "alias": request.alias,
                         "limit": self._quotas.limit_for(request.caller),
                         "used": self._quotas.usage(request.caller)})
            self.metrics.record_rejected(request.caller, request.alias)
            log_event(self.log, "rejected", request_id=request.request_id,
                      reason="quota_exceeded",
                      usage=self._quotas.usage(request.caller),
                      limit=self._quotas.limit_for(request.caller))
            self._ledger.fail(request.request_id, err)
            raise err

        try:
            async with self._limiter.acquire():
                return await self._execute(request, alias_cfg, version)
        except asyncio.CancelledError:
            self._quotas.refund(request.caller)
            log_event(self.log, "request_cancelled",
                      request_id=request.request_id)
            raise
        except GatewayError as exc:
            # Queue/concurrency fast rejects happen before any backend work.
            if exc.code in ("queue_limit", "concurrency_limit"):
                self._quotas.refund(request.caller)
                self.metrics.record_rejected(request.caller, request.alias)
                log_event(self.log, "rejected",
                          request_id=request.request_id, reason=exc.code)
            raise

    # -- cache/coalesce + fallback chain -----------------------------------
    async def _execute(self, request, alias_cfg, version):
        spec = next(v for v in alias_cfg.versions if v.version == version)
        cache_key = f"{request.alias}|{version}|{request.input}"

        cached = self._cache.get(cache_key)
        if cached is not None:
            result = cached.value
            log_event(self.log, "cache_hit", request_id=request.request_id,
                      key=cache_key, version=result.version,
                      backend=result.backend)
            self.metrics.record_success(request.caller, request.alias,
                                        result.version)
            self._ledger.complete(request.request_id, result)
            return self._as_cached(result)

        async def _worker():
            return await self._run_chain(request, alias_cfg, version,
                                         spec.backends)

        try:
            result, coalesced = await self._singleflight.execute(
                cache_key, _worker)
        except asyncio.CancelledError:
            self._quotas.refund(request.caller)
            log_event(self.log, "cancelled_in_flight",
                      request_id=request.request_id)
            raise
        except GatewayError as exc:
            self._ledger.fail(request.request_id, exc)
            self.metrics.record_failure(request.caller, request.alias, version)
            log_event(self.log, "chain_failed",
                      request_id=request.request_id, reason=exc.code)
            raise

        # Only the single leader run is cached; coalesced piggy-backers never
        # rewrite it, and a run with no surviving waiter is not cached here.
        if not coalesced:
            self._cache.put(cache_key, result, version=result.version,
                            backend=result.backend)
        self._ledger.complete(request.request_id, result)

        if coalesced:
            log_event(self.log, "coalesced", request_id=request.request_id,
                      key=cache_key, version=result.version,
                      backend=result.backend)
            self.metrics.record_success(request.caller, request.alias,
                                        result.version)
            return self._as_coalesced(result)

        self.metrics.record_success(request.caller, request.alias,
                                    result.version)
        log_event(self.log, "request_ok", request_id=request.request_id,
                  version=result.version, backend=result.backend,
                  fallback=result.fallback, attempts=len(result.attempts))
        return result

    async def _run_chain(self, request, alias_cfg, version, backend_names):
        """Try version backends in order, then stable-version fallback."""
        attempts: list[BackendAttempt] = []
        chain: list[tuple[str, list[str]]] = [(version, list(backend_names))]
        if alias_cfg.stable_version and \
                alias_cfg.stable_version != version:
            stable_spec = next(v for v in alias_cfg.versions
                               if v.version == alias_cfg.stable_version)
            chain.append((stable_spec.version, list(stable_spec.backends)))

        last_exc = None
        for ver, names in chain:
            for name in names:
                backend = self._registry.get(name)
                started = self._clock()
                log_event(self.log, "backend_attempt",
                          request_id=request.request_id, backend=name,
                          version=ver,
                          position="primary" if ver == version else "fallback")
                try:
                    output = await asyncio.wait_for(
                        backend.infer(request, ver),
                        timeout=self._attempt_timeout)
                except asyncio.TimeoutError:
                    self.metrics.record_timeout()
                    attempts.append(BackendAttempt(
                        backend=name, version=ver,
                        latency=self._clock() - started, ok=False,
                        error_code="backend_timeout"))
                    last_exc = "backend_timeout"
                    log_event(self.log, "backend_timeout",
                              request_id=request.request_id, backend=name,
                              version=ver)
                    continue
                except asyncio.CancelledError:
                    raise
                except GatewayError as exc:
                    attempts.append(BackendAttempt(
                        backend=name, version=ver,
                        latency=self._clock() - started, ok=False,
                        error_code=exc.code))
                    last_exc = exc.code
                    log_event(self.log, "backend_error",
                              request_id=request.request_id, backend=name,
                              version=ver, code=exc.code)
                    continue

                used_fallback = (ver != version)
                attempts.append(BackendAttempt(
                    backend=name, version=ver,
                    latency=self._clock() - started, ok=True))
                return InferenceResult(
                    request_id=request.request_id, alias=request.alias,
                    version=ver, backend=name, output=output,
                    served_from_cache=False,
                    attempts=tuple(attempts),
                    charged=True,
                    source="backend",
                    fallback=used_fallback)

        raise AllBackendsFailedError(
            "all candidate backends failed",
            details={"alias": request.alias, "version": version,
                     "last_error": last_exc,
                     "attempts": [a.backend for a in attempts]})

    # -- presentation helpers ----------------------------------------------
    def _as_cached(self, result):
        from dataclasses import replace
        return replace(result, served_from_cache=True, charged=False,
                       source="cache")

    def _as_coalesced(self, result):
        from dataclasses import replace
        return replace(result, request_id=result.request_id, charged=False,
                       source="coalesced", served_from_cache=False)

    def _present_replay(self, request):
        outcome = self._ledger.get_outcome(request.request_id)
        from dataclasses import replace
        result = replace(outcome, charged=False, source="replay",
                         served_from_cache=False)
        self.metrics.record_success(request.caller, request.alias,
                                    result.version)
        log_event(self.log, "request_ok", request_id=request.request_id,
                  version=result.version, backend=result.backend,
                  source="replay")
        return result

    def _reject(self, request, reason):
        self.metrics.record_rejected(request.caller, request.alias)
        log_event(self.log, "rejected", request_id=request.request_id,
                  reason=reason)
