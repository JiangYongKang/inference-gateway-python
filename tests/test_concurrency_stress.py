"""Deterministic concurrency race: tight bounds + simultaneous fan-out.

With max_concurrency=1 and max_queue=1 and no cache merging (distinct inputs),
a burst of N simultaneous requests must yield an exact, repeatable split of
success vs queue-limit rejections; quota usage afterwards reflects exactly
the accepted calls (rejected ones are refunded).
"""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import QueueLimitError
from gateway.models import InferenceRequest


@pytest.mark.asyncio
async def test_simultaneous_burst_has_expected_admission_split(env_factory):
    svc, reg, backends, _ = env_factory(
        max_concurrency=1, max_queue=1, latency=0.05,
        quotas={"alpha": {"limit": 100}}, attempt_timeout=2.0)

    n = 12
    reqs = [InferenceRequest(request_id=f"burst-{i}", caller="alpha",
                             alias="chat", input=f"distinct-{i}")
            for i in range(n)]

    results = await asyncio.gather(
        *(svc.infer(r) for r in reqs), return_exceptions=True)

    ok = [r for r in results if not isinstance(r, BaseException)]
    rejected = [r for r in results
                if isinstance(r, QueueLimitError)]
    assert len(ok) + len(rejected) == n
    # FIFO depth 1 => at most 2 in flight => at least n-2 fast rejections.
    assert len(rejected) == n - 2
    assert len(ok) == 2
    assert svc.limiter_snapshot() == (0, 0)
    # exactly the two accepted calls consumed quota
    assert svc.quota_usage("alpha") == 2
    snap = svc.metrics.snapshot()
    assert snap["rejected"]["alpha|chat"] == n - 2
    # no backend call was ever dropped silently
    completed = sum(b.completed for b in backends.values())
    assert completed == 2


@pytest.mark.asyncio
async def test_per_caller_quotas_are_isolated_under_concurrency(env_factory):
    svc, reg, backends, _ = env_factory(
        max_concurrency=8, max_queue=4, latency=0.02,
        quotas={"alpha": {"limit": 100}, "beta": {"limit": 1}},
        attempt_timeout=2.0)

    async def call(caller, rid):
        return await svc.infer(InferenceRequest(
            request_id=rid, caller=caller, alias="chat",
            input=f"{caller}-{rid}"))

    beta_results = await asyncio.gather(
        *(call("beta", f"b-{i}") for i in range(6)),
        return_exceptions=True)
    beta_ok = [r for r in beta_results if not isinstance(r, BaseException)]
    beta_rej = [r for r in beta_results
                if getattr(r, "code", None) == "quota_exceeded"]
    assert len(beta_ok) == 1 and len(beta_rej) == 5

    alpha_results = await asyncio.gather(
        *(call("alpha", f"a-{i}") for i in range(6)),
        return_exceptions=True)
    assert all(not isinstance(r, BaseException) for r in alpha_results)
    assert svc.quota_usage("alpha") == 6
    assert svc.quota_usage("beta") == 1
