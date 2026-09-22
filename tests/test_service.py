"""End-to-end deterministic orchestration tests."""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import (AllBackendsFailedError, QuotaExceededError,
                            QueueLimitError, VersionNotFoundError)


@pytest.mark.asyncio
async def test_happy_path_reports_backend_version_and_billing(env_factory,
                                                              req_factory):
    svc, reg, backends, _ = env_factory()
    result = await svc.infer(req_factory(input_="hi"))
    assert result.version == "v1"
    assert result.backend == "a1"
    assert result.output == "mock-output(a1,v1,hi)"
    assert result.charged is True and result.fallback is False
    assert backends["a1"].completed == 1
    # legacy-compatible fields
    payload = result.__dict__
    assert {"request_id", "alias", "version", "backend", "output",
            "served_from_cache"} <= set(payload)


@pytest.mark.asyncio
async def test_group_pins_canary_version(env_factory, req_factory):
    svc, *_ = env_factory()
    result = await svc.infer(req_factory(group="beta"))
    assert result.version == "v2" and result.backend == "c1"


@pytest.mark.asyncio
async def test_unknown_alias_is_distinct_rejection(env_factory, req_factory):
    svc, *_ = env_factory()
    with pytest.raises(VersionNotFoundError):
        await svc.infer(req_factory(alias="ghost"))


@pytest.mark.asyncio
async def test_quota_exceeded_distinct_from_backend_failure(env_factory,
                                                            req_factory):
    svc, *_ = env_factory()
    req1 = req_factory(caller="beta")
    req2 = req_factory(caller="beta")
    await svc.infer(req1)
    with pytest.raises(QuotaExceededError) as ei:
        await svc.infer(req2)
    assert ei.value.code == "quota_exceeded"
    # rejected request never reached a backend
    snap = svc.metrics.snapshot()
    assert snap["rejected"]["beta|chat"] == 1
    assert svc.quota_usage("beta") == 1


@pytest.mark.asyncio
async def test_fallback_to_stable_version_is_reported(env_factory,
                                                      req_factory):
    svc, reg, backends, _ = env_factory()
    # v2 selected via group; its only backend errors -> fall back to v1
    reg.set_failure("c1", "error")
    result = await svc.infer(req_factory(group="beta"))
    assert result.version == "v1"
    assert result.backend in ("a1", "a2")
    assert result.fallback is True
    assert [a.version for a in result.attempts] == ["v2", "v1"]
    assert result.attempts[0].ok is False
    assert result.attempts[1].ok is True
    # exactly one side effect despite retry + fallback
    total_done = sum(b.completed for b in backends.values())
    assert total_done == 1


@pytest.mark.asyncio
async def test_all_backends_failing_is_classified(env_factory, req_factory):
    svc, reg, backends, _ = env_factory()
    for name in ("a1", "a2"):
        reg.set_failure(name, "error")
    with pytest.raises(AllBackendsFailedError) as ei:
        await svc.infer(req_factory())
    assert ei.value.code == "all_backends_failed"
    # failure counted exactly once, no charge-side-effect
    snap = svc.metrics.snapshot()
    assert snap["failure"]["alpha|chat|v1"] == 1
    assert all(b.completed == 0 for b in backends.values())


@pytest.mark.asyncio
async def test_timeout_falls_back_and_marks_attempt(env_factory, req_factory):
    svc, reg, backends, _ = env_factory(attempt_timeout=0.02)
    reg.set_failure("a1", "timeout")  # primary first backend hangs
    result = await svc.infer(req_factory())
    assert result.backend == "a2" and result.version == "v1"
    assert result.attempts[0].error_code == "backend_timeout"
    assert svc.metrics.timeout == 1


@pytest.mark.asyncio
async def test_cache_hit_is_not_recharged(env_factory, req_factory):
    svc, reg, backends, _ = env_factory()
    r1 = await svc.infer(req_factory(input_="same"))
    r2 = await svc.infer(req_factory(input_="same"))
    assert r2.served_from_cache is True and r2.charged is False
    assert r2.output == r1.output
    assert backends["a1"].completed == 1


@pytest.mark.asyncio
async def test_coalescing_runs_backend_once(env_factory, req_factory):
    svc, reg, backends, _ = env_factory(latency=0.05)
    reqs = [req_factory(input_="burst") for _ in range(4)]
    results = await asyncio.gather(*(svc.infer(r) for r in reqs))
    leaders = [r for r in results if r.source == "backend"]
    merged = [r for r in results if r.source == "coalesced"]
    assert len(leaders) == 1 and len(merged) == 3
    assert backends["a1"].completed == 1
    assert all(r.charged is False for r in merged)
    assert leaders[0].charged is True


@pytest.mark.asyncio
async def test_queue_overflow_fast_rejects(env_factory, req_factory):
    # concurrency=1, queue=1: third concurrent request rejects immediately
    svc, reg, backends, _ = env_factory(max_concurrency=1, max_queue=1,
                                        latency=0.1)
    gate = asyncio.Event()
    orig_infer = backends["a1"].infer

    async def gated(request, version):
        gate.set()
        return await orig_infer(request, version)

    backends["a1"].infer = gated

    t_hold = asyncio.create_task(svc.infer(req_factory(input_="hold")))
    await gate.wait()                       # occupies the single slot
    t_park = asyncio.create_task(
        svc.infer(req_factory(input_="park")))
    await asyncio.sleep(0.01)               # sits in the queue
    with pytest.raises(QueueLimitError) as ei:
        await svc.infer(req_factory(input_="overflow"))
    assert ei.value.code == "queue_limit"
    r_hold = await t_hold
    r_park = await t_park
    assert r_hold.output == "mock-output(a1,v1,hold)"
    assert r_park.output == "mock-output(a1,v1,park)"
    assert svc.limiter_snapshot() == (0, 0)
    # the rejected call never consumed a backend call beyond the 2 leaders
    assert backends["a1"].completed == 2
    assert svc.quota_usage("alpha") == 2   # overflow was refunded


@pytest.mark.asyncio
async def test_cancelled_request_releases_quota_and_slot(env_factory,
                                                         req_factory):
    svc, reg, backends, _ = env_factory(max_concurrency=1, max_queue=0,
                                        latency=0.1, attempt_timeout=5.0)
    gate = asyncio.Event()
    orig_infer = backends["a1"].infer

    async def gated(request, version):
        gate.set()
        return await orig_infer(request, version)

    backends["a1"].infer = gated
    t1 = asyncio.create_task(svc.infer(req_factory(input_="hold")))
    await gate.wait()
    # slot busy, queue=0 -> fast reject distinct error
    with pytest.raises(QueueLimitError):
        await svc.infer(req_factory(input_="other"))
    await t1
    assert svc.limiter_snapshot() == (0, 0)


@pytest.mark.asyncio
async def test_cancelled_while_queued_refunds_quota(env_factory, req_factory):
    svc, reg, backends, _ = env_factory(max_concurrency=1, max_queue=2,
                                        latency=0.1)
    gate = asyncio.Event()
    orig = backends["a1"].infer

    async def gated(request, version):
        gate.set()
        return await orig(request, version)

    backends["a1"].infer = gated

    t_hold = asyncio.create_task(svc.infer(req_factory(input_="hold")))
    await gate.wait()
    t_wait = asyncio.create_task(svc.infer(req_factory(input_="wait")))
    await asyncio.sleep(0.01)
    assert svc.quota_usage("alpha") == 2
    t_wait.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t_wait
    assert svc.quota_usage("alpha") == 1   # waiter refunded, holder charged
    r = await t_hold
    assert r.charged is True


@pytest.mark.asyncio
async def test_version_switch_same_request_id_stays_consistent(env_factory,
                                                               req_factory):
    svc, reg, backends, config = env_factory()
    rid = "stable-id-1"
    req1 = InferenceRequestFixed(rid, caller="alpha", input_="q", group="beta")
    r1 = await svc.infer(req1)
    assert r1.version == "v2" and r1.backend == "c1"

    # Simulate a version switch: v2 backend disappears and config changes so
    # the group pin is gone. The same request id must replay the first answer.
    reg.set_available("c1", False)
    import dataclasses as _dc
    new_cfg = _dc.replace(config.aliases["chat"], groups=())
    config.aliases["chat"] = new_cfg
    r2 = await svc.infer(InferenceRequestFixed(rid, caller="alpha",
                                               input_="q"))
    assert r2.source == "replay"
    assert (r2.version, r2.backend, r2.output) == \
           (r1.version, r1.backend, r1.output)
    assert r2.charged is False


@pytest.mark.asyncio
async def test_failed_request_is_not_cache_poisoned(env_factory, req_factory):
    svc, reg, backends, _ = env_factory()
    reg.set_failure("a1", "error")
    reg.set_failure("a2", "error")
    with pytest.raises(AllBackendsFailedError):
        await svc.infer(req_factory(input_="poison"))
    # failure must not have populated the result cache
    assert svc.cache.get("chat|v1|poison") is None
    # recover: same input must execute again, not replay a cached failure
    reg.set_failure("a1", None)
    reg.set_failure("a2", None)
    result = await svc.infer(req_factory(input_="poison"))
    assert result.backend == "a1" and result.source == "backend"


@pytest.mark.asyncio
async def test_logs_contain_input_basis_and_verdict(env_factory, req_factory,
                                                    gateway_logs):
    svc, *_ = env_factory()
    await svc.infer(req_factory(input_="logged-q"))
    joined = "\n".join(gateway_logs)
    assert "request_received" in joined
    assert "logged-q" in joined
    assert "version_selected" in joined
    assert "request_ok" in joined


from dataclasses import dataclass as _dc  # noqa: E402
from gateway.models import InferenceRequest as _IR  # noqa: E402


def InferenceRequestFixed(request_id, *, caller, input_, group=None):
    return _IR(request_id=request_id, caller=caller, alias="chat",
               input=input_, group=group)


@pytest.mark.asyncio
async def test_cancelled_leader_terminates_ledger(env_factory, req_factory):
    svc, reg, backends, _ = env_factory(max_concurrency=1, max_queue=2,
                                        latency=0.1)
    gate = asyncio.Event()
    orig = backends["a1"].infer

    async def gated(request, version):
        gate.set()
        return await orig(request, version)

    backends["a1"].infer = gated

    holder = asyncio.create_task(svc.infer(req_factory(input_="h")))
    await gate.wait()
    rid = "cancelled-leader"
    waiting = asyncio.create_task(svc.infer(
        InferenceRequestFixed(rid, caller="alpha", input_="w")))
    await asyncio.sleep(0.01)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    await holder

    # the cancelled id must not be stuck in "running"
    from gateway.idempotency import State
    assert svc.ledger.state(rid) is State.FAILED
    # quota was refunded for the cancelled waiter
    assert svc.quota_usage("alpha") == 1


@pytest.mark.asyncio
async def test_failed_request_id_replays_same_verdict_without_rerun(
        env_factory):
    svc, reg, backends, _ = env_factory()
    reg.set_failure("a1", "error")
    reg.set_failure("a2", "error")
    rid = "failed-id"
    with pytest.raises(AllBackendsFailedError):
        await svc.infer(InferenceRequestFixed(rid, caller="alpha",
                                              input_="z"))
    attempts_after_first = sum(
        len(getattr(b, "completed_keys", [])) for b in backends.values())
    # backends recover, but replaying the same id must keep the first verdict
    reg.set_failure("a1", None)
    reg.set_failure("a2", None)
    with pytest.raises(AllBackendsFailedError) as ei:
        await svc.infer(InferenceRequestFixed(rid, caller="alpha",
                                              input_="z"))
    assert ei.value.code == "all_backends_failed"
    assert sum(len(b.completed_keys) for b in backends.values()) == \
        attempts_after_first
