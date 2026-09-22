"""并发竞争：有界并发快速拒绝、相同请求合并、取消后配额释放且不污染统计。"""

import asyncio
import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import QueueFullError
from gateway.models import InferRequest

log = logging.getLogger("gateway.test")


def req(prompt="p", caller="c1", **kw):
    return InferRequest(model="m", caller_id=caller, prompt=prompt, **kw)


def slow_backend(name, latency_ms=50, **kw):
    return DeterministicBackend(name, latency_ms=latency_ms, **kw)


async def test_queue_full_fast_reject(make_gateway):
    gw = make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": slow_backend("b", latency_ms=100)},
        max_concurrent=1, max_queue=1,
    )
    t1 = asyncio.ensure_future(gw.infer(req(prompt="a")))
    t2 = asyncio.ensure_future(gw.infer(req(prompt="b")))
    await asyncio.sleep(0.02)  # t1 占用槽位，t2 排队
    with pytest.raises(QueueFullError) as ei:
        await gw.infer(req(prompt="c"))
    log.info("input=3rd-concurrent basis=queue:1/1 judgement=%s", ei.value.code)
    assert ei.value.code == "queue_full"
    assert gw.stats.rejected == 1
    await asyncio.gather(t1, t2)
    assert gw.stats.success == 2


async def test_concurrent_requests_isolated(make_gateway):
    """并发请求互不串扰：不同 prompt 得到各自确定的结果。"""
    gw = make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": slow_backend("b", latency_ms=20)},
        max_concurrent=8, max_queue=16,
    )
    prompts = [f"p{i}" for i in range(20)]
    results = await asyncio.gather(*[gw.infer(req(prompt=p)) for p in prompts])
    for p, r in zip(prompts, results):
        expected = DeterministicBackend("b")
        exp = await expected.infer(p, version="v1", timeout_ms=None)
        assert r.output == exp
    assert gw.stats.success == 20
    log.info("input=20-parallel basis=isolated-keys judgement=all-correct")


async def test_identical_inflight_requests_coalesced(make_gateway):
    backend = slow_backend("b", latency_ms=50)
    gw = make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": backend},
        max_concurrent=4, max_queue=8,
    )
    results = await asyncio.gather(*[gw.infer(req(prompt="same")) for _ in range(5)])
    log.info("input=5-identical basis=coalesce-key judgement backend_calls=%d", backend.calls)
    assert backend.calls == 1  # 只执行一次
    assert all(r.output == results[0].output for r in results)
    assert sum(1 for r in results if r.cached) == 4  # follower 标记为共享结果
    assert gw.stats.success == 5


async def test_cancel_releases_slot_and_skips_stats(make_gateway):
    gw = make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": slow_backend("b", latency_ms=200)},
        limits={"c1": 10},
        max_concurrent=1, max_queue=1,
    )
    t = asyncio.ensure_future(gw.infer(req()))
    await asyncio.sleep(0.02)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t
    log.info("input=cancelled judgement quota=%d stats=%s",
             gw._quota.usage("c1"), gw.stats)
    assert gw._quota.usage("c1") == 0       # 取消后退回配额
    assert gw.stats.cancelled == 1
    assert gw.stats.success == 0            # 不计入成功
    # 槽位已释放，后续请求可立即执行
    backend = gw._backends["b"]
    backend.latency_ms = 0
    r = await gw.infer(req())
    assert r.output and gw.stats.success == 1


async def test_cancelled_follower_does_not_kill_shared_execution(make_gateway):
    backend = slow_backend("b", latency_ms=80)
    gw = make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": backend},
        max_concurrent=2, max_queue=4,
    )
    t1 = asyncio.ensure_future(gw.infer(req(prompt="shared")))
    t2 = asyncio.ensure_future(gw.infer(req(prompt="shared")))
    await asyncio.sleep(0.02)
    t2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t2
    r1 = await t1  # leader 不受 follower 取消影响
    assert r1.output and backend.calls == 1
    log.info("input=follower-cancel judgement leader-ok calls=%d", backend.calls)
