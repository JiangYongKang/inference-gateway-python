"""结果复用：命中语义、版本隔离、失败不入缓存、过期/淘汰、配额口径、定向清理。"""

import asyncio
import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import AllBackendsFailedError, QuotaExceededError
from gateway.models import InferRequest

log = logging.getLogger("gateway.test")

RET = dict(
    idempotency_ttl_seconds=10_000.0,
    idempotency_max_entries_per_caller=100,
    reuse_ttl_seconds=60.0,
    reuse_max_entries_per_caller=2,
)


def req(prompt="p", caller="c1", model="m", rid=None, **kw):
    return InferRequest(model=model, caller_id=caller, prompt=prompt, request_id=rid, **kw)


def mk(make_gateway, backends=None, versions=None, **kw):
    return make_gateway(
        versions=versions or [("m", "v1", ["b"], 1)],
        backends=backends or {"b": DeterministicBackend("b")},
        retention=RET,
        **kw,
    )


async def test_cross_request_reuse_hit(make_gateway):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend})
    first = await gw.infer(req(rid="r1"))
    second = await gw.infer(req(rid="r2"))          # 不同调用标识、同内容
    log.info("input=2nd-call-same-content basis=reuse-key judgement=%s", second.model_dump())
    assert not first.reused and second.reused is True
    assert second.version == first.version and second.backend == first.backend
    assert second.output == first.output
    assert second.fallback_from == first.fallback_from  # 保留最初结论的回退信息
    assert backend.calls == 1                       # 本次未重新执行后端
    assert gw.stats.success == 1                    # 复用不算新的后端成功
    assert gw._retention.stats.reuse_hits == 1
    assert gw._quota.usage("c1") == 2               # 复用仍按配额规则正常准入


async def test_reuse_not_shared_across_caller_or_model(make_gateway):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend},
            versions=[("m", "v1", ["b"], 1), ("m2", "v1", ["b"], 1)])
    await gw.infer(req(caller="alice"))
    other_caller = await gw.infer(req(caller="bob"))
    other_model = await gw.infer(req(caller="alice", model="m2"))
    log.info("input=cross-caller/model basis=reuse-key-isolation judgement calls=%d", backend.calls)
    assert not other_caller.reused and not other_model.reused
    assert backend.calls == 3


async def test_version_change_does_not_reuse_old_version(make_gateway):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend})
    first = await gw.infer(req())
    assert first.version == "v1"
    # 灰度/分组变化：调用方被钉到 v2，新请求必须按新路由执行
    gw._registry.register_version("m", "v2", ["b"], weight=100)
    gw._registry.set_caller_group("m", "c1", "v2")
    second = await gw.infer(req())
    log.info("input=after-version-pin basis=new-route judgement=%s", second.model_dump())
    assert not second.reused                        # 不能拿 v1 结果冒充 v2
    assert second.version == "v2" and second.output != first.output
    assert backend.calls == 2


async def test_failures_never_enter_reuse(make_gateway):
    backend = DeterministicBackend("b", fault_plan=["error", "timeout"])
    gw = mk(make_gateway, backends={"b": backend})
    with pytest.raises(AllBackendsFailedError):     # error
        await gw.infer(req(prompt="x"))
    with pytest.raises(AllBackendsFailedError):     # timeout
        await gw.infer(req(prompt="x", timeout_ms=50))
    ok = await gw.infer(req(prompt="x"))
    log.info("input=after-2-failures basis=no-reuse-entry judgement calls=%d", backend.calls)
    assert not ok.reused and backend.calls == 3     # 两次失败均未进入复用


async def test_cancelled_request_never_enters_reuse(make_gateway):
    backend = DeterministicBackend("b", latency_ms=200)
    gw = mk(make_gateway, backends={"b": backend})
    task = asyncio.ensure_future(gw.infer(req(prompt="slow")))
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    again = await gw.infer(req(prompt="slow"))
    log.info("input=after-cancel basis=no-reuse-entry judgement reused=%s", again.reused)
    assert not again.reused                         # 取消的结论不得进入复用
    assert backend.calls == 2


async def test_reuse_expires_and_counts(make_gateway, clock):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend})
    await gw.infer(req())
    clock.advance(RET["reuse_ttl_seconds"] + 1)
    again = await gw.infer(req())
    log.info("input=after-reuse-ttl basis=expired judgement reused=%s calls=%d",
             again.reused, backend.calls)
    assert not again.reused and backend.calls == 2  # 过期按未命中处理
    assert gw._retention.stats.reuse_expired == 1


async def test_reuse_capacity_eviction_deterministic(make_gateway):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend})
    await gw.infer(req(prompt="p1"))
    await gw.infer(req(prompt="p2"))
    await gw.infer(req(prompt="p3"))                # 容量=2，挤出最久未访问的 p1
    assert gw._retention.stats.reuse_evicted == 1
    hit_p3 = await gw.infer(req(prompt="p3"))
    miss_p1 = await gw.infer(req(prompt="p1"))
    log.info("input=capacity-eviction basis=LRU judgement p3.reused=%s p1.reused=%s",
             hit_p3.reused, miss_p1.reused)
    assert hit_p3.reused is True                    # 新条目仍在
    assert not miss_p1.reused                       # p1 已被淘汰，重新真实执行
    # 淘汰只影响复用记录：已承诺的调用标识结论不受容量挤压
    rid_first = await gw.infer(req(prompt="keep", rid="keep-1"))
    for i in range(5):
        await gw.infer(req(prompt=f"filler-{i}"))
    replay = await gw.infer(req(prompt="keep", rid="keep-1"))
    assert replay.replayed and replay.output == rid_first.output


async def test_reuse_hit_still_needs_quota(make_gateway):
    gw = mk(make_gateway, limits={"c1": 1})
    await gw.infer(req())                           # 用掉唯一配额
    with pytest.raises(QuotaExceededError) as ei:
        await gw.infer(req())                       # 本可复用，但准入仍按配额规则
    log.info("input=reuse-without-quota judgement=%s", ei.value.code)
    assert ei.value.code == "quota_exceeded"
    assert gw._retention.stats.reuse_hits == 0


async def test_purge_and_shorten_ttl(make_gateway, clock):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend},
            versions=[("m", "v1", ["b"], 1), ("m", "v2", ["b"], 0)])
    await gw.infer(req(rid="r1"))                   # 写入 (m, v1, p) 复用记录
    removed = gw.purge_reuse(model="m", version="v1")
    log.info("input=purge(m,v1) judgement removed=%d", removed)
    assert removed == 1
    again = await gw.infer(req())
    assert not again.reused                         # 清理后新请求重新真实执行

    # 定向清理不改写保留期内已承诺的调用标识结论
    replay = await gw.infer(req(rid="r1"))
    assert replay.replayed and replay.version == "v1"

    # 缩短保留期：旧记录立即按新窗口过期
    await gw.infer(req(prompt="q"))
    gw.shorten_reuse_ttl(5.0)
    clock.advance(6.0)
    stale = await gw.infer(req(prompt="q"))
    assert not stale.reused
    with pytest.raises(Exception) as ei:
        gw.shorten_reuse_ttl(0)
    assert getattr(ei.value, "code", None) == "invalid_retention_config"


async def test_retention_stats_visible(make_gateway, clock):
    gw = mk(make_gateway)
    await gw.infer(req(rid="r1"))
    await gw.infer(req(rid="r1"))                   # 重放命中
    await gw.infer(req(rid="r2"))                   # 复用命中
    snap = gw.retention_stats()
    log.info("input=stats judgement=%s", snap)
    assert snap["counters"]["replay_hits"] == 1
    assert snap["counters"]["reuse_hits"] == 1
    assert snap["records"]["idempotency"] == {"c1": 2}  # r1 真实执行 + r2 复用成功均落标识
    assert snap["records"]["reuse"] == {"c1": 1}
    assert snap["policy"]["reuse_ttl_seconds"] == RET["reuse_ttl_seconds"]
