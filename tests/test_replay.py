"""重放（调用标识幂等）：只执行一次、配置变化仍复用原结论、冲突、隔离、容量与过期。"""

import asyncio
import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import (
    AllBackendsFailedError,
    IdempotencyConflictError,
    IdempotencyStoreFullError,
    InvalidRetentionConfigError,
)
from gateway.models import InferRequest
from gateway.retention import RetentionConfig

log = logging.getLogger("gateway.test")

RET = dict(
    idempotency_ttl_seconds=300.0,
    idempotency_max_entries_per_caller=3,
    reuse_ttl_seconds=300.0,         # 与重放保留期一致，过期场景便于观察真实重执行
    reuse_max_entries_per_caller=100,
)


def req(rid, caller="c1", model="m", prompt="p", **kw):
    return InferRequest(model=model, caller_id=caller, prompt=prompt, request_id=rid, **kw)


def mk(make_gateway, backends=None, **kw):
    return make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends=backends or {"b": DeterministicBackend("b")},
        retention=RET,
        **kw,
    )


async def test_concurrent_same_id_executes_once(make_gateway):
    backend = DeterministicBackend("b", latency_ms=50)
    gw = mk(make_gateway, backends={"b": backend})
    results = await asyncio.gather(*[gw.infer(req("r1")) for _ in range(5)])
    log.info("input=5-concurrent-same-id basis=idempotency-claim judgement backend_calls=%d replayed=%d",
             backend.calls, sum(r.replayed for r in results))
    assert backend.calls == 1                       # 只真正执行一次
    assert sum(1 for r in results if r.replayed) == 4
    assert all(r.output == results[0].output for r in results)
    assert gw.stats.success == 1                    # 后端成功只计一次
    assert gw._quota.usage("c1") == 1               # 重放不重复扣额度


async def test_replay_survives_config_change(make_gateway, clock):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend})
    first = await gw.infer(req("r1"))
    assert first.version == "v1" and not first.replayed

    # 路由与分组调整：v1 摘除，调用方被分组钉到 v2
    gw._registry.register_version("m", "v2", ["b"], weight=100)
    gw._registry.remove_version("m", "v1")
    gw._registry.set_caller_group("m", "c1", "v2")

    replay = await gw.infer(req("r1"))
    log.info("input=replay-after-config-change basis=idempotency-promise judgement=%s",
             replay.model_dump())
    assert replay.replayed is True
    assert replay.version == "v1"                   # 仍返回当时生效的版本
    assert replay.output == first.output
    assert replay.attempts == first.attempts
    assert backend.calls == 1                       # 未触发新后端调用
    assert gw._quota.usage("c1") == 1               # 重放不扣额度
    assert gw.stats.success == 1                    # 不计入后端成功


async def test_same_id_different_content_conflicts(make_gateway):
    gw = mk(make_gateway)
    await gw.infer(req("r1", prompt="p1"))
    with pytest.raises(IdempotencyConflictError) as ei:
        await gw.infer(req("r1", prompt="p2"))
    log.info("input=same-id-diff-prompt judgement=%s", ei.value.code)
    assert ei.value.code == "idempotency_conflict"
    assert ei.value.http_status == 409              # 与 429 配额、502 后端可区分

    with pytest.raises(IdempotencyConflictError):   # 不同模型同样冲突
        await gw.infer(req("r1", model="other"))

    # 冲突不覆盖旧结论：原标识重放仍返回首次结论
    replay = await gw.infer(req("r1", prompt="p1"))
    assert replay.replayed and replay.output.endswith("") and replay.model == "m"
    assert gw._retention.stats.conflict_rejected == 2


async def test_id_scoped_per_caller(make_gateway):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend})
    r1 = await gw.infer(req("shared-id", caller="alice"))
    r2 = await gw.infer(req("shared-id", caller="bob"))
    log.info("input=same-id-two-callers basis=caller-isolation judgement calls=%d", backend.calls)
    assert not r1.replayed and not r2.replayed      # 互不可见，各自真实执行
    assert backend.calls == 2


async def test_failed_first_attempt_does_not_claim_id(make_gateway):
    backend = DeterministicBackend("b", fault_plan=["error"])
    gw = mk(make_gateway, backends={"b": backend})
    with pytest.raises(AllBackendsFailedError):
        await gw.infer(req("r1"))
    # 首次未成功不占标识：重试按当时路由重新执行并成功
    retry = await gw.infer(req("r1"))
    log.info("input=retry-after-failure basis=id-released judgement=%s", retry.model_dump())
    assert not retry.replayed and retry.output
    assert backend.calls == 2


async def test_replay_expires_after_retention(make_gateway, clock):
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend})
    await gw.infer(req("r1"))
    clock.advance(RET["idempotency_ttl_seconds"] + 1)
    again = await gw.infer(req("r1"))
    log.info("input=replay-after-ttl basis=expired judgement replayed=%s calls=%d",
             again.replayed, backend.calls)
    assert not again.replayed                       # 保留期外不再承诺一致性
    assert backend.calls == 2
    assert gw._retention.stats.replay_expired == 1


async def test_store_full_fast_reject_and_caller_isolated(make_gateway):
    gw = mk(make_gateway)
    for i in range(RET["idempotency_max_entries_per_caller"]):
        await gw.infer(req(f"r{i}", caller="greedy"))
    with pytest.raises(IdempotencyStoreFullError) as ei:
        await gw.infer(req("rX", caller="greedy"))
    log.info("input=4th-id basis=capacity:3/3 judgement=%s", ei.value.code)
    assert ei.value.code == "idempotency_store_full"
    assert ei.value.code not in ("quota_exceeded", "all_backends_failed")
    # 单个调用方打满不影响其他调用方
    ok = await gw.infer(req("rX", caller="other"))
    assert not ok.replayed
    assert gw._retention.stats.store_full_rejected == 1


async def test_store_full_reject_does_not_consume_quota(make_gateway):
    gw = mk(make_gateway, limits={"greedy": 10})
    for i in range(RET["idempotency_max_entries_per_caller"]):
        await gw.infer(req(f"r{i}", caller="greedy"))
    with pytest.raises(IdempotencyStoreFullError):
        await gw.infer(req("rX", caller="greedy"))
    assert gw._quota.usage("greedy") == 3           # 快速拒绝发生在配额准入之前


def test_invalid_retention_config_rejected_at_load():
    with pytest.raises(InvalidRetentionConfigError):
        RetentionConfig(idempotency_ttl_seconds=0)
    with pytest.raises(InvalidRetentionConfigError):
        RetentionConfig(reuse_ttl_seconds=-1)
    with pytest.raises(InvalidRetentionConfigError):
        RetentionConfig(idempotency_max_entries_per_caller=0)
    with pytest.raises(InvalidRetentionConfigError):
        RetentionConfig(reuse_max_entries_per_caller=0)
    log.info("input=invalid-configs judgement=invalid_retention_config x4")
