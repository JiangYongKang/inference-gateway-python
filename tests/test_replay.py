"""重放（调用标识幂等）：只执行一次、配置变化仍复用原结论、冲突、隔离、容量与过期。"""

import asyncio
import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import (
    AllBackendsFailedError,
    BackendExecutionError,
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


class BlockingBackend:
    """确定性阻塞后端：调用进入后在 gate 上等待，直到测试决定放行或注入故障。

    用于稳定复现"一批并发同标识同时在途"的竞态：先确认所有请求都已进入后端，
    再让这次真实执行失败，避免依赖 sleep 时序。
    """

    def __init__(self, name: str, *, fail: bool = False) -> None:
        self.name = name
        self.calls = 0
        self._fail = fail
        self._entered = asyncio.Event()
        self._gate = asyncio.Event()

    async def wait_entered(self) -> None:
        await self._entered.wait()

    def release_ok(self) -> None:
        self._fail = False
        self._gate.set()

    def release_fail(self) -> None:
        self._fail = True
        self._gate.set()

    async def infer(self, prompt: str, *, version: str, timeout_ms: int | None) -> str:
        self.calls += 1
        self._entered.set()
        await self._gate.wait()
        if self._fail:
            raise BackendExecutionError(
                f"backend {self.name} injected execution error",
                detail={"backend": self.name},
            )
        return f"{self.name}:{version}:blocking-ok:{prompt}"


def _codes(outcomes) -> list[str | None]:
    return [getattr(o, "code", None) if isinstance(o, BaseException) else None for o in outcomes]


def _ok_count(outcomes) -> int:
    return sum(1 for o in outcomes if not isinstance(o, BaseException))


async def test_concurrent_same_id_single_backend_failure_shared(make_gateway):
    """后端固定报错：一批并发同标识只真正执行一次，全批拿到同一个失败，只扣一次额度。"""
    backend = DeterministicBackend("b", latency_ms=30, fault_plan=["error"])
    gw = mk(make_gateway, backends={"b": backend}, limits={"c1": 100})
    outcomes = await asyncio.gather(
        *[gw.infer(req("r1")) for _ in range(6)], return_exceptions=True)
    log.info(
        "input=6-concurrent-same-id-all-error basis=idempotency-single-lead "
        "judgement codes=%s backend_calls=%d quota=%d stats(success=%d failed=%d rejected=%d)",
        _codes(outcomes), backend.calls, gw._quota.usage("c1"),
        gw.stats.success, gw.stats.failed, gw.stats.rejected,
    )
    assert backend.calls == 1                          # 整批只真正执行一次
    assert _codes(outcomes) == ["all_backends_failed"] * 6  # 结论不分叉
    assert gw._quota.usage("c1") == 1                 # 只扣一次额度
    assert gw.stats.failed == 1 and gw.stats.success == 0
    assert gw.stats.rejected == 0
    assert gw._retention.stats.replay_hits == 0       # 失败共享不计重放命中

    # 失败不占标识：之后拿同一标识重试，按当时路由重新执行并成功
    retry = await gw.infer(req("r1"))
    log.info("input=retry-after-concurrent-failure basis=id-released judgement=%s",
             retry.model_dump())
    assert not retry.replayed and retry.output
    assert backend.calls == 2
    assert gw.stats.success == 1


async def test_concurrent_same_id_failure_then_success_mixed(make_gateway):
    """报错与成功混合：同标识首批并发全失败（一次执行）；标识释放后下一批并发全成功，
    两批之间不出现一部分失败、一部分成功的分叉。"""
    backend = DeterministicBackend("b", latency_ms=30, fault_plan=["error"])
    gw = mk(make_gateway, backends={"b": backend}, limits={"c1": 100})

    batch_fail = await asyncio.gather(
        *[gw.infer(req("r1")) for _ in range(4)], return_exceptions=True)
    assert _codes(batch_fail) == ["all_backends_failed"] * 4
    assert backend.calls == 1 and gw.stats.failed == 1

    # fault_plan 已耗尽，之后调用成功；第二批并发同标识只执行一次，其余重放
    batch_ok = await asyncio.gather(
        *[gw.infer(req("r1")) for _ in range(4)], return_exceptions=True)
    log.info(
        "input=fail-batch-then-success-batch basis=release-and-reclaim "
        "judgement fail_codes=%s ok=%d replayed=%d backend_calls=%d quota=%d "
        "stats(success=%d failed=%d)",
        _codes(batch_fail), _ok_count(batch_ok),
        sum(getattr(o, "replayed", False) for o in batch_ok if not isinstance(o, BaseException)),
        backend.calls, gw._quota.usage("c1"), gw.stats.success, gw.stats.failed,
    )
    assert _ok_count(batch_ok) == 4                   # 无分叉：4 个全部成功
    assert all(not isinstance(o, BaseException) and o.output == batch_ok[0].output
               for o in batch_ok)
    assert sum(o.replayed for o in batch_ok) == 3
    assert backend.calls == 2                         # 失败一次 + 成功一次
    assert gw._quota.usage("c1") == 2                 # 每批真实执行只扣一次
    assert gw.stats.success == 1 and gw.stats.failed == 1
    assert gw._retention.stats.replay_hits == 3


async def test_concurrent_same_id_failure_blocked_at_backend(make_gateway):
    """用显式门闩保证 5 个请求都已在途后再让后端失败：整批共享失败、只执行一次。"""
    backend = BlockingBackend("b", fail=True)
    gw = mk(make_gateway, backends={"b": backend}, limits={"c1": 100})
    tasks = [asyncio.ensure_future(gw.infer(req("r1"))) for _ in range(5)]
    await backend.wait_entered()                      # 唯一一次真实执行已进入后端
    await asyncio.sleep(0)                            # 让其余 follower 挂到在途记录上
    backend.release_fail()
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    log.info(
        "input=5-concurrent-gated-failure basis=single-inflight-lead "
        "judgement codes=%s backend_calls=%d quota=%d",
        _codes(outcomes), backend.calls, gw._quota.usage("c1"),
    )
    assert backend.calls == 1
    assert _codes(outcomes) == ["all_backends_failed"] * 5
    assert gw._quota.usage("c1") == 1 and gw.stats.failed == 1

    # 放行成功后，同标识重试应重新真实执行（失败未占位）
    backend.release_ok()
    retry = await gw.infer(req("r1"))
    assert not retry.replayed and retry.output.endswith("p")
    assert backend.calls == 2


async def test_concurrent_same_id_timeout_failure_shared(make_gateway):
    """后端超时同样属于终态失败：一批并发同标识只真实执行/超时一次并共享失败。"""
    backend = DeterministicBackend("b", latency_ms=0, fault_plan=["timeout"])
    gw = mk(make_gateway, backends={"b": backend}, limits={"c1": 100})

    async def one():
        return await gw.infer(req("r1", timeout_ms=30))

    outcomes = await asyncio.gather(*[one() for _ in range(4)], return_exceptions=True)
    codes = _codes(outcomes)
    log.info("input=4-concurrent-same-id-timeout judgement codes=%s backend_calls=%d quota=%d",
             codes, backend.calls, gw._quota.usage("c1"))
    assert codes == ["all_backends_failed"] * 4
    assert backend.calls == 1
    assert gw._quota.usage("c1") == 1 and gw.stats.failed == 1


async def test_concurrent_same_id_quota_reject_is_shared_and_distinct(make_gateway):
    """额度不足在并发同标识下也整批一致拒绝（不分叉、只计一次），错误码与后端失败可区分，
    且不触发任何后端调用。"""
    backend = DeterministicBackend("b")
    gw = mk(make_gateway, backends={"b": backend}, limits={"c1": 1})
    await gw.infer(req("other", prompt="seed"))       # 用掉唯一额度
    outcomes = await asyncio.gather(
        *[gw.infer(req("r1")) for _ in range(4)], return_exceptions=True)
    log.info("input=4-concurrent-same-id-no-quota judgement codes=%s backend_calls=%d",
             _codes(outcomes), backend.calls)
    assert _codes(outcomes) == ["quota_exceeded"] * 4
    assert backend.calls == 1                         # 额度拒绝不调后端
    assert gw.stats.rejected == 1 and gw.stats.failed == 0
    assert gw._retention.stats.replay_hits == 0

async def test_concurrent_same_id_failure_does_not_occupy_capacity(make_gateway):
    """并发失败不写入标识记录：容量上限内，失败批次之后仍能用新标识/同标识成功。"""
    backend = DeterministicBackend("b", latency_ms=20, fault_plan=["error"])
    gw = mk(make_gateway, backends={"b": backend})    # 容量=3（见 RET）
    outcomes = await asyncio.gather(
        *[gw.infer(req("r1")) for _ in range(3)], return_exceptions=True)
    assert all(isinstance(o, AllBackendsFailedError) for o in outcomes)
    assert gw.retention_stats()["records"]["idempotency"] == {}
    # 容量仍是满的可用空间：连续 3 个新标识成功且不触发 store_full
    for i in range(RET["idempotency_max_entries_per_caller"]):
        r = await gw.infer(req(f"ok-{i}"))
        assert not r.replayed
    log.info("input=fail-then-fill-capacity judgement calls=%d records=%s",
             backend.calls, gw.retention_stats()["records"])
    assert gw._retention.stats.store_full_rejected == 0


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
