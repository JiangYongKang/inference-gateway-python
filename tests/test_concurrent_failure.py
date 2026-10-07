"""并发同一调用标识 + 后端失败：整批只真正执行一次、共享同一份失败、标识可重试。

覆盖：
- 后端固定报错：并发同标识整批共享同一份失败，不各自重新执行、不重复扣额度；
- 报错与成功混合（第一次调用报错、之后成功）：同批结论不分叉，
  失败不占标识，之后同标识重试按当时路由重新执行并成功；
- 后端超时：与报错同口径（整批一次执行、共享失败）。
"""

import asyncio
import logging

from gateway.backends import DeterministicBackend
from gateway.errors import AllBackendsFailedError, BackendExecutionError
from gateway.models import InferRequest

log = logging.getLogger("gateway.test")

RET = dict(
    idempotency_ttl_seconds=300.0,
    idempotency_max_entries_per_caller=16,
    reuse_ttl_seconds=300.0,
    reuse_max_entries_per_caller=100,
)


def req(rid, caller="c1", model="m", prompt="p", **kw):
    return InferRequest(model=model, caller_id=caller, prompt=prompt, request_id=rid, **kw)


class GatedBackend:
    """测试用后端：进入时置位 entered，阻塞在 release 上，按 outcomes 序列出结果。

    outcomes 按调用次序弹出："error" 抛执行错误，"ok" 返回确定性输出。
    测试借此确定性地让一整批并发请求都挂上同一标识的等待之后再放出结果，
    避免依赖时序运气。
    """

    def __init__(self, name, outcomes):
        self.name = name
        self.calls = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self._outcomes = list(outcomes)

    async def infer(self, prompt, *, version, timeout_ms):
        self.calls += 1
        outcome = self._outcomes.pop(0) if self._outcomes else "ok"
        self.entered.set()
        await self.release.wait()
        if outcome == "error":
            raise BackendExecutionError(
                f"backend {self.name} injected execution error",
                detail={"backend": self.name},
            )
        return f"{self.name}:{version}:{prompt}"


def mk(make_gateway, backend, **kw):
    return make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": backend},
        retention=RET,
        **kw,
    )


async def _settle(rounds=10):
    """让出事件循环若干轮，确保 follower 都挂上同一标识的等待。"""
    for _ in range(rounds):
        await asyncio.sleep(0)


async def test_concurrent_same_id_backend_error_shared_once(make_gateway):
    backend = GatedBackend("b", ["error"])
    gw = mk(make_gateway, backend)

    tasks = [asyncio.create_task(gw.infer(req("r1"))) for _ in range(5)]
    await backend.entered.wait()
    await _settle()
    backend.release.set()
    results = await asyncio.gather(*tasks, return_exceptions=True)

    log.info(
        "input=5-concurrent-same-id backend=error path=real-execution(shared-failure) "
        "judgement calls=%d failed=%d success=%d quota=%d idem_records=%s",
        backend.calls, gw.stats.failed, gw.stats.success,
        gw._quota.usage("c1"), gw._retention.record_counts()["idempotency"],
    )
    assert backend.calls == 1                                  # 整批只真正执行一次
    assert all(isinstance(r, AllBackendsFailedError) for r in results)  # 结论不分叉
    assert len({id(r) for r in results}) == 1                  # 同一份失败结论
    assert gw.stats.failed == 1 and gw.stats.success == 0      # 统计口径=真实执行次数
    assert gw._quota.usage("c1") == 1                          # 整批只扣一次额度
    assert gw._retention.stats.replay_hits == 0                # 共享失败不算重放命中
    assert not gw._retention.record_counts()["idempotency"]    # 失败不占标识

    # 标识已释放：之后同标识重试按当时路由重新执行（outcomes 已空 -> ok）
    retry = await gw.infer(req("r1"))
    log.info("input=retry-same-id-after-failure path=real-execution judgement=%s",
             retry.model_dump())
    assert not retry.replayed and retry.output == "b:v1:p"
    assert backend.calls == 2
    assert gw._quota.usage("c1") == 2
    assert gw.stats.success == 1 and gw.stats.failed == 1


async def test_concurrent_mixed_error_then_success_no_divergence(make_gateway):
    # 后端第一次调用报错、之后成功：同批并发不得一部分失败一部分成功
    backend = GatedBackend("b", ["error", "ok"])
    gw = mk(make_gateway, backend)

    tasks = [asyncio.create_task(gw.infer(req("r1"))) for _ in range(5)]
    await backend.entered.wait()
    await _settle()
    backend.release.set()
    batch = await asyncio.gather(*tasks, return_exceptions=True)

    log.info(
        "input=5-concurrent-same-id backend=flaky(error,ok) path=real-execution "
        "judgement calls=%d outcomes=%s",
        backend.calls, [getattr(r, "code", type(r).__name__) for r in batch],
    )
    assert backend.calls == 1                                  # 重试不会发生在这批里
    assert all(isinstance(r, AllBackendsFailedError) for r in batch)  # 整批同一份失败
    assert gw.stats.failed == 1 and gw.stats.success == 0
    assert gw._quota.usage("c1") == 1

    # 之后同标识重试：按当时路由重新执行并成功
    retry = await gw.infer(req("r1"))
    assert not retry.replayed and retry.output == "b:v1:p"
    assert backend.calls == 2

    # 首次成功后的重放：返回当时那一份结论，不再触发后端调用
    replay = await gw.infer(req("r1"))
    log.info("input=replay-after-retry-success path=replay judgement=%s",
             replay.model_dump())
    assert replay.replayed and replay.output == retry.output
    assert replay.version == retry.version and replay.backend == retry.backend
    assert backend.calls == 2
    assert gw._quota.usage("c1") == 2                          # 重放不扣额度
    assert gw.stats.success == 1 and gw.stats.failed == 1      # 口径不变
    assert gw._retention.stats.replay_hits == 1


async def test_concurrent_same_id_backend_timeout_shared_once(make_gateway):
    backend = DeterministicBackend("b", fault_plan=["timeout"])
    gw = mk(make_gateway, backend)

    tasks = [
        asyncio.create_task(gw.infer(req("r1", timeout_ms=50)))
        for _ in range(5)
    ]
    while backend.calls == 0:                                  # 等 leader 进入后端
        await asyncio.sleep(0)
    await _settle()                                            # follower 挂上等待
    results = await asyncio.gather(*tasks, return_exceptions=True)

    log.info(
        "input=5-concurrent-same-id backend=timeout path=real-execution(shared-failure) "
        "judgement calls=%d failed=%d quota=%d",
        backend.calls, gw.stats.failed, gw._quota.usage("c1"),
    )
    assert backend.calls == 1
    assert all(isinstance(r, AllBackendsFailedError) for r in results)
    assert all(r.detail["last_error"] == "backend_timeout" for r in results)
    assert gw.stats.failed == 1 and gw.stats.success == 0
    assert gw._quota.usage("c1") == 1
    assert not gw._retention.record_counts()["idempotency"]    # 超时不占标识

    retry = await gw.infer(req("r1", timeout_ms=50))           # 标识已释放可重试
    assert not retry.replayed and retry.output
    assert backend.calls == 2
