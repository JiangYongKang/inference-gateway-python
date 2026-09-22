"""后端超时/异常时按回退次序处理：不重复计费、不重复计数，结果说明实际生效后端。"""

import asyncio
import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import AllBackendsFailedError, BackendTimeoutError
from gateway.models import InferRequest

log = logging.getLogger("gateway.test")


def req(prompt="p", caller="c1", **kw):
    return InferRequest(model="m", caller_id=caller, prompt=prompt, **kw)


VERSIONS = [("m", "v1", ["primary", "backup"], 1)]


async def test_error_falls_back_to_next_backend(make_gateway):
    primary = DeterministicBackend("primary", fault_plan=["error"])
    backup = DeterministicBackend("backup")
    gw = make_gateway(versions=VERSIONS, backends={"primary": primary, "backup": backup},
                      limits={"c1": 10})
    r = await gw.infer(req())
    log.info("input=%s basis=primary:error judgement=%s", req().model_dump(), r.model_dump())
    assert r.backend == "backup"
    assert r.fallback_from == ["primary"]
    assert r.attempts == 2
    assert r.output.startswith("backup:v1:")
    # 回退不重复计费、不重复计数
    assert gw._quota.usage("c1") == 1
    assert gw.stats.success == 1
    assert gw.stats.per_backend == {"backup": 1}


async def test_timeout_falls_back(make_gateway):
    primary = DeterministicBackend("primary", fault_plan=["timeout"])
    backup = DeterministicBackend("backup")
    gw = make_gateway(versions=VERSIONS, backends={"primary": primary, "backup": backup})
    r = await gw.infer(req(timeout_ms=30))
    log.info("input=timeout_ms:30 basis=primary:timeout judgement=%s", r.model_dump())
    assert r.backend == "backup" and r.fallback_from == ["primary"]


async def test_all_backends_failed_is_distinct(make_gateway):
    primary = DeterministicBackend("primary", fault_plan=["error"])
    backup = DeterministicBackend("backup", fault_plan=["timeout"])
    gw = make_gateway(versions=VERSIONS, backends={"primary": primary, "backup": backup})
    with pytest.raises(AllBackendsFailedError) as ei:
        await gw.infer(req(timeout_ms=20))
    log.info("input=all-faulty judgement=%s detail=%s", ei.value.code, ei.value.detail)
    assert ei.value.code == "all_backends_failed"
    assert ei.value.detail["tried"] == ["primary", "backup"]
    assert ei.value.detail["last_error"] == "backend_timeout"
    assert gw.stats.failed == 1 and gw.stats.success == 0


async def test_retry_same_request_no_duplicate_effects(make_gateway):
    """同一 request_id 重试（首次后端全部失败后）：每次准入计一次，成功只计一次。"""
    primary = DeterministicBackend("primary", fault_plan=["error", "ok"])
    gw = make_gateway(versions=[("m", "v1", ["primary"], 1)],
                      backends={"primary": primary}, limits={"c1": 10})
    with pytest.raises(AllBackendsFailedError):
        await gw.infer(req(request_id="idem-1"))
    r = await gw.infer(req(request_id="idem-1"))
    log.info("input=retry idem-1 judgement=%s", r.model_dump())
    assert gw._quota.usage("c1") == 2  # 两次独立准入各计一次
    assert gw.stats.failed == 1 and gw.stats.success == 1
    assert primary.calls == 2


async def test_fallback_result_deterministic(make_gateway):
    primary = DeterministicBackend("primary", fault_plan=["error"])
    backup = DeterministicBackend("backup")
    gw = make_gateway(versions=VERSIONS, backends={"primary": primary, "backup": backup})
    r1 = await gw.infer(req())
    primary2 = DeterministicBackend("primary", fault_plan=["error"])
    gw2 = make_gateway(versions=VERSIONS, backends={"primary": primary2, "backup": backup})
    r2 = await gw2.infer(req())
    assert r1.output == r2.output  # 相同输入 + 相同故障脚本 => 相同结论
