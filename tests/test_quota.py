"""配额：按调用方独立限制，超额拒绝且原因可与后端错误区分，窗口滚动后恢复。"""

import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import AllBackendsFailedError, QuotaExceededError
from gateway.models import InferRequest

log = logging.getLogger("gateway.test")


def req(caller, prompt="p"):
    return InferRequest(model="m", caller_id=caller, prompt=prompt)


def mk(make_gateway, limits, default=None):
    return make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": DeterministicBackend("b")},
        limits=limits, default_limit=default,
    )


async def test_per_caller_quota_independent(make_gateway):
    gw = mk(make_gateway, {"a": 2, "b": 1})
    await gw.infer(req("a"))
    await gw.infer(req("a"))
    with pytest.raises(QuotaExceededError) as ei:
        await gw.infer(req("a"))
    log.info("input=caller:a#3 basis=limit:2 judgement=%s", ei.value.code)
    assert ei.value.code == "quota_exceeded"
    assert ei.value.http_status == 429
    # b 的配额独立，不受 a 超额影响
    r = await gw.infer(req("b"))
    assert r.output
    assert gw.stats.rejected == 1 and gw.stats.success == 3


async def test_quota_error_distinct_from_backend_error(make_gateway):
    gw = make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": DeterministicBackend("b", fault_plan=["error"])},
        limits={"a": 1},
    )
    with pytest.raises(AllBackendsFailedError) as e1:
        await gw.infer(req("a"))
    with pytest.raises(QuotaExceededError) as e2:
        await gw.infer(req("a"))
    log.info("judgement backend=%s quota=%s", e1.value.code, e2.value.code)
    assert e1.value.code != e2.value.code
    assert e1.value.http_status == 502 and e2.value.http_status == 429


async def test_window_rolls_over(make_gateway, clock):
    gw = mk(make_gateway, {"a": 1})
    await gw.infer(req("a"))
    with pytest.raises(QuotaExceededError):
        await gw.infer(req("a"))
    clock.advance(61)  # 窗口滚动
    r = await gw.infer(req("a"))
    log.info("input=after-window-roll judgement=accepted")
    assert r.output


async def test_default_limit_applies_to_unknown_callers(make_gateway):
    gw = mk(make_gateway, {}, default=1)
    await gw.infer(req("newbie"))
    with pytest.raises(QuotaExceededError):
        await gw.infer(req("newbie"))


async def test_no_limit_when_unconfigured(make_gateway):
    gw = mk(make_gateway, {})  # default_limit=None 表示不限
    for _ in range(50):
        await gw.infer(req("anyone"))
    assert gw.stats.success == 50
