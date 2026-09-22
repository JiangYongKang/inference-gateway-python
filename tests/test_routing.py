"""路由与版本切换：别名解析、灰度粘性、调用方分组、非法配置的可区分拒绝。"""

import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.errors import (
    InvalidRouteConfigError,
    NoAvailableBackendError,
    UnknownModelError,
)
from gateway.models import InferRequest
from gateway.registry import ModelRegistry

log = logging.getLogger("gateway.test")


def req(alias="echo", caller="c1", prompt="p", **kw):
    return InferRequest(model=alias, caller_id=caller, prompt=prompt, **kw)


class TestRegistryValidation:
    def test_unknown_alias_rejected(self):
        reg = ModelRegistry()
        with pytest.raises(UnknownModelError) as ei:
            reg.resolve("ghost", "c1")
        log.info("input=resolve(ghost) basis=no-such-alias judgement=%s", ei.value.code)
        assert ei.value.code == "unknown_model"

    def test_negative_weight_rejected(self):
        reg = ModelRegistry()
        with pytest.raises(InvalidRouteConfigError):
            reg.register_version("m", "v1", ["b"], weight=-1)

    def test_empty_backends_rejected(self):
        reg = ModelRegistry()
        with pytest.raises(InvalidRouteConfigError):
            reg.register_version("m", "v1", [], weight=1)

    def test_zero_total_weight_rejected(self):
        reg = ModelRegistry()
        reg.register_version("m", "v1", ["b"], weight=0)
        reg.register_version("m", "v2", ["b"], weight=0)
        with pytest.raises(InvalidRouteConfigError) as ei:
            reg.resolve("m", "c1")
        log.info("input=resolve(zero-weights) judgement=%s", ei.value.code)

    def test_caller_group_unknown_version_rejected(self):
        reg = ModelRegistry()
        reg.register_version("m", "v1", ["b"], weight=1)
        with pytest.raises(InvalidRouteConfigError):
            reg.set_caller_group("m", "c1", "vX")


class TestRouting:
    def test_caller_group_beats_canary(self):
        reg = ModelRegistry()
        reg.register_version("m", "v1", ["b"], weight=100)
        reg.register_version("m", "v2", ["b"], weight=0)
        reg.set_caller_group("m", "vip", "v2")
        d = reg.resolve("m", "vip")
        log.info("input=caller:vip basis=caller_group judgement=%s", d.version)
        assert (d.version, d.reason) == ("v2", "caller_group")

    def test_canary_is_sticky_per_caller(self):
        reg = ModelRegistry()
        reg.register_version("m", "v1", ["b"], weight=50)
        reg.register_version("m", "v2", ["b"], weight=50)
        seen = {}
        for i in range(50):
            caller = f"caller-{i}"
            first = reg.resolve("m", caller)
            for _ in range(5):
                again = reg.resolve("m", caller)
                assert again.version == first.version  # 同一调用方结果一致
            seen[first.version] = seen.get(first.version, 0) + 1
        log.info("input=50-callers basis=sticky-hash judgement=%s", seen)
        assert set(seen) == {"v1", "v2"}  # 两个版本都分到流量

    def test_version_switch_keeps_caller_consistent(self):
        reg = ModelRegistry()
        reg.register_version("m", "v1", ["b"], weight=100)
        before = {f"c{i}": reg.resolve("m", f"c{i}").version for i in range(20)}
        assert set(before.values()) == {"v1"}
        # 灰度开始：加入 v2，已哈希到 v1 的调用方只要不越界就不应全部翻转
        reg.register_version("m", "v2", ["b"], weight=100)
        after = {f"c{i}": reg.resolve("m", f"c{i}").version for i in range(20)}
        stayed = sum(1 for c in before if after[c] == before[c])
        log.info("input=version-switch basis=sticky-bucket judgement stayed=%d/20", stayed)
        assert stayed > 0  # 确定性：同一调用标识不会得到互相矛盾的结果序列
        for c in before:
            assert reg.resolve("m", c).version == after[c]  # 配置不变则结论不变

    def test_remove_version_falls_back(self):
        reg = ModelRegistry()
        reg.register_version("m", "v1", ["b"], weight=0)
        reg.register_version("m", "v2", ["b"], weight=100)
        reg.set_caller_group("m", "vip", "v2")
        reg.remove_version("m", "v2")
        d = reg.resolve("m", "vip")
        log.info("input=removed-v2 basis=group-cleared judgement=%s", d.version)
        assert d.version == "v1"


class TestGatewayRouting:
    async def test_unregistered_backend_rejected(self, make_gateway):
        gw = make_gateway(versions=[("m", "v1", ["ghost"], 1)], backends={})
        with pytest.raises(NoAvailableBackendError) as ei:
            await gw.infer(req("m"))
        log.info("input=unregistered-backend judgement=%s", ei.value.code)
        assert ei.value.code == "no_available_backend"

    async def test_response_includes_version_and_backend(self, make_gateway):
        gw = make_gateway(
            versions=[("m", "v1", ["b1"], 1)],
            backends={"b1": DeterministicBackend("b1")},
        )
        r = await gw.infer(req("m", request_id="r1"))
        log.info("input=%s basis=single_version judgement=%s", req("m").model_dump(), r.model_dump())
        assert r.version == "v1" and r.backend == "b1" and r.request_id == "r1"
        # 旧客户端兼容：响应含全部既有字段且可重复解析
        assert r.model == "m" and r.output.startswith("b1:v1:")
