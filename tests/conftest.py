import logging

import pytest

from gateway.backends import DeterministicBackend
from gateway.concurrency import ConcurrencyLimiter
from gateway.gateway import InferenceGateway
from gateway.governance import GovernanceConfig
from gateway.quotas import QuotaLimiter
from gateway.registry import ModelRegistry


class ManualClock:
    """可手动推进的确定性时钟。"""

    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    return ManualClock()


@pytest.fixture
def make_gateway(clock, caplog):
    """工厂：按测试需要组装网关，并注入日志记录路由依据。"""

    def _make(
        *,
        versions=None,
        caller_groups=None,
        backends=None,
        limits=None,
        default_limit=None,
        max_concurrent=4,
        max_queue=4,
        governance=None,
        governance_enabled=True,
    ):
        registry = ModelRegistry()
        for alias, version, backend_names, weight in versions or []:
            registry.register_version(alias, version, backends=backend_names, weight=weight)
        for alias, caller, version in caller_groups or []:
            registry.set_caller_group(alias, caller, version)
        quota = QuotaLimiter(limits or {}, default_limit=default_limit,
                             window_seconds=60.0, clock=clock)
        if governance is None and governance_enabled:
            governance = GovernanceConfig()
        gw = InferenceGateway(
            registry,
            backends or {},
            quota,
            ConcurrencyLimiter(max_concurrent=max_concurrent, max_queue=max_queue),
            governance,
            clock=clock,
        )
        return gw

    return _make


@pytest.fixture(autouse=True)
def _debug_logs(caplog):
    caplog.set_level(logging.DEBUG)
    return caplog


def log_judgement(record: str) -> None:
    """单测日志：打印请求输入、选择依据与最终判定。"""
    logging.getLogger("gateway.test").info("JUDGEMENT %s", record)
