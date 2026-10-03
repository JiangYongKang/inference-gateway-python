"""本地演示配置：构造默认注册表、后端与网关。"""

from __future__ import annotations

from .backends import DeterministicBackend
from .concurrency import ConcurrencyLimiter
from .gateway import InferenceGateway
from .governance import GovernanceConfig
from .quotas import QuotaLimiter
from .registry import ModelRegistry


def build_default_gateway() -> InferenceGateway:
    registry = ModelRegistry()
    # 别名 echo 下两个版本：v1 权重 80，v2 权重 20（灰度）
    registry.register_version("echo", "v1", backends=["primary", "backup"], weight=80)
    registry.register_version("echo", "v2", backends=["primary"], weight=20)
    # 调用方分组：内部调用方固定走 v2
    registry.set_caller_group("echo", "internal", "v2")

    backends = {
        "primary": DeterministicBackend("primary", latency_ms=1),
        "backup": DeterministicBackend("backup", latency_ms=1),
    }
    quota = QuotaLimiter({"internal": 1000}, default_limit=100, window_seconds=60.0)
    concurrency = ConcurrencyLimiter(max_concurrent=16, max_queue=64)
    # 治理：重放与复用均保留 5 分钟，按调用方隔离容量
    governance = GovernanceConfig(
        replay_ttl_seconds=300.0,
        replay_max_per_caller=128,
        reuse_ttl_seconds=300.0,
        reuse_max_per_caller=256,
    )
    return InferenceGateway(registry, backends, quota, concurrency, governance)
