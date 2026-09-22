"""模型注册表：别名 -> 版本，灰度权重与调用方分组路由。

路由优先级：调用方分组 > 灰度权重（按 caller_id 粘性哈希）> 单版本直通。
粘性哈希保证配置不变时同一调用方始终落在同一版本。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from .errors import InvalidRouteConfigError, NoAvailableBackendError, UnknownModelError


@dataclass(frozen=True)
class ModelVersion:
    version: str
    backends: tuple[str, ...]  # 该版本可用的后端名，按回退次序排列
    weight: int = 0            # 灰度权重（非负整数）


@dataclass
class AliasConfig:
    alias: str
    versions: dict[str, ModelVersion] = field(default_factory=dict)
    caller_groups: dict[str, str] = field(default_factory=dict)  # caller_id -> 固定版本


@dataclass(frozen=True)
class RouteDecision:
    alias: str
    version: str
    backends: tuple[str, ...]
    reason: str  # "caller_group" | "canary_weight" | "single_version"


class ModelRegistry:
    def __init__(self) -> None:
        self._aliases: dict[str, AliasConfig] = {}

    def _alias_config(self, alias: str) -> AliasConfig:
        cfg = self._aliases.get(alias)
        if cfg is None:
            raise UnknownModelError(f"model alias not registered: {alias!r}", detail={"alias": alias})
        return cfg

    def register_version(
        self,
        alias: str,
        version: str,
        backends: list[str],
        weight: int = 0,
    ) -> None:
        if weight < 0:
            raise InvalidRouteConfigError(
                f"negative weight {weight} for {alias}/{version}",
                detail={"alias": alias, "version": version, "weight": weight},
            )
        if not backends:
            raise InvalidRouteConfigError(
                f"version {alias}/{version} must declare at least one backend",
                detail={"alias": alias, "version": version},
            )
        cfg = self._aliases.setdefault(alias, AliasConfig(alias=alias))
        cfg.versions[version] = ModelVersion(version=version, backends=tuple(backends), weight=weight)

    def set_caller_group(self, alias: str, caller_id: str, version: str) -> None:
        cfg = self._alias_config(alias)
        if version not in cfg.versions:
            raise InvalidRouteConfigError(
                f"caller group pins unknown version {version!r} for alias {alias!r}",
                detail={"alias": alias, "caller_id": caller_id, "version": version},
            )
        cfg.caller_groups[caller_id] = version

    def remove_version(self, alias: str, version: str) -> None:
        cfg = self._alias_config(alias)
        cfg.versions.pop(version, None)
        stale = [c for c, v in cfg.caller_groups.items() if v == version]
        for c in stale:
            del cfg.caller_groups[c]

    def resolve(self, alias: str, caller_id: str) -> RouteDecision:
        cfg = self._alias_config(alias)

        pinned = cfg.caller_groups.get(caller_id)
        if pinned is not None:
            mv = cfg.versions.get(pinned)
            if mv is None:
                raise InvalidRouteConfigError(
                    f"caller group pins removed version {pinned!r}",
                    detail={"alias": alias, "caller_id": caller_id, "version": pinned},
                )
            return RouteDecision(alias, mv.version, mv.backends, "caller_group")

        usable = [mv for mv in cfg.versions.values() if mv.backends]
        if not usable:
            raise NoAvailableBackendError(
                f"alias {alias!r} has no version with backends",
                detail={"alias": alias},
            )
        if len(usable) == 1:
            mv = usable[0]
            return RouteDecision(alias, mv.version, mv.backends, "single_version")

        total = sum(mv.weight for mv in usable)
        if total <= 0:
            raise InvalidRouteConfigError(
                f"alias {alias!r} has non-positive total canary weight",
                detail={"alias": alias, "weights": {mv.version: mv.weight for mv in usable}},
            )
        digest = hashlib.sha256(f"{alias}|{caller_id}".encode()).hexdigest()
        bucket = int(digest, 16) % total
        acc = 0
        for mv in sorted(usable, key=lambda m: m.version):
            acc += mv.weight
            if bucket < acc:
                return RouteDecision(alias, mv.version, mv.backends, "canary_weight")
        raise InvalidRouteConfigError(  # 理论上不可达
            f"canary bucket {bucket} escaped total {total}", detail={"alias": alias}
        )
