"""Domain value objects (skeleton: fields only)."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class VersionSpec:
    version: str
    backends: list[str] = field(default_factory=list)
    weight: int = 0


@dataclass(frozen=True)
class GroupRoute:
    group: str
    version: str


@dataclass(frozen=True)
class AliasConfig:
    alias: str
    versions: list[VersionSpec]
    groups: list[GroupRoute] = field(default_factory=list)
    stable_version: str | None = None


@dataclass(frozen=True)
class QuotaSpec:
    caller: str
    limit: int
    window_seconds: float = 60.0


@dataclass
class GatewayConfig:
    aliases: dict[str, AliasConfig] = field(default_factory=dict)
    quotas: dict[str, QuotaSpec] = field(default_factory=dict)
    max_concurrency: int = 0
    max_queue: int = 0


@dataclass(frozen=True)
class BackendAttempt:
    backend: str
    version: str
    latency: float
    ok: bool
    error_code: str | None = None


@dataclass(frozen=True)
class InferenceResult:
    request_id: str
    alias: str
    version: str
    backend: str
    output: str
    served_from_cache: bool
    attempts: tuple[BackendAttempt, ...] = ()
    charged: bool = True
    # Additive fields (old clients may ignore them; the legacy fields above
    # keep their original meaning).
    source: str = "backend"  # backend | cache | coalesced | replay
    fallback: bool = False


@dataclass
class InferenceRequest:
    request_id: str
    caller: str
    alias: str
    input: str
    group: str | None = None
    deadline: float | None = None
