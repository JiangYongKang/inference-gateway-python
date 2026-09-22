"""Load + validate gateway configuration from a plain JSON-like dict.

Validation performed at load time (fail fast, before serving traffic):

* alias names, version ids and group targets must be internally consistent;
* every version must declare at least one backend;
* canary weights must be positive integers summing to 1000;
* the stable (fallback) version must be registered;
* quota limits must be positive integers; global limits must be positive.
"""
from __future__ import annotations

import json
from pathlib import Path

from gateway.errors import ConfigError, InvalidCanaryError
from gateway.models import (AliasConfig, GatewayConfig, GroupRoute, QuotaSpec,
                            VersionSpec)
from gateway.pipeline import BUCKETS


def _version_spec(alias: str, raw: dict) -> VersionSpec:
    try:
        version = str(raw["version"])
        backends = list(raw["backends"])
        weight = int(raw.get("weight", 0))
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"malformed version under alias {alias!r}: {exc}",
                          details={"alias": alias}) from exc
    if not backends:
        raise ConfigError(f"version {version!r} declares no backends",
                          details={"alias": alias, "version": version})
    if not all(isinstance(b, str) and b for b in backends):
        raise ConfigError(f"version {version!r} has invalid backend names",
                          details={"alias": alias, "version": version})
    if weight <= 0:
        raise InvalidCanaryError("canary weights must be positive integers",
                                 details={"alias": alias, "version": version})
    return VersionSpec(version=version, backends=backends, weight=weight)


def load_config(data: dict | None = None) -> GatewayConfig:
    if not isinstance(data, dict):
        raise ConfigError("configuration must be a mapping")
    data = data or {}

    aliases: dict[str, AliasConfig] = {}
    for alias, raw_alias in (data.get("aliases") or {}).items():
        versions = [_version_spec(alias, v)
                    for v in raw_alias.get("versions", [])]
        if not versions:
            raise ConfigError(f"alias {alias!r} declares no versions",
                              details={"alias": alias})
        total = sum(v.weight for v in versions)
        if total != BUCKETS:
            raise InvalidCanaryError(
                f"canary weights must sum to {BUCKETS}, got {total}",
                details={"alias": alias, "sum": total})
        known = {v.version for v in versions}
        groups = [GroupRoute(str(g["group"]), str(g["version"]))
                  for g in raw_alias.get("groups", [])]
        for g in groups:
            if g.version not in known:
                raise ConfigError(
                    f"group {g.group!r} pins unregistered version "
                    f"{g.version!r}",
                    details={"alias": alias, "group": g.group,
                             "version": g.version})
        stable = raw_alias.get("stable_version")
        if stable is not None and str(stable) not in known:
            raise ConfigError(
                f"stable_version {stable!r} is not registered",
                details={"alias": alias, "version": str(stable)})
        aliases[alias] = AliasConfig(
            alias=alias, versions=versions, groups=groups,
            stable_version=str(stable) if stable is not None else None)

    quotas: dict[str, QuotaSpec] = {}
    for caller, raw in (data.get("quotas") or {}).items():
        limit = int(raw.get("limit", 0))
        window = float(raw.get("window_seconds", 60.0))
        if limit <= 0:
            raise ConfigError(f"quota limit must be positive for {caller!r}",
                              details={"caller": caller})
        if window <= 0:
            raise ConfigError(f"quota window must be positive for {caller!r}",
                              details={"caller": caller})
        quotas[caller] = QuotaSpec(caller=caller, limit=limit,
                                   window_seconds=window)

    limits = data.get("limits") or {}
    max_concurrency = int(limits.get("max_concurrency", 32))
    max_queue = int(limits.get("max_queue", 16))
    if max_concurrency < 1:
        raise ConfigError("limits.max_concurrency must be >= 1")
    if max_queue < 0:
        raise ConfigError("limits.max_queue must be >= 0")

    return GatewayConfig(aliases=aliases, quotas=quotas,
                         max_concurrency=max_concurrency,
                         max_queue=max_queue)


def load_config_file(path: str | Path) -> GatewayConfig:
    with open(path, encoding="utf-8") as fh:
        return load_config(json.load(fh))
