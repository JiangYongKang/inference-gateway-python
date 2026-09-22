"""Alias -> version resolution.

Selection order for one request:

1. Explicit caller-group routing (``groups``): the group pins a version.
2. Otherwise deterministic canary over the alias' weighted versions:
   bucket in [0, 1000) walks cumulative weights.

Failure modes (distinguishable):

* :class:`VersionNotFoundError`  — alias or pinned/resolved version unknown.
* :class:`InvalidCanaryError`    — weights are missing / non-positive / do not
  sum to 1000, or a group pins an unregistered version.
* :class:`NoBackendAvailableError` — resolved version has no *available*
  backend (availability injected so the router stays a pure decision maker).
"""
from __future__ import annotations

from collections.abc import Callable

from gateway.errors import (InvalidCanaryError, NoBackendAvailableError,
                            VersionNotFoundError)
from gateway.models import AliasConfig
from gateway.pipeline import BUCKETS, canary_bucket


class Router:
    def __init__(self,
                 backend_available: Callable[[str], bool] | None = None) -> None:
        self._backend_available = backend_available or (lambda _name: True)

    def _known_versions(self, alias_cfg: AliasConfig) -> set[str]:
        return {v.version for v in alias_cfg.versions}

    def validate(self, alias_cfg: AliasConfig) -> None:
        """Reject illegal canary / group configuration up front."""
        known = self._known_versions(alias_cfg)
        if alias_cfg.stable_version is not None and \
                alias_cfg.stable_version not in known:
            raise VersionNotFoundError(
                f"stable version not registered: {alias_cfg.stable_version}",
                details={"alias": alias_cfg.alias,
                         "version": alias_cfg.stable_version})
        for grp in alias_cfg.groups:
            if grp.version not in known:
                raise InvalidCanaryError(
                    f"group {grp.group!r} pins unregistered version "
                    f"{grp.version!r}",
                    details={"alias": alias_cfg.alias, "group": grp.group,
                             "version": grp.version})
        weights = [(v.version, v.weight) for v in alias_cfg.versions]
        if not weights:
            raise InvalidCanaryError(
                "alias declares no versions",
                details={"alias": alias_cfg.alias})
        bad = [ver for ver, w in weights if w <= 0]
        if bad:
            raise InvalidCanaryError(
                "canary weights must be positive integers",
                details={"alias": alias_cfg.alias, "versions": bad})
        total = sum(w for _, w in weights)
        if total != BUCKETS:
            raise InvalidCanaryError(
                f"canary weights must sum to {BUCKETS}, got {total}",
                details={"alias": alias_cfg.alias, "sum": total})

    def resolve(self, alias_cfg: AliasConfig, caller: str, group: str | None,
                bucket: int | None = None) -> str:
        self.validate(alias_cfg)
        known = self._known_versions(alias_cfg)
        basis: str

        if group is not None:
            match = next((g for g in alias_cfg.groups if g.group == group),
                         None)
            if match is None:
                raise VersionNotFoundError(
                    f"caller group not registered: {group}",
                    details={"alias": alias_cfg.alias, "group": group})
            version = match.version
            basis = f"group={group}->version={version}"
        else:
            if bucket is None:
                bucket = canary_bucket(caller, alias_cfg.alias)
            cumulative = 0
            version = alias_cfg.versions[-1].version
            chosen = None
            for spec in alias_cfg.versions:
                upper = cumulative + spec.weight
                if cumulative <= bucket < upper:
                    chosen = spec.version
                    basis = (f"canary bucket={bucket} in "
                             f"[{cumulative},{upper})->version={spec.version}")
                    break
                cumulative = upper
            if chosen is None:  # pragma: no cover - weights validated == 1000
                raise InvalidCanaryError(
                    "bucket matched no weight range",
                    details={"alias": alias_cfg.alias, "bucket": bucket})
            version = chosen

        if version not in known:
            raise VersionNotFoundError(
                f"resolved version not registered: {version}",
                details={"alias": alias_cfg.alias, "version": version})

        spec = next(v for v in alias_cfg.versions if v.version == version)
        available_backends = [b for b in spec.backends
                              if self._backend_available(b)]
        if not spec.backends or not available_backends:
            raise NoBackendAvailableError(
                f"no available backend for version {version!r}",
                details={"alias": alias_cfg.alias, "version": version,
                         "backends": list(spec.backends)})
        return version
