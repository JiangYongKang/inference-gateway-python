"""Deterministic router tests: group pin, weighted canary, distinct rejects."""
from __future__ import annotations

import logging

import pytest

from gateway.errors import (InvalidCanaryError, NoBackendAvailableError,
                            VersionNotFoundError)
from gateway.models import AliasConfig, GroupRoute, VersionSpec
from gateway.router import Router
from gateway.telemetry import configure_logging

log = configure_logging(logging.INFO)


def _cfg(**kw) -> AliasConfig:
    versions = kw.pop("versions", [
        VersionSpec("v1", ["b1"], weight=900),
        VersionSpec("v2", ["b2"], weight=100),
    ])
    return AliasConfig(alias="chat", versions=versions, **kw)


def test_weighted_canary_boundaries():
    r = Router()
    cfg = _cfg()
    assert r.resolve(cfg, "anyone", None, bucket=0) == "v1"
    assert r.resolve(cfg, "anyone", None, bucket=899) == "v1"
    assert r.resolve(cfg, "anyone", None, bucket=900) == "v2"
    assert r.resolve(cfg, "anyone", None, bucket=999) == "v2"
    log.info("router weighted boundaries checked")


def test_group_pins_version():
    cfg = _cfg(groups=[GroupRoute("beta-testers", "v2")])
    assert Router().resolve(cfg, "u1", "beta-testers", bucket=0) == "v2"


def test_unknown_group_is_version_not_found():
    with pytest.raises(VersionNotFoundError):
        Router().resolve(_cfg(), "u1", "ghost", bucket=0)


def test_bad_weights_sum_rejected():
    cfg = _cfg(versions=[VersionSpec("v1", ["b1"], weight=800),
                         VersionSpec("v2", ["b2"], weight=100)])
    with pytest.raises(InvalidCanaryError) as ei:
        Router().resolve(cfg, "u", None, bucket=0)
    assert ei.value.details["sum"] == 900


def test_non_positive_weight_rejected():
    cfg = _cfg(versions=[VersionSpec("v1", ["b1"], weight=1000),
                         VersionSpec("v2", ["b2"], weight=0)])
    with pytest.raises(InvalidCanaryError):
        Router().resolve(cfg, "u", None, bucket=0)


def test_group_pins_unregistered_version():
    cfg = AliasConfig(
        alias="chat",
        versions=[VersionSpec("v1", ["b1"], weight=1000)],
        groups=[GroupRoute("g", "v9")],
    )
    with pytest.raises(InvalidCanaryError):
        Router().resolve(cfg, "u", "g", bucket=0)


def test_no_available_backend_distinct_error():
    cfg = _cfg(versions=[VersionSpec("v1", ["dead"], weight=1000)])
    r = Router(backend_available=lambda name: False)
    with pytest.raises(NoBackendAvailableError):
        r.resolve(cfg, "u", None, bucket=0)


def test_caller_bucket_is_deterministic():
    from gateway.pipeline import canary_bucket
    assert canary_bucket("user-7", "chat") == canary_bucket("user-7", "chat")
