"""Config validation tests: distinct rejection reasons at load time."""
from __future__ import annotations

import pytest

from gateway.config_loader import load_config
from gateway.errors import ConfigError, InvalidCanaryError

_GOOD = {
    "aliases": {
        "chat": {
            "versions": [
                {"version": "v1", "backends": ["b1"], "weight": 900},
                {"version": "v2", "backends": ["b2"], "weight": 100},
            ],
            "groups": [{"group": "g", "version": "v2"}],
            "stable_version": "v1",
        }
    },
    "quotas": {"c1": {"limit": 3}},
    "limits": {"max_concurrency": 2, "max_queue": 1},
}


def test_good_config_loads():
    cfg = load_config(_GOOD)
    assert cfg.aliases["chat"].stable_version == "v1"
    assert cfg.quotas["c1"].limit == 3


def test_weights_must_sum_1000():
    bad = {"aliases": {"chat": {"versions": [
        {"version": "v1", "backends": ["b1"], "weight": 500}]}}}
    with pytest.raises(InvalidCanaryError):
        load_config(bad)


def test_version_without_backend_rejected():
    bad = {"aliases": {"chat": {"versions": [
        {"version": "v1", "backends": [], "weight": 1000}]}}}
    with pytest.raises(ConfigError):
        load_config(bad)


def test_group_target_must_exist():
    bad = {"aliases": {"chat": {"versions": [
        {"version": "v1", "backends": ["b1"], "weight": 1000}],
        "groups": [{"group": "g", "version": "v9"}]}}}
    with pytest.raises(ConfigError):
        load_config(bad)


def test_non_positive_quota_rejected():
    bad = {"aliases": _GOOD["aliases"],
           "quotas": {"c1": {"limit": 0}}}
    with pytest.raises(ConfigError):
        load_config(bad)
