"""Shared deterministic fixtures.

Logging: the gateway logger writes JSON lines to stderr directly; we attach a
caplog handler too so pytest captures request input / selection basis / verdict
in the test report.
"""
from __future__ import annotations

import logging

import pytest

from gateway.backend import BackendRegistry, MockBackend
from gateway.config_loader import load_config
from gateway.models import InferenceRequest
from gateway.service import InferenceService
from gateway.telemetry import LOGGER_NAME, configure_logging


@pytest.fixture
def gateway_logs():
    """Collect gateway JSON log lines deterministically for assertions."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)

    class _Collect(logging.Handler):
        def __init__(self):
            super().__init__(logging.DEBUG)
            self.lines: list[str] = []

        def emit(self, record):
            self.lines.append(record.getMessage())

    handler = _Collect()
    logger.addHandler(handler)
    try:
        yield handler.lines
    finally:
        logger.removeHandler(handler)


@pytest.fixture(autouse=True)
def _gateway_logs_to_stderr():
    # Keep the JSON lines visible in the unit-test report as well.
    configure_logging(logging.DEBUG)
    yield


@pytest.fixture
def env_factory():
    def build(*, max_concurrency=4, max_queue=2, weights=(900, 100),
              quotas=None, attempt_timeout=0.2, latency=0.0):
        backends = {
            "a1": MockBackend("a1", latency=latency),
            "a2": MockBackend("a2", latency=latency),
            "stable": MockBackend("stable", latency=latency),
            "c1": MockBackend("c1", latency=latency),
        }
        registry = BackendRegistry(backends)
        data = {
            "aliases": {
                "chat": {
                    "versions": [
                        {"version": "v1", "backends": ["a1", "a2"],
                         "weight": weights[0]},
                        {"version": "v2", "backends": ["c1"],
                         "weight": weights[1]},
                    ],
                    "groups": [{"group": "beta", "version": "v2"}],
                    "stable_version": "v1",
                }
            },
            "quotas": quotas or {
                "alpha": {"limit": 5, "window_seconds": 60},
                "beta": {"limit": 1, "window_seconds": 60},
            },
            "limits": {"max_concurrency": max_concurrency,
                       "max_queue": max_queue},
        }
        # stable backend serves v1 fallback chain: alias v1 backends are
        # a1,a2 ; keep "stable" reachable by registering it into v1 list in
        # fallback-specific tests instead.
        config = load_config(data)
        svc = InferenceService(config, registry,
                               attempt_timeout=attempt_timeout)
        return svc, registry, backends, config

    return build


@pytest.fixture
def req_factory():
    counter = {"n": 0}

    def make(caller="alpha", alias="chat", input_="hello", group=None):
        counter["n"] += 1
        return InferenceRequest(request_id=f"req-{counter['n']}",
                                caller=caller, alias=alias,
                                input=input_, group=group)

    return make
