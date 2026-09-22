"""HTTP integration tests: status mapping + legacy-compatible body."""
from __future__ import annotations

import json

import pytest
from httpx import ASGITransport, AsyncClient

from gateway.api import create_app
from gateway.backend import BackendRegistry, MockBackend
from gateway.config_loader import load_config
from gateway.service import InferenceService
from gateway.telemetry import configure_logging


def _service(*, quotas=None, latency=0.0, attempt_timeout=0.2,
             max_concurrency=4, max_queue=2):
    backends = {n: MockBackend(n, latency=latency)
                for n in ("a1", "a2", "stable", "c1")}
    registry = BackendRegistry(backends)
    data = {
        "aliases": {"chat": {
            "versions": [
                {"version": "v1", "backends": ["a1", "a2"], "weight": 900},
                {"version": "v2", "backends": ["c1"], "weight": 100},
            ],
            "groups": [{"group": "beta", "version": "v2"}],
            "stable_version": "v1",
        }},
        "quotas": quotas or {"alpha": {"limit": 3}},
        "limits": {"max_concurrency": max_concurrency,
                   "max_queue": max_queue},
    }
    svc = InferenceService(load_config(data), registry,
                           log=configure_logging(),
                           attempt_timeout=attempt_timeout)
    return svc, registry, backends


@pytest.mark.asyncio
async def test_healthz():
    svc, _, _ = _service()
    async with AsyncClient(transport=ASGITransport(app=create_app(svc)),
                           base_url="http://t") as client:
        r = await client.get("/healthz")
    assert r.status_code == 200 and r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_infer_legacy_compatible_body():
    svc, _, _ = _service()
    payload = {"request_id": "http-1", "caller": "alpha", "alias": "chat",
               "input": "hello"}
    async with AsyncClient(transport=ASGITransport(app=create_app(svc)),
                           base_url="http://t") as client:
        r = await client.post("/v1/infer", json=payload)
    assert r.status_code == 200
    body = r.json()
    # legacy fields keep their original meaning
    assert body["request_id"] == "http-1"
    assert body["output"] == "mock-output(a1,v1,hello)"
    assert body["served_from_cache"] is False
    assert isinstance(body["attempts"], list) and body["attempts"][0]["ok"]
    # additive fields present
    assert body["charged"] is True and body["fallback"] is False
    assert body["source"] == "backend"


@pytest.mark.asyncio
async def test_quota_returns_429_with_distinct_code():
    svc, _, _ = _service(quotas={"alpha": {"limit": 1}})
    app = create_app(svc)
    body = {"request_id": "q1", "caller": "alpha", "alias": "chat",
            "input": "x"}
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://t") as client:
        assert (await client.post("/v1/infer", json=body)).status_code == 200
        r2 = await client.post("/v1/infer",
                               json={**body, "request_id": "q2"})
    assert r2.status_code == 429
    assert r2.json()["error"]["code"] == "quota_exceeded"


@pytest.mark.asyncio
async def test_unknown_alias_returns_404():
    svc, _, _ = _service()
    async with AsyncClient(transport=ASGITransport(app=create_app(svc)),
                           base_url="http://t") as client:
        r = await client.post("/v1/infer", json={
            "request_id": "q3", "caller": "alpha", "alias": "nope",
            "input": "x"})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "version_not_found"


@pytest.mark.asyncio
async def test_fallback_response_explains_effective_backend():
    svc, registry, _ = _service()
    registry.set_failure("c1", "error")
    async with AsyncClient(transport=ASGITransport(app=create_app(svc)),
                           base_url="http://t") as client:
        r = await client.post("/v1/infer", json={
            "request_id": "q4", "caller": "alpha", "alias": "chat",
            "input": "x", "group": "beta"})
    body = r.json()
    assert r.status_code == 200
    assert body["fallback"] is True
    assert body["version"] == "v1"
    assert body["attempts"][0]["version"] == "v2"
    assert body["attempts"][0]["error_code"] == "backend_error"


@pytest.mark.asyncio
async def test_all_backends_failed_returns_502():
    svc, registry, _ = _service()
    registry.set_failure("a1", "error")
    registry.set_failure("a2", "error")
    async with AsyncClient(transport=ASGITransport(app=create_app(svc)),
                           base_url="http://t") as client:
        r = await client.post("/v1/infer", json={
            "request_id": "q5", "caller": "alpha", "alias": "chat",
            "input": "x"})
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "all_backends_failed"
