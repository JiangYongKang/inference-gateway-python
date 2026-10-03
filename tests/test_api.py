"""HTTP 层：错误码到状态码映射、响应兼容性、本地端到端验证。"""

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.backends import DeterministicBackend
from main import create_app

log = logging.getLogger("gateway.test")


@pytest.fixture
def client(make_gateway):
    gw = make_gateway(
        versions=[("m", "v1", ["primary", "backup"], 1)],
        caller_groups=[],
        backends={
            "primary": DeterministicBackend("primary", fault_plan=["error"]),
            "backup": DeterministicBackend("backup"),
        },
        limits={"limited": 1},
    )
    c = TestClient(create_app(gw))
    c.gateway = gw
    return c


def test_root_compatible():
    from main import app
    r = TestClient(app).get("/")
    assert r.status_code == 200 and r.json() == {"Hello": "World"}


def test_infer_success_with_fallback_visible(client):
    r = client.post("/v1/infer", json={
        "model": "m", "prompt": "hello", "caller_id": "c1", "request_id": "req-1"})
    body = r.json()
    log.info("input=/v1/infer req-1 judgement=%s", body)
    assert r.status_code == 200
    assert body["request_id"] == "req-1"
    assert body["version"] == "v1"
    assert body["backend"] == "backup"          # 实际生效后端
    assert body["fallback_from"] == ["primary"]  # 回退路径可见
    assert set(body) >= {"request_id", "model", "version", "backend", "output"}


def test_unknown_model_404(client):
    r = client.post("/v1/infer", json={"model": "ghost", "prompt": "x", "caller_id": "c"})
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "unknown_model"


def test_quota_exceeded_429_distinct(client):
    payload = {"model": "m", "prompt": "x", "caller_id": "limited"}
    assert client.post("/v1/infer", json=payload).status_code == 200
    r = client.post("/v1/infer", json=payload)
    log.info("input=limited#2 judgement=%s", r.json())
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "quota_exceeded"


def test_backend_failure_502_distinct(client):
    client.gateway._backends["backup"] = DeterministicBackend("backup", fault_plan=["error"])
    r = client.post("/v1/infer", json={"model": "m", "prompt": "z", "caller_id": "c2"})
    log.info("input=all-failing judgement=%s", r.json())
    assert r.status_code == 502
    assert r.json()["error"]["code"] == "all_backends_failed"


def test_stats_endpoint(client):
    client.post("/v1/infer", json={"model": "m", "prompt": "s", "caller_id": "c9"})
    r = client.get("/v1/stats")
    assert r.status_code == 200 and r.json()["success"] == 1


@pytest.fixture
def retention_client(make_gateway):
    gw = make_gateway(
        versions=[("m", "v1", ["b"], 1)],
        backends={"b": DeterministicBackend("b")},
        retention=dict(
            idempotency_ttl_seconds=300.0,
            idempotency_max_entries_per_caller=2,
            reuse_ttl_seconds=60.0,
            reuse_max_entries_per_caller=10,
        ),
    )
    c = TestClient(create_app(gw))
    c.gateway = gw
    return c


def test_replay_and_reuse_flags_over_http(retention_client):
    r1 = retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p", "caller_id": "c1", "request_id": "rid-1"}).json()
    r2 = retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p", "caller_id": "c1", "request_id": "rid-1"}).json()
    r3 = retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p", "caller_id": "c1", "request_id": "rid-2"}).json()
    log.info("input=replay+reuse judgement r2.replayed=%s r3.reused=%s",
             r2["replayed"], r3["reused"])
    assert r1["replayed"] is False and r1["reused"] is False
    assert r2["replayed"] is True and r2["output"] == r1["output"]
    assert r3["reused"] is True and r3["replayed"] is False
    # 旧字段名字与含义不变
    assert set(r2) >= {"request_id", "model", "version", "backend", "output",
                       "cached", "attempts", "fallback_from"}


def test_conflict_409_distinct(retention_client):
    retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p1", "caller_id": "c1", "request_id": "rid-1"})
    r = retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p2", "caller_id": "c1", "request_id": "rid-1"})
    log.info("input=conflict judgement=%s", r.json())
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "idempotency_conflict"


def test_store_full_429_distinct(retention_client):
    for i in range(2):
        retention_client.post("/v1/infer", json={
            "model": "m", "prompt": f"p{i}", "caller_id": "c1", "request_id": f"rid-{i}"})
    r = retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "px", "caller_id": "c1", "request_id": "rid-x"})
    log.info("input=store-full judgement=%s", r.json())
    assert r.status_code == 429
    assert r.json()["error"]["code"] == "idempotency_store_full"


def test_stats_expose_retention(retention_client):
    retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p", "caller_id": "c1", "request_id": "rid-1"})
    retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p", "caller_id": "c1", "request_id": "rid-1"})
    body = retention_client.get("/v1/stats").json()
    log.info("input=stats judgement=%s", body["retention"])
    assert body["success"] == 1
    assert body["retention"]["counters"]["replay_hits"] == 1
    assert body["retention"]["records"]["idempotency"] == {"c1": 1}


def test_admin_purge_endpoint(retention_client):
    retention_client.post("/v1/infer", json={
        "model": "m", "prompt": "p", "caller_id": "c1"})
    r = retention_client.post("/v1/admin/reuse/purge", params={"model": "m", "version": "v1"})
    assert r.status_code == 200 and r.json()["removed"] == 1
