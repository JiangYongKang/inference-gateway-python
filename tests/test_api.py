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
