"""Deterministic mock-backend tests: output, errors, deadline cancellation."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

import pytest

from gateway.backend import BackendRegistry, MockBackend
from gateway.errors import (BackendError, BackendTimeoutError,
                            NoBackendAvailableError)


@dataclass
class Req:
    request_id: str
    input: str


@pytest.mark.asyncio
async def test_deterministic_output_and_side_effect():
    b = MockBackend("b1")
    out = await b.infer(Req("r1", "ping"), "v1")
    assert out == "mock-output(b1,v1,ping)"
    assert b.completed_keys == [("r1", "v1")]


@pytest.mark.asyncio
async def test_scripted_error_leaves_no_side_effect():
    b = MockBackend("b1", mode="error")
    with pytest.raises(BackendError):
        await b.infer(Req("r2", "x"), "v1")
    assert b.completed == 0
    assert b.completed_keys == []


@pytest.mark.asyncio
async def test_timeout_cancelled_at_deadline():
    b = MockBackend("b1", mode="timeout")
    with pytest.raises((asyncio.TimeoutError, BackendTimeoutError)):
        await asyncio.wait_for(b.infer(Req("r3", "x"), "v1"), timeout=0.01)
    assert b.completed == 0


@pytest.mark.asyncio
async def test_per_request_failure_set():
    b = MockBackend("b1", error_requests={"bad"})
    assert await b.infer(Req("ok", "i"), "v1") == "mock-output(b1,v1,i)"
    with pytest.raises(BackendError):
        await b.infer(Req("bad", "i"), "v1")


def test_registry_availability():
    reg = BackendRegistry({"b1": MockBackend("b1")})
    assert reg.available("b1")
    reg.set_available("b1", False)
    assert not reg.available("b1")
    with pytest.raises(NoBackendAvailableError):
        reg.get("ghost")
