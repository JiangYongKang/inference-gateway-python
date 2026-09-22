"""Deterministic ledger tests: unique execution, replay, error replay."""
from __future__ import annotations

import asyncio

import pytest

from gateway.idempotency import IdempotencyLedger, State


@pytest.mark.asyncio
async def test_unique_execution_then_replay():
    led = IdempotencyLedger()
    assert await led.begin("r1") == "leader"
    assert await led.begin("r1") == "follower"
    led.complete("r1", {"version": "v1", "backend": "b1"})
    assert await led.begin("r1") == "replay"
    assert led.get_outcome("r1")["version"] == "v1"
    assert led.state("r1") is State.COMPLETED


@pytest.mark.asyncio
async def test_follower_receives_identical_outcome():
    led = IdempotencyLedger()
    assert await led.begin("r2") == "leader"
    outcome = {}

    async def follower():
        assert await led.begin("r2") == "follower"
        outcome["v"] = await led.wait_outcome("r2")

    t = asyncio.create_task(follower())
    await asyncio.sleep(0.01)
    led.complete("r2", 7)
    await t
    assert outcome["v"] == 7


@pytest.mark.asyncio
async def test_failure_is_remembered_not_contradicted():
    led = IdempotencyLedger()
    await led.begin("r3")
    led.fail("r3", RuntimeError("boom"))
    assert await led.begin("r3") == "replay_error"
    with pytest.raises(RuntimeError):
        led.get_outcome("r3")
