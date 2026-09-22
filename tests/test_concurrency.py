"""Deterministic concurrency/queue tests using asyncio synchronization."""
from __future__ import annotations

import asyncio

import pytest

from gateway.concurrency import ConcurrencyLimiter
from gateway.errors import QueueLimitError


@pytest.mark.asyncio
async def test_queue_full_fast_rejects():
    lim = ConcurrencyLimiter(max_concurrency=1, max_queue=1)
    entered = asyncio.Event()

    async def hold():
        async with lim.acquire():
            entered.set()
            await asyncio.sleep(0.05)

    t0 = asyncio.create_task(hold())
    await entered.wait()

    async def wait_slot():
        async with lim.acquire():
            pass

    t1 = asyncio.create_task(wait_slot())        # parks in queue
    await asyncio.sleep(0.01)
    with pytest.raises(QueueLimitError):         # queue now full
        async with lim.acquire():
            pass
    await t0
    await t1
    assert lim.active == 0 and lim.in_queue == 0


@pytest.mark.asyncio
async def test_cancelled_while_parked_releases_queue():
    lim = ConcurrencyLimiter(max_concurrency=1, max_queue=2)
    release = asyncio.Event()
    entered = asyncio.Event()

    async def hold():
        async with lim.acquire():
            entered.set()
            await release.wait()

    t0 = asyncio.create_task(hold())
    await entered.wait()

    async def park():
        async with lim.acquire():
            pass

    t1 = asyncio.create_task(park())
    await asyncio.sleep(0.01)
    assert lim.in_queue == 1
    t1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t1
    assert lim.in_queue == 0
    assert lim.active == 1

    # A later waiter must now be admittable into the freed queue position.
    t2 = asyncio.create_task(park())
    await asyncio.sleep(0.01)
    assert lim.in_queue == 1
    release.set()
    await t0
    await t2
    assert lim.active == 0


@pytest.mark.asyncio
async def test_zero_queue_rejects_when_all_slots_busy():
    lim = ConcurrencyLimiter(max_concurrency=1, max_queue=0)
    entered = asyncio.Event()

    async def hold():
        async with lim.acquire():
            entered.set()
            await asyncio.sleep(0.02)

    t0 = asyncio.create_task(hold())
    await entered.wait()
    with pytest.raises(QueueLimitError):
        async with lim.acquire():
            pass
    await t0
