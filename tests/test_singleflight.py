"""Deterministic coalescing tests."""
from __future__ import annotations

import asyncio

import pytest

from gateway.singleflight import SingleFlight


@pytest.mark.asyncio
async def test_concurrent_calls_share_one_execution():
    sf = SingleFlight()
    calls = 0
    started = asyncio.Event()

    async def work():
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.sleep(0.02)
        return "v"

    results = await asyncio.gather(*(sf.execute("k", work) for _ in range(5)))
    values = [v for v, merged in results]
    flags = [merged for v, merged in results]
    assert values == ["v"] * 5
    assert flags.count(False) == 1 and flags.count(True) == 4
    assert calls == 1


@pytest.mark.asyncio
async def test_follower_cancel_keeps_shared_result_for_others():
    sf = SingleFlight()
    calls = 0
    release = asyncio.Event()

    async def work():
        nonlocal calls
        calls += 1
        await release.wait()
        return 42

    t_cancel = asyncio.create_task(sf.execute("k", work))
    t_keep = asyncio.create_task(sf.execute("k", work))  # noqa
    await asyncio.sleep(0.01)
    t_cancel.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t_cancel
    release.set()
    value, merged = await t_keep
    assert value == 42 and merged is True
    assert calls == 1


@pytest.mark.asyncio
async def test_error_is_shared_not_rerun():
    sf = SingleFlight()
    calls = 0

    async def boom():
        nonlocal calls
        calls += 1
        raise RuntimeError("bang")

    with pytest.raises(RuntimeError):
        await asyncio.gather(sf.execute("k", boom), sf.execute("k", boom))
    assert calls == 1  # shared error, no retry
