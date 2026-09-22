"""Global concurrency limiter with a bounded queue.

Admission states for a request:

* a free slot is available            -> admitted immediately;
* all slots busy but ``in_queue < max_queue`` -> parked in the FIFO queue;
* queue already full                  -> :class:`QueueLimitError` *fast reject*.

A caller cancelled while parked leaves the queue immediately (it never held a
slot). Semantics are implemented with a Condition so that freeing a slot
wakes exactly the next parked waiter.
"""
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass

from gateway.errors import ConcurrencyLimitError, QueueLimitError


@dataclass
class _Ticket:
    granted: bool = False


class ConcurrencyLimiter:
    def __init__(self, max_concurrency: int, max_queue: int) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1")
        if max_queue < 0:
            raise ValueError("max_queue must be >= 0")
        self._max_concurrency = max_concurrency
        self._max_queue = max_queue
        self._active = 0
        self._in_queue = 0
        self._cond = asyncio.Condition()

    @property
    def active(self) -> int:
        return self._active

    @property
    def in_queue(self) -> int:
        return self._in_queue

    @contextlib.asynccontextmanager
    async def acquire(self):
        async with self._cond:
            if self._active >= self._max_concurrency:
                if self._in_queue >= self._max_queue:
                    raise QueueLimitError(
                        "queue full, fast rejecting",
                        details={"active": self._active,
                                 "in_queue": self._in_queue,
                                 "max_queue": self._max_queue})
                self._in_queue += 1
                ticket = _Ticket()
                try:
                    await self._cond.wait_for(
                        lambda: self._active < self._max_concurrency)
                    ticket.granted = True
                except asyncio.CancelledError:
                    # Parked caller gave up: leave the queue without touching
                    # any slot, and let a sibling waiter take the wakeup.
                    if not ticket.granted:
                        self._in_queue -= 1
                        self._cond.notify()
                    raise
                self._in_queue -= 1
            self._active += 1
        try:
            yield
        finally:
            async with self._cond:
                self._active -= 1
                self._cond.notify()
