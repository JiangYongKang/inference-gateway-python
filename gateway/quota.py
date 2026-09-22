"""Per-caller quota enforcement (fixed window).

A call is *reserved* before any backend work starts. Reservations are
explicitly refundable so that requests rejected by the concurrency/queue
layer, or cancelled before a slot is acquired, do not burn the caller's
quota. Time is injected (``clock`` -> monotonic seconds) to keep tests
deterministic.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Callable, Mapping


@dataclass
class _Window:
    window_start: float
    used: int = 0


class QuotaManager:
    def __init__(self,
                 specs: Mapping[str, "object"],
                 clock: Callable[[], float] | None = None) -> None:
        self._specs = dict(specs)
        self._clock = clock or time.monotonic
        self._windows: dict[str, _Window] = {}
        self._lock = asyncio.Lock()

    def limit_for(self, caller: str) -> int:
        spec = self._specs.get(caller)
        return spec.limit if spec is not None else 0

    def _current(self, caller: str) -> _Window:
        spec = self._specs.get(caller)
        now = self._clock()
        win = self._windows.get(caller)
        window_seconds = spec.window_seconds if spec is not None else 60.0
        if win is None or now - win.window_start >= window_seconds:
            win = _Window(window_start=now)
            self._windows[caller] = win
        return win

    def try_acquire(self, caller: str) -> bool:
        """Reserve one unit for ``caller``. Callers without a spec get 0."""
        win = self._current(caller)
        limit = self.limit_for(caller)
        if limit <= 0 or win.used >= limit:
            return False
        win.used += 1
        return True

    def refund(self, caller: str) -> None:
        """Give a reservation back (reject / cancel before backend work)."""
        win = self._windows.get(caller)
        if win is not None and win.used > 0:
            win.used -= 1

    def usage(self, caller: str) -> int:
        return self._current(caller).used

    def reset(self) -> None:
        self._windows.clear()
