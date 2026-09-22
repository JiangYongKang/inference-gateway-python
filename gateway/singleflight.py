"""In-flight request coalescing ("singleflight").

Concurrent calls with the same ``key`` share exactly one leader execution;
followers await the same outcome instead of hitting backends, so N merged
calls produce one backend call and one billable charge.

The leader runs as a detached task and broadcasts completion via an
:class:`asyncio.Event`. Followers await that *event* -- never the leader's
future -- so cancelling one follower only aborts its own wait and can never
cancel the shared execution or spoil the outcome for the other followers.
The service layer decides whether a returned outcome is cached/charged.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


@dataclass
class _Flight:
    followers: int = 0
    result: object = None
    exc: BaseException | None = None
    done: bool = False
    event: asyncio.Event = field(default_factory=asyncio.Event)


class SingleFlight:
    def __init__(self) -> None:
        self._flights: dict[str, _Flight] = {}
        self._lock = asyncio.Lock()

    async def execute(self, key: str,
                      func: Callable[[], Awaitable[T]]) -> tuple[T, bool]:
        """Run/join the flight for ``key``.

        Returns ``(result, coalesced)``; ``coalesced`` is False for the
        leader, True for a caller that piggy-backed on the leader's run.
        """
        async with self._lock:
            flight = self._flights.get(key)
            if flight is None:
                flight = _Flight()
                flight.followers = 1
                self._flights[key] = flight
                is_leader = True
            else:
                flight.followers += 1
                is_leader = False

        if is_leader:
            async def _run() -> None:
                try:
                    flight.result = await func()
                except BaseException as exc:  # noqa: BLE001 - share errors
                    flight.exc = exc
                finally:
                    flight.done = True
                    flight.event.set()
                    async with self._lock:
                        self._flights.pop(key, None)

            asyncio.ensure_future(_run())

        try:
            await flight.event.wait()
        except asyncio.CancelledError:
            async with self._lock:
                flight.followers -= 1
            raise

        async with self._lock:
            flight.followers -= 1
        if flight.exc is not None:
            raise flight.exc
        return flight.result, (not is_leader)  # type: ignore[return-value]
