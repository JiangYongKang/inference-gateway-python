"""Request-id ledger: uniqueness while in flight, replay once complete.

Three states per request id:

* ``running``   — exactly one caller gets to execute; concurrent duplicate
  callers wait and receive the same outcome;
* ``completed`` — a later caller receives the stored identical result, even
  if routing configuration changed in between (version switch consistency);
* ``failed``    — the stored error is replayed; failure is not cached in the
  result cache, but it is remembered so a retry of the same id cannot produce
  a contradictory success ("bill twice" is also impossible).

Waiting is again on an :class:`asyncio.Event`, so cancelling a duplicate
waiter cannot disturb the unique execution.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum


class State(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class _Entry:
    state: State = State.RUNNING
    result: object = None
    exc: BaseException | None = None
    event: asyncio.Event = field(default_factory=asyncio.Event)


class IdempotencyLedger:
    def __init__(self) -> None:
        self._entries: dict[str, _Entry] = {}
        self._lock = asyncio.Lock()

    async def begin(self, request_id: str) -> str:
        """Return ``"leader"`` or ``"follower"`` for this request id."""
        async with self._lock:
            entry = self._entries.get(request_id)
            if entry is None:
                self._entries[request_id] = _Entry()
                return "leader"
            if entry.state == State.RUNNING:
                return "follower"
            if entry.state == State.COMPLETED:
                return "replay"
            return "replay_error"

    async def wait_outcome(self, request_id: str) -> object:
        entry = self._entries[request_id]
        await entry.event.wait()
        if entry.exc is not None:
            raise entry.exc
        return entry.result

    def get_outcome(self, request_id: str) -> object:
        entry = self._entries[request_id]
        if entry.exc is not None:
            raise entry.exc
        return entry.result

    def complete(self, request_id: str, result: object) -> None:
        entry = self._entries.get(request_id)
        if entry is None:  # pragma: no cover - protocol use only
            entry = _Entry()
            self._entries[request_id] = entry
        entry.state = State.COMPLETED
        entry.result = result
        entry.event.set()

    def fail(self, request_id: str, exc: BaseException) -> None:
        entry = self._entries.get(request_id)
        if entry is None:  # pragma: no cover - protocol use only
            entry = _Entry()
            self._entries[request_id] = entry
        entry.state = State.FAILED
        entry.exc = exc
        entry.event.set()

    def state(self, request_id: str) -> State | None:
        entry = self._entries.get(request_id)
        return entry.state if entry is not None else None

    def reset(self) -> None:
        self._entries.clear()
