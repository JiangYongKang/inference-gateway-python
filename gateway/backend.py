"""Deterministic local mock inference backends.

A :class:`MockBackend` never performs real model work. Its behaviour is fully
scriptable, so tests are reproducible:

* ``set_available(False)``          -> backend refuses immediately;
* failure mode ``"error"``          -> raises :class:`BackendError`;
* failure mode ``"timeout"``        -> awaits an event that is never set, so
  the caller's ``wait_for`` cancels it exactly at the deadline (no wall-clock
  races);
* ``latency`` seconds of sleep      -> lets concurrency tests occupy a slot;
* per-request-id failure sets       -> script a failure for exactly one call.

``completed_keys`` records ``(request_id, version)`` for calls that finished
successfully; a cancelled call never reaches the counter, so cancellation
leaves no side effect behind.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from gateway.errors import (BackendError, BackendTimeoutError,
                            NoBackendAvailableError)


@dataclass
class MockBackend:
    name: str
    latency: float = 0.0
    mode: str | None = None  # None | "error" | "timeout"
    available: bool = True
    completed: int = 0
    completed_keys: list[tuple[str, str]] = field(default_factory=list)
    error_requests: set[str] = field(default_factory=set)
    timeout_requests: set[str] = field(default_factory=set)
    _hang: asyncio.Event = field(default_factory=asyncio.Event)

    async def infer(self, request, version: str) -> str:
        if not self.available:
            raise NoBackendAvailableError(
                f"backend {self.name!r} is unavailable",
                details={"backend": self.name, "version": version})

        rid = request.request_id
        wants_timeout = self.mode == "timeout" or rid in self.timeout_requests
        wants_error = self.mode == "error" or rid in self.error_requests

        if wants_timeout:
            # Never completes; the caller's ``wait_for`` cancels this await at
            # the deadline. CancelledError is deliberately NOT swallowed, so
            # the timeout boundary stays owned by the caller.
            await self._hang.wait()
            raise BackendTimeoutError(  # pragma: no cover - event never set
                f"backend {self.name!r} timed out",
                details={"backend": self.name, "version": version})

        if self.latency:
            await asyncio.sleep(self.latency)

        if wants_error:
            raise BackendError(
                f"backend {self.name!r} scripted error",
                details={"backend": self.name, "version": version,
                         "request_id": rid})

        self.completed += 1
        self.completed_keys.append((rid, version))
        return f"mock-output({self.name},{version},{request.input})"


class BackendRegistry:
    def __init__(self, backends: dict[str, MockBackend] | None = None) -> None:
        self._backends = dict(backends or {})

    def add(self, backend: MockBackend) -> "BackendRegistry":
        self._backends[backend.name] = backend
        return self

    def get(self, name: str) -> MockBackend:
        backend = self._backends.get(name)
        if backend is None:
            raise NoBackendAvailableError(
                f"backend not registered: {name}",
                details={"backend": name})
        return backend

    def available(self, name: str) -> bool:
        backend = self._backends.get(name)
        return bool(backend and backend.available)

    def set_failure(self, name: str, mode: str | None) -> None:
        self.get(name).mode = mode

    def set_available(self, name: str, available: bool) -> None:
        self.get(name).available = available
