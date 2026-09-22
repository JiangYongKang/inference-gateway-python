"""Deterministic TTL result cache.

Only successful results are ever inserted (the service layer owns that
decision), so failed/timed-out calls can never poison the cache. Expiry is
checked against an injectable clock; entries also store the version/backend
that produced them for observability.
"""
from __future__ import annotations

import time
from typing import Callable


class CacheEntry:
    __slots__ = ("value", "stored_at", "version", "backend")

    def __init__(self, value: object, stored_at: float,
                 version: str, backend: str) -> None:
        self.value = value
        self.stored_at = stored_at
        self.version = version
        self.backend = backend


class ResultCache:
    def __init__(self, ttl_seconds: float = 300.0,
                 clock: Callable[[], float] | None = None) -> None:
        self._ttl = ttl_seconds
        self._clock = clock or time.monotonic
        self._store: dict[str, CacheEntry] = {}

    def get(self, key: str) -> CacheEntry | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if self._clock() - entry.stored_at >= self._ttl:
            self._store.pop(key, None)
            return None
        return entry

    def put(self, key: str, value: object, *, version: str,
            backend: str) -> None:
        self._store[key] = CacheEntry(value, self._clock(), version, backend)

    def __len__(self) -> int:
        return len(self._store)

    def reset(self) -> None:
        self._store.clear()
