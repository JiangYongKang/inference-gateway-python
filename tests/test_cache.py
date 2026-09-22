"""Deterministic TTL cache tests."""
from __future__ import annotations

from gateway.cache import ResultCache


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_put_get_and_expiry():
    c = Clock()
    cache = ResultCache(ttl_seconds=10, clock=c)
    assert cache.get("k") is None
    cache.put("k", "v", version="v1", backend="b1")
    e = cache.get("k")
    assert e is not None and e.value == "v" and e.backend == "b1"
    c.advance(10)
    assert cache.get("k") is None
