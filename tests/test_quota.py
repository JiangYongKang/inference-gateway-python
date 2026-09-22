"""Deterministic per-caller quota tests: limits, window rollover, refunds."""
from __future__ import annotations

from gateway.models import QuotaSpec
from gateway.quota import QuotaManager


class FakeClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def test_caller_without_spec_has_zero_quota():
    qm = QuotaManager({})
    assert not qm.try_acquire("stranger")


def test_quota_enforced_and_refunded():
    clock = FakeClock()
    qm = QuotaManager({"c1": QuotaSpec("c1", limit=2, window_seconds=60)},
                      clock=clock)
    assert qm.try_acquire("c1")
    assert qm.try_acquire("c1")
    assert not qm.try_acquire("c1")
    qm.refund("c1")
    assert qm.usage("c1") == 1
    assert qm.try_acquire("c1")


def test_window_rolls_over_deterministically():
    clock = FakeClock()
    qm = QuotaManager({"c1": QuotaSpec("c1", limit=1, window_seconds=10)},
                      clock=clock)
    assert qm.try_acquire("c1")
    assert not qm.try_acquire("c1")
    clock.advance(10)
    assert qm.try_acquire("c1")


def test_callers_are_independent():
    qm = QuotaManager({"a": QuotaSpec("a", 1), "b": QuotaSpec("b", 1)})
    assert qm.try_acquire("a")
    assert not qm.try_acquire("a")
    assert qm.try_acquire("b")
