"""Deterministic selection inputs: canary bucket.

Python's built-in ``hash()`` is salted per process and therefore unusable for
reproducible canary decisions. We use FNV-1a (32 bit) so a given (caller,
salt) always maps to the same bucket in ``[0, 1000)``.
"""
from __future__ import annotations

FNV_OFFSET_32 = 0x811C9DC5
FNV_PRIME_32 = 0x01000193
MASK_32 = 0xFFFFFFFF
BUCKETS = 1000


def fnv1a_32(data: bytes) -> int:
    h = FNV_OFFSET_32
    for b in data:
        h ^= b
        h = (h * FNV_PRIME_32) & MASK_32
    return h


def canary_bucket(caller: str, salt: str = "") -> int:
    """Return a stable bucket in [0, 1000) for (caller, salt)."""
    return fnv1a_32(f"{caller}|{salt}".encode("utf-8")) % BUCKETS
