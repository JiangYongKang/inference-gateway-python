"""按调用方的独立配额限制：固定窗口计数，时钟可注入以保证测试确定性。

语义：acquire 在请求准入时计费一次；请求被取消或超时后由网关 refund，
不计入消耗；重试与回退发生在同一次准入之内，不重复计费。
"""

from __future__ import annotations

import time
from typing import Callable

from .errors import QuotaExceededError


class QuotaLimiter:
    def __init__(
        self,
        limits: dict[str, int],
        *,
        window_seconds: float = 60.0,
        clock: Callable[[], float] | None = None,
        default_limit: int | None = None,
    ) -> None:
        self._limits = dict(limits)
        self._window = window_seconds
        self._clock = clock or time.monotonic
        self._default = default_limit
        self._usage: dict[str, tuple[int, float]] = {}  # caller -> (count, window_start)

    def _limit_for(self, caller_id: str) -> int | None:
        return self._limits.get(caller_id, self._default)

    def _current(self, caller_id: str) -> tuple[int, float]:
        now = self._clock()
        count, start = self._usage.get(caller_id, (0, now))
        if now - start >= self._window:
            count, start = 0, now
        return count, start

    def acquire(self, caller_id: str) -> None:
        """占用一份配额；超额抛 QuotaExceededError。"""
        limit = self._limit_for(caller_id)
        count, start = self._current(caller_id)
        if limit is not None and count >= limit:
            raise QuotaExceededError(
                f"caller {caller_id!r} exceeded quota {limit}/{self._window}s",
                detail={"caller_id": caller_id, "limit": limit, "window_seconds": self._window},
            )
        self._usage[caller_id] = (count + 1, start)

    def refund(self, caller_id: str) -> None:
        """退回一份配额（请求被取消/超时时由网关调用）。"""
        count, start = self._current(caller_id)
        self._usage[caller_id] = (max(0, count - 1), start)

    def usage(self, caller_id: str) -> int:
        return self._current(caller_id)[0]
