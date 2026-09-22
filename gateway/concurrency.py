"""并发隔离与合并：有界并发 + 有界队列，满员快速拒绝；相同合并键的在途请求共享一次执行。

- 槽位满时排队，队列也满时立即抛 QueueFullError，不无限阻塞、不静默丢弃。
- 合并键相同的在途请求由 leader 执行一次，follower 共享结果（不占用槽位）。
- 等待者被取消时不获取槽位；执行者被取消时 finally 释放槽位，不泄漏配额。
- follower 通过 shield 等待共享任务：自身取消不会取消共享执行。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

from .errors import QueueFullError

T = TypeVar("T")


class ConcurrencyLimiter:
    def __init__(self, *, max_concurrent: int, max_queue: int) -> None:
        self._max_concurrent = max_concurrent
        self._max_queue = max_queue
        self._inflight = 0
        self._queued = 0
        self._lock = asyncio.Lock()
        self._slot_available = asyncio.Condition(self._lock)
        self._coalescing: dict[str, asyncio.Task] = {}

    @property
    def inflight(self) -> int:
        return self._inflight

    @property
    def queued(self) -> int:
        return self._queued

    async def execute(
        self,
        coalesce_key: str | None,
        factory: Callable[[], Awaitable[T]],
    ) -> tuple[T, bool]:
        """在并发配额内执行 factory，返回 (结果, 是否共享了他人执行)。"""
        if coalesce_key is None:
            return await self._run_with_slot(factory), False

        async with self._lock:
            task = self._coalescing.get(coalesce_key)
            if task is None:
                task = asyncio.ensure_future(self._run_with_slot(factory))
                self._coalescing[coalesce_key] = task
                leader = True
            else:
                leader = False

        if not leader:
            return await asyncio.shield(task), True
        try:
            return await asyncio.shield(task), False
        finally:
            async with self._lock:
                self._coalescing.pop(coalesce_key, None)

    async def _run_with_slot(self, factory: Callable[[], Awaitable[T]]) -> T:
        await self._acquire()
        try:
            return await factory()
        finally:
            await self._release()

    async def _acquire(self) -> None:
        queued = False
        async with self._lock:
            if self._inflight < self._max_concurrent:
                self._inflight += 1
                return
            if self._queued >= self._max_queue:
                raise QueueFullError(
                    f"concurrency queue full ({self._max_queue})",
                    detail={"max_concurrent": self._max_concurrent, "max_queue": self._max_queue},
                )
            self._queued += 1
            queued = True
        try:
            async with self._slot_available:
                while self._inflight >= self._max_concurrent:
                    await self._slot_available.wait()
                self._inflight += 1
        finally:
            if queued:
                async with self._lock:
                    self._queued -= 1

    async def _release(self) -> None:
        async with self._slot_available:
            self._inflight -= 1
            self._slot_available.notify()
