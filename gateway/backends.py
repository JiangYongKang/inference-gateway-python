"""确定性本地模拟推理后端：输出只取决于 (backend, version, prompt)，可脚本化注入故障。"""

from __future__ import annotations

import asyncio
import hashlib
from typing import Protocol

from .errors import BackendExecutionError, BackendTimeoutError


class InferenceBackend(Protocol):
    name: str

    async def infer(self, prompt: str, *, version: str, timeout_ms: int | None) -> str:
        ...


class DeterministicBackend:
    """本地模拟后端。

    - 输出为 sha256(name|version|prompt) 的确定性前缀，无随机性、无真实模型。
    - latency_ms 为固定处理延迟。
    - fault_plan 为脚本化故障序列：每次调用弹出队首元素，
      "ok" 正常返回，"timeout" 挂起直到超时，"error" 抛出执行错误；
      序列为空后默认正常。测试借此获得确定性的故障链路。
    """

    def __init__(self, name: str, *, latency_ms: int = 0, fault_plan: list[str] | None = None) -> None:
        self.name = name
        self.latency_ms = latency_ms
        self._fault_plan: list[str] = list(fault_plan or [])
        self.calls: int = 0

    def _next_fault(self) -> str:
        if self._fault_plan:
            return self._fault_plan.pop(0)
        return "ok"

    async def infer(self, prompt: str, *, version: str, timeout_ms: int | None) -> str:
        self.calls += 1
        fault = self._next_fault()

        async def _work() -> str:
            if fault == "error":
                raise BackendExecutionError(
                    f"backend {self.name} injected execution error",
                    detail={"backend": self.name},
                )
            if fault == "timeout":
                await asyncio.sleep(3600)  # 挂起，由外层超时取消
            if self.latency_ms:
                await asyncio.sleep(self.latency_ms / 1000)
            digest = hashlib.sha256(f"{self.name}|{version}|{prompt}".encode()).hexdigest()
            return f"{self.name}:{version}:{digest[:16]}"

        if timeout_ms is None:
            return await _work()
        try:
            return await asyncio.wait_for(_work(), timeout=timeout_ms / 1000)
        except asyncio.TimeoutError:
            raise BackendTimeoutError(
                f"backend {self.name} timed out after {timeout_ms}ms",
                detail={"backend": self.name, "timeout_ms": timeout_ms},
            ) from None
