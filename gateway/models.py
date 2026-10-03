"""请求 / 响应模型。响应字段只做新增、不改语义，保持对旧客户端兼容。"""

from __future__ import annotations

from pydantic import BaseModel, Field


class InferRequest(BaseModel):
    model: str = Field(..., description="模型别名，如 'echo'")
    prompt: str
    caller_id: str = Field(..., description="调用方身份，用于配额与灰度分组")
    request_id: str | None = Field(default=None, description="调用方提供的幂等标识")
    timeout_ms: int | None = Field(default=None, ge=1, description="单次后端调用超时")


class InferResponse(BaseModel):
    request_id: str
    model: str
    version: str
    backend: str
    output: str
    cached: bool = False
    attempts: int = 1
    fallback_from: list[str] = Field(default_factory=list)
    # 以下为治理新增字段（只新增，旧客户端可忽略）：
    replayed: bool = False       # True = 命中同一调用标识的历史结论，未重新执行后端
    reuse_hit: bool = False      # True = 命中跨请求复用结论，未重新执行后端
    reused_from: str | None = None  # 复用来源结论的 request_id（无则为 None）


class ErrorBody(BaseModel):
    code: str
    message: str
    detail: dict = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorBody
