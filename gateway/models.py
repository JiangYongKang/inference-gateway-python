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


class ErrorBody(BaseModel):
    code: str
    message: str
    detail: dict = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorBody
