"""网关错误类型：所有失败场景都有可区分的错误码。"""

from __future__ import annotations


class GatewayError(Exception):
    """网关层错误基类，code 用于客户端区分失败原因。"""

    code: str = "gateway_error"
    http_status: int = 500

    def __init__(self, message: str = "", *, detail: dict | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.detail = detail or {}


class UnknownModelError(GatewayError):
    code = "unknown_model"
    http_status = 404


class NoAvailableBackendError(GatewayError):
    code = "no_available_backend"
    http_status = 503


class InvalidRouteConfigError(GatewayError):
    code = "invalid_route_config"
    http_status = 500


class QuotaExceededError(GatewayError):
    code = "quota_exceeded"
    http_status = 429


class ConcurrencyLimitError(GatewayError):
    code = "concurrency_limit"
    http_status = 429


class QueueFullError(ConcurrencyLimitError):
    code = "queue_full"
    http_status = 429


class BackendTimeoutError(GatewayError):
    code = "backend_timeout"
    http_status = 504


class BackendExecutionError(GatewayError):
    code = "backend_error"
    http_status = 502


class AllBackendsFailedError(GatewayError):
    code = "all_backends_failed"
    http_status = 502
