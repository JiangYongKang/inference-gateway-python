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


class IdempotencyConflictError(GatewayError):
    """同一调用方的同一调用标识绑定了不同模型或不同请求内容。"""

    code = "idempotency_conflict"
    http_status = 409


class RecordCapacityError(GatewayError):
    """该调用方的治理记录区已满，且无过期项可清，新请求被快速拒绝。"""

    code = "record_capacity_exceeded"
    http_status = 507


class InvalidGovernanceConfigError(GatewayError):
    """重放/复用治理配置非法（保留期、容量等越界），加载阶段直接拒绝。"""

    code = "invalid_governance_config"
    http_status = 500
