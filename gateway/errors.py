"""Error taxonomy. Failures are distinguishable by ``code`` (and HTTP status)."""
from __future__ import annotations


class GatewayError(Exception):
    """Base class for all gateway-level failures."""

    code: str = "gateway_error"
    http_status: int = 500

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details or {}


class ConfigError(GatewayError):
    code = "config_error"
    http_status = 500


class VersionNotFoundError(GatewayError):
    code = "version_not_found"
    http_status = 404


class NoBackendAvailableError(GatewayError):
    code = "no_backend_available"
    http_status = 503


class InvalidCanaryError(GatewayError):
    code = "invalid_canary"
    http_status = 422


class QuotaExceededError(GatewayError):
    code = "quota_exceeded"
    http_status = 429


class ConcurrencyLimitError(GatewayError):
    code = "concurrency_limit"
    http_status = 429


class QueueLimitError(GatewayError):
    code = "queue_limit"
    http_status = 429


class BackendTimeoutError(GatewayError):
    code = "backend_timeout"
    http_status = 504


class BackendError(GatewayError):
    code = "backend_error"
    http_status = 502


class AllBackendsFailedError(GatewayError):
    code = "all_backends_failed"
    http_status = 502


class RequestCancelledError(GatewayError):
    code = "request_cancelled"
    http_status = 499
