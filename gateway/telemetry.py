"""Deterministic structured logging + in-memory metrics.

Logs are emitted as single-line JSON so that tests can assert on the exact
fields (request input / selection basis / final verdict). ``Metrics`` keeps
plain counters keyed by caller/alias/version; success and failure counting is
owned by the orchestration layer, which is the only place that knows whether a
fallback chain actually succeeded.
"""
from __future__ import annotations

import json
import logging
import sys
from dataclasses import dataclass, field
from typing import Any

LOGGER_NAME = "inference-gateway"


def configure_logging(level: int = logging.DEBUG) -> logging.Logger:
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    if not any(getattr(h, "_gateway_json", False) for h in logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler._gateway_json = True  # type: ignore[attr-defined]
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    return logger


def log_event(logger: logging.Logger | None, event: str, **fields: Any) -> None:
    """Emit one deterministic JSON log line: ``event`` plus all fields."""
    if logger is None:
        logger = configure_logging()
    payload = {"event": event}
    payload.update(fields)
    logger.debug(json.dumps(payload, sort_keys=True, ensure_ascii=False,
                            default=str))


@dataclass
class Metrics:
    """Per-(caller, alias, version) counters; observable in tests."""

    success: dict[tuple[str, str, str], int] = field(default_factory=dict)
    failure: dict[tuple[str, str, str], int] = field(default_factory=dict)
    rejected: dict[tuple[str, str], int] = field(default_factory=dict)
    timeout: int = 0

    @staticmethod
    def _bump(bucket: dict, key: tuple) -> None:
        bucket[key] = bucket.get(key, 0) + 1

    def record_success(self, caller: str, alias: str, version: str) -> None:
        self._bump(self.success, (caller, alias, version))

    def record_failure(self, caller: str, alias: str, version: str) -> None:
        self._bump(self.failure, (caller, alias, version))

    def record_rejected(self, caller: str, alias: str) -> None:
        self._bump(self.rejected, (caller, alias))

    def record_timeout(self) -> None:
        self.timeout += 1

    def snapshot(self) -> dict:
        return {
            "success": {f"{c}|{a}|{v}": n
                        for (c, a, v), n in self.success.items()},
            "failure": {f"{c}|{a}|{v}": n
                        for (c, a, v), n in self.failure.items()},
            "rejected": {f"{c}|{a}": n
                         for (c, a), n in self.rejected.items()},
            "timeout": self.timeout,
        }

    def reset(self) -> None:
        self.success.clear()
        self.failure.clear()
        self.rejected.clear()
        self.timeout = 0
