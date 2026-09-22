"""FastAPI wiring.

Endpoints:

* ``GET  /healthz``          — liveness;
* ``POST /v1/infer``         — inference (legacy-compatible response body).

Response body (stable for old clients)::

    {
      "request_id", "alias", "version", "backend", "output",
      "served_from_cache",
      "attempts": [{"backend","version","latency","ok","error_code"}],
      # additive fields, safe for old clients to ignore:
      "charged", "source", "fallback"
    }

Errors always look like ``{"error": {"code", "message", "details"}}`` and the
HTTP status reflects the failure class, so callers can distinguish:
404 version_not_found / 422 invalid_canary / 429 quota_exceeded|queue_limit /
503 no_backend_available / 504 backend_timeout / 502 all_backends_failed.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from gateway.backend import BackendRegistry, MockBackend
from gateway.config_loader import load_config, load_config_file
from gateway.errors import GatewayError
from gateway.models import InferenceRequest
from gateway.service import InferenceService
from gateway.telemetry import configure_logging, log_event


class InferBody(BaseModel):
    request_id: str = Field(..., min_length=1)
    caller: str = Field(..., min_length=1)
    alias: str = Field(..., min_length=1)
    input: str
    group: str | None = None


def _build_default_service() -> InferenceService:
    config_path = Path("config/sample.json")
    config = load_config_file(config_path)
    # One mock backend per backend name referenced by the config.
    names = {b for a in config.aliases.values()
             for v in a.versions for b in v.backends}
    registry = BackendRegistry({n: MockBackend(n) for n in names})
    return InferenceService(config, registry,
                            log=configure_logging())


def create_app(service: InferenceService | None = None) -> FastAPI:
    service = service or _build_default_service()
    app = FastAPI(title="inference-gateway", version="0.1.0")
    app.state.service = service

    @app.exception_handler(GatewayError)
    async def _gateway_error(_request, exc: GatewayError):
        log_event(service.log, "http_error", code=exc.code,
                  message=exc.message, details=exc.details)
        return JSONResponse(status_code=exc.http_status,
                            content={"error": {"code": exc.code,
                                               "message": exc.message,
                                               "details": exc.details}})

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @app.post("/v1/infer")
    async def infer(body: InferBody):
        request = InferenceRequest(request_id=body.request_id,
                                   caller=body.caller, alias=body.alias,
                                   input=body.input, group=body.group)
        result = await service.infer(request)
        return asdict(result)

    return app


app = create_app()
