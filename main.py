import dataclasses

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from gateway.config import build_default_gateway
from gateway.errors import GatewayError
from gateway.gateway import InferenceGateway
from gateway.models import ErrorBody, ErrorResponse, InferRequest


class PurgeBody(BaseModel):
    model: str | None = Field(default=None, description="按模型别名定向清理复用记录")
    version: str | None = Field(default=None, description="按实际版本定向清理复用记录")


class RetentionBody(BaseModel):
    reuse_ttl_seconds: float | None = Field(default=None, gt=0)
    replay_ttl_seconds: float | None = Field(default=None, gt=0)


def create_app(gateway: InferenceGateway) -> FastAPI:
    app = FastAPI(title="inference-gateway")

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, exc: GatewayError) -> JSONResponse:
        body = ErrorResponse(error=ErrorBody(code=exc.code, message=exc.message, detail=exc.detail))
        return JSONResponse(status_code=exc.http_status, content=body.model_dump())

    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.post("/v1/infer", response_model=None)
    async def infer(request: InferRequest):
        return await gateway.infer(request)

    @app.get("/v1/stats")
    async def stats():
        body = dataclasses.asdict(gateway.stats)
        body["governance"] = gateway.governance_snapshot()
        return body

    @app.post("/v1/admin/governance/purge", response_model=None)
    async def purge_reuse(body: PurgeBody):
        # 只清理复用记录；重放承诺在保留期内不可改写，正在执行的请求不受影响。
        removed = await gateway.purge_reuse(model=body.model, version=body.version)
        return {"purged_reuse_records": removed, "governance": gateway.governance_snapshot()}

    @app.post("/v1/admin/governance/retention", response_model=None)
    async def set_retention(body: RetentionBody):
        # 缩短保留期只影响之后新写入的结论/记录，不回溯改写已承诺内容。
        if gateway.replay_store is not None and body.replay_ttl_seconds is not None:
            gateway.replay_store.set_retention(body.replay_ttl_seconds)
        if gateway.reuse_store is not None and body.reuse_ttl_seconds is not None:
            gateway.reuse_store.set_retention(body.reuse_ttl_seconds)
        return {
            "replay_ttl_seconds": gateway.replay_store.retention_seconds
            if gateway.replay_store is not None else None,
            "reuse_ttl_seconds": gateway.reuse_store.retention_seconds
            if gateway.reuse_store is not None else None,
        }

    return app


app = create_app(build_default_gateway())
