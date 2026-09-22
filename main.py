from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from gateway.config import build_default_gateway
from gateway.errors import GatewayError
from gateway.gateway import InferenceGateway
from gateway.models import ErrorBody, ErrorResponse, InferRequest


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
        return gateway.stats

    return app


app = create_app(build_default_gateway())
