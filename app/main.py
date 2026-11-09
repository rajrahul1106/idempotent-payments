"""FastAPI application assembly."""

from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.errors import register_error_handlers
from app.core.config import settings
from app.db.session import ping

logging.basicConfig(level=settings.log_level)

app = FastAPI(
    title="Idempotent Payment Event Processing System",
    version="0.1.0",
    description=(
        "Accepts payment instructions and applies each one to a double-entry "
        "ledger exactly once, under client retries, broker redelivery and "
        "consumer crashes."
    ),
)

register_error_handlers(app)


@app.middleware("http")
async def attach_request_id(request: Request, call_next):
    request.state.request_id = request.headers.get(
        "X-Request-Id", f"req_{uuid.uuid4().hex[:16]}"
    )
    response = await call_next(request)
    response.headers["X-Request-Id"] = request.state.request_id
    return response


@app.get("/health", tags=["ops"])
async def health() -> dict:
    """Liveness. Answers as long as the process is up."""
    return {"status": "ok", "service": "payments-api", "version": app.version}


@app.get("/ready", tags=["ops"])
async def ready() -> JSONResponse:
    """Readiness. Fails when a dependency the API needs is unreachable."""
    db_ok = ping()
    payload = {"status": "ok" if db_ok else "degraded", "checks": {"database": db_ok}}
    return JSONResponse(status_code=200 if db_ok else 503, content=payload)


def _register_routers() -> None:
    """Routers are added phase by phase. Missing ones are not an error yet."""
    from importlib import import_module

    for module_name, prefix in (
        ("app.api.routes.auth", "/v1"),
        ("app.api.routes.accounts", "/v1"),
        ("app.api.routes.payments", "/v1"),
        ("app.api.routes.dlq", "/v1"),
        ("app.api.routes.admin", "/v1"),
    ):
        try:
            module = import_module(module_name)
        except ModuleNotFoundError:
            continue
        app.include_router(module.router, prefix=prefix)


_register_routers()
