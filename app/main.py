"""FastAPI application assembly."""

from __future__ import annotations

import logging
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.errors import register_error_handlers
from app.core.config import settings
from app.core.telemetry import setup_tracing, shutdown_tracing
from app.db.session import ping

logging.basicConfig(level=settings.log_level)

# The api process's tracer provider. A no-op if one is already installed
# (the test suite installs an in-memory one before importing this module).
setup_tracing("api")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    yield
    # Shutdown. The DLQ replay producer is created on the first replay and
    # lives as long as the process; flush and close it so a replay that was
    # acknowledged-in-flight is not dropped by an abrupt exit.
    from app.api.routes.dlq import close_replay_producer

    close_replay_producer()
    shutdown_tracing()


app = FastAPI(
    lifespan=lifespan,
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


def _instrument() -> None:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    # One server span per request. Probes are not traced. Request and
    # response headers and bodies are not captured (the instrumentation's
    # default), so no bearer token or request body reaches a span.
    FastAPIInstrumentor.instrument_app(app, excluded_urls="health,ready,metrics")


_instrument()
