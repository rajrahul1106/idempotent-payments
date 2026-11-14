"""FastAPI application assembly."""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.api.body_limit import MAX_BODY_BYTES, BodySizeLimit
from app.api.errors import register_error_handlers
from app.api.routes import accounts, admin, auth, dlq, payments
from app.core import metrics
from app.core.config import settings
from app.core.telemetry import setup_tracing, shutdown_tracing
from app.db.session import ping

logging.basicConfig(level=settings.log_level)

# The api process's tracer provider. A no-op if one is already installed
# (the test suite installs an in-memory one before importing this module).
setup_tracing("api")
metrics.register("api")


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
async def time_request(request: Request, call_next):
    """payment_duration_seconds{endpoint}: the route template, never the
    raw path, so /v1/accounts/{account_id} is one series, not one per id."""
    started = time.perf_counter()
    try:
        return await call_next(request)
    finally:
        metrics.REQUEST_DURATION.labels(endpoint=_route_template(request)).observe(
            time.perf_counter() - started
        )


def _route_template(request: Request) -> str:
    """The full route template, /v1 prefix included, e.g. /v1/accounts/{account_id}.

    This FastAPI version mounts included routers, so scope["route"].path lacks
    the prefix. The OpenTelemetry instrumentation's resolver gets it right, and
    using it keeps this label identical to the span's http.route. It is a
    private helper of a pinned version; tests/api/test_metrics.py fails if it
    changes behaviour.
    """
    from opentelemetry.instrumentation.fastapi import _get_route_details

    try:
        return _get_route_details(request.scope) or "unmatched"
    except Exception:  # pragma: no cover - never let a label break a request
        return "unmatched"


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


@app.get("/metrics", tags=["ops"], include_in_schema=False)
def prometheus_metrics() -> Response:
    """This process's metrics, for Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/ready", tags=["ops"])
async def ready() -> JSONResponse:
    """Readiness. Fails when a dependency the API needs is unreachable."""
    db_ok = ping()
    payload = {"status": "ok" if db_ok else "degraded", "checks": {"database": db_ok}}
    return JSONResponse(status_code=200 if db_ok else 503, content=payload)


DEMO_PAGE = Path(__file__).resolve().parent / "static" / "demo.html"


@app.get("/demo", include_in_schema=False)
def demo_page() -> FileResponse:
    """The demo page: one static file with no data and no secrets in it.
    Everything it shows comes from authenticated /v1 calls it makes itself,
    so it needs no auth of its own and is not rate limited."""
    return FileResponse(DEMO_PAGE, media_type="text/html", headers={"Cache-Control": "no-store"})


# Imported directly, not discovered: a router that fails to import must stop
# the process at startup, never leave the API running with its endpoints
# missing.
for _router in (auth.router, accounts.router, payments.router, dlq.router, admin.router):
    app.include_router(_router, prefix="/v1")


# Outermost but for the tracing middleware added below, so a refused body is
# still traced and its error carries the trace id.
app.add_middleware(BodySizeLimit, max_bytes=MAX_BODY_BYTES)


def _instrument() -> None:
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    # One server span per request. Probes are not traced. Request and
    # response headers and bodies are not captured (the instrumentation's
    # default), so no bearer token or request body reaches a span.
    FastAPIInstrumentor.instrument_app(app, excluded_urls="health,ready,metrics,demo")


_instrument()
