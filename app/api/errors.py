"""One error body shape for every endpoint.

Blueprint section 7: every error returns
{"error": {"code", "message", "request_id", "trace_id"}}.
"""

from __future__ import annotations

import logging
import uuid

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import OperationalError
from starlette.exceptions import HTTPException as StarletteHTTPException

# Domain exceptions live in app.core so services never import the web
# framework. This module only maps them onto the error body.
from app.core.exceptions import DomainError
from app.db.errors import database_unavailable

logger = logging.getLogger(__name__)


def _current_trace_id() -> str:
    """Trace id if OpenTelemetry is wired up (Phase 7), empty string before."""
    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        ctx = span.get_span_context()
        if ctx.is_valid:
            return format(ctx.trace_id, "032x")
    except Exception:
        pass
    return ""


def _body(code: str, message: str, request: Request) -> dict:
    return {
        "error": {
            "code": code,
            "message": message,
            "request_id": getattr(request.state, "request_id", str(uuid.uuid4())),
            "trace_id": _current_trace_id(),
        }
    }


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=_body(exc.code, exc.message, request),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        detail = exc.errors()
        first = detail[0] if detail else {}
        location = ".".join(str(p) for p in first.get("loc", [])[1:])
        message = f"{location}: {first.get('msg', 'invalid request')}".lstrip(": ")
        return JSONResponse(
            status_code=400, content=_body("invalid_request", message, request)
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        codes = {401: "unauthenticated", 403: "forbidden", 404: "not_found",
                 429: "rate_limited"}
        return JSONResponse(
            status_code=exc.status_code,
            content=_body(
                codes.get(exc.status_code, "invalid_request"),
                str(exc.detail),
                request,
            ),
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(OperationalError)
    async def _database(request: Request, exc: OperationalError) -> JSONResponse:
        if database_unavailable(exc):
            logger.error(
                "database unavailable on %s %s: %s",
                request.method,
                request.url.path,
                exc.orig,
            )
            return JSONResponse(
                status_code=503,
                content=_body(
                    "service_unavailable",
                    "The database is unavailable; retry later",
                    request,
                ),
            )
        logger.exception(
            "unhandled database error on %s %s", request.method, request.url.path
        )
        return JSONResponse(
            status_code=500,
            content=_body("internal_error", "An unexpected error occurred", request),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content=_body("internal_error", "An unexpected error occurred", request),
        )
