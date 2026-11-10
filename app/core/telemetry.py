"""OpenTelemetry setup and trace-context helpers (blueprint §5.8).

One TracerProvider per process, named for the process: api, relay,
consumer, retry-5s, retry-1m, retry-10m, dlq-writer, reconciliation. Spans
go to the Compose Jaeger over OTLP through a BatchSpanProcessor, which
exports on its own thread. An exporter that is down, slow or failing only
drops spans; it never fails or blocks the operation being traced.

One payment is one trace. The W3C traceparent of the API request is stored
on the outbox row, and every hop after that (relay, consumer, each retry
tier, the DLQ writer, an operator replay) starts its span as a child of the
traceparent it received and passes its own span's traceparent on. The
trace id never changes.

Attributes are chosen so a trace can be shared safely: event_id,
account_id, attempt_count and a SHA-256 prefix of the idempotency key.
Never the raw key, a token, or a request body.
"""

from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Sequence

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_ON,
    Decision,
    ParentBased,
    Sampler,
    SamplingResult,
)
from opentelemetry.trace import SpanKind
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from app.core.config import settings

logger = logging.getLogger(__name__)

TRACEPARENT = "traceparent"
_propagator = TraceContextTextMapPropagator()
_provider: TracerProvider | None = None
_lock = threading.Lock()


class SkipOrphanDatabaseSpans(Sampler):
    """Drop a database span that has no parent; keep everything else.

    The relay polls every 200 ms and the consumers check their locks and
    claims between messages. Traced on their own, those queries would be a
    one-span trace each, several per second, burying the payment traces in
    Jaeger. A query inside a traced operation (a request, a publish, a
    message being processed) has a parent and is kept.

    SQLAlchemy's spans are the only CLIENT spans this system starts, and
    their attributes are set after sampling, so the kind is what is tested.
    """

    _inner = ParentBased(ALWAYS_ON)

    def should_sample(
        self,
        parent_context: Context | None,
        trace_id: int,
        name: str,
        kind: SpanKind | None = None,
        attributes=None,
        links: Sequence | None = None,
        trace_state=None,
    ) -> SamplingResult:
        parent = trace.get_current_span(parent_context).get_span_context()
        if not parent.is_valid and kind is SpanKind.CLIENT:
            return SamplingResult(Decision.DROP)
        return self._inner.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )

    def get_description(self) -> str:
        return "SkipOrphanDatabaseSpans"


def setup_tracing(
    service_name: str,
    *,
    exporter: SpanExporter | None = None,
    synchronous: bool = False,
) -> TracerProvider:
    """Install this process's tracer provider. The first call wins.

    With no exporter, spans go to OTLP at settings.otel_exporter_otlp_endpoint
    (unless OTEL_ENABLED is false). Tests pass their own exporter, and
    `synchronous` so a span is exported the moment it ends.
    """
    global _provider
    with _lock:
        if _provider is not None:
            return _provider
        provider = TracerProvider(
            resource=Resource.create({SERVICE_NAME: service_name}),
            sampler=SkipOrphanDatabaseSpans(),
        )
        if exporter is None and settings.otel_enabled:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            exporter = OTLPSpanExporter(
                endpoint=settings.otel_exporter_otlp_endpoint,
                insecure=True,
                timeout=settings.otel_export_timeout_seconds,
            )
        if exporter is not None:
            processor = (
                SimpleSpanProcessor(exporter) if synchronous else BatchSpanProcessor(exporter)
            )
            provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        _instrument_sqlalchemy(provider)
        _provider = provider
        logger.info(
            "tracing: service=%s exporter=%s",
            service_name,
            type(exporter).__name__ if exporter is not None else "none",
        )
        return provider


def _instrument_sqlalchemy(provider: TracerProvider) -> None:
    """Trace every statement on this process's engine.

    EngineTracer, not SQLAlchemyInstrumentor().instrument(): the instrumentor
    0.66b2 declares "sqlalchemy < 2.1.0" and silently does nothing on our
    pinned 2.1.8; forcing it past that check makes it patch
    sqlalchemy.ext.asyncio, which needs greenlet, which this project does not
    use. EngineTracer hooks one engine's cursor-execute events, which 2.1
    keeps, and patches nothing globally. tests/api/test_tracing.py fails if
    the database spans disappear.

    Statements are recorded with their placeholders, never the bound
    values, so no key, token or amount reaches a span from here.
    """
    from opentelemetry.instrumentation.sqlalchemy import engine as sqlalchemy_tracing
    from opentelemetry.metrics import NoOpMeter

    from app.db.session import engine

    sqlalchemy_tracing.EngineTracer(
        trace.get_tracer("opentelemetry.instrumentation.sqlalchemy", tracer_provider=provider),
        engine,
        # Connection-pool metrics are Phase 7b; until then a no-op counter.
        NoOpMeter("app.core.telemetry").create_up_down_counter("db.client.connections.usage"),
    )


def shutdown_tracing() -> None:
    """Export what is buffered. Safe to call when tracing was never set up."""
    if _provider is not None:
        _provider.shutdown()


def tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name)


# --- trace context ------------------------------------------------------------


def current_traceparent() -> str | None:
    """The W3C traceparent of the current span, or None outside a trace."""
    carrier: dict[str, str] = {}
    _propagator.inject(carrier)
    return carrier.get(TRACEPARENT)


def context_from(traceparent: str | None) -> Context | None:
    """A parent context for a span, from a stored or received traceparent.

    None for a missing or malformed value: the span then starts a new trace
    rather than failing the message.
    """
    if not traceparent:
        return None
    context = _propagator.extract({TRACEPARENT: traceparent})
    if not trace.get_current_span(context).get_span_context().is_valid:
        return None
    return context


def trace_id_of(traceparent: str | None) -> str | None:
    context = context_from(traceparent)
    if context is None:
        return None
    return format(trace.get_current_span(context).get_span_context().trace_id, "032x")


def current_trace_id() -> str:
    """The current trace id as 32 hex digits, or "" outside a trace."""
    span_context = trace.get_current_span().get_span_context()
    return format(span_context.trace_id, "032x") if span_context.is_valid else ""


def key_digest(idempotency_key: str) -> str:
    """What a span may carry instead of an idempotency key: a SHA-256 prefix."""
    return hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:16]
