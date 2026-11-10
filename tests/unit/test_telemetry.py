"""Trace-context helpers and the sampler, before any I/O."""

from __future__ import annotations

import hashlib

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.sampling import Decision
from opentelemetry.trace import NonRecordingSpan, SpanContext, SpanKind, TraceFlags

from app.core.telemetry import (
    SkipOrphanDatabaseSpans,
    context_from,
    key_digest,
    trace_id_of,
)

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
TRACEPARENT = f"00-{TRACE_ID}-00f067aa0ba902b7-01"


def test_key_digest_is_a_sha256_prefix_never_the_key():
    key = "6f1c2a8e-3b4d-4c5e-9f60-7a8b9c0d1e2f"
    digest = key_digest(key)
    assert digest == hashlib.sha256(key.encode()).hexdigest()[:16]
    assert key not in digest and len(digest) == 16


def test_traceparent_round_trip():
    assert trace_id_of(TRACEPARENT) == TRACE_ID


@pytest.mark.parametrize("bad", [None, "", "garbage", "00-" + "0" * 32 + "-00f067aa0ba902b7-01"])
def test_missing_or_invalid_traceparent_starts_a_new_trace(bad):
    assert context_from(bad) is None
    assert trace_id_of(bad) is None


def _parent_context():
    span_context = SpanContext(
        trace_id=int(TRACE_ID, 16), span_id=0x00F067AA0BA902B7, is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return trace.set_span_in_context(NonRecordingSpan(span_context))


@pytest.mark.parametrize(
    "kind, parent, sampled",
    [
        (SpanKind.CLIENT, None, False),  # a poll query outside any operation
        (SpanKind.CLIENT, "parent", True),  # a query inside a traced operation
        (SpanKind.SERVER, None, True),  # an HTTP request starts a trace
        (SpanKind.CONSUMER, None, True),  # a message with no traceparent
        (SpanKind.INTERNAL, None, True),
    ],
)
def test_sampler_drops_only_orphan_database_spans(kind, parent, sampled):
    context = _parent_context() if parent else None
    result = SkipOrphanDatabaseSpans().should_sample(context, int(TRACE_ID, 16), "SELECT", kind)
    assert (result.decision is Decision.RECORD_AND_SAMPLE) is sampled
