"""Span capture for the test suite.

The test process is the api: tests/conftest.py installs its tracer provider
(service.name "api") with SPANS as the exporter, before the app is
imported, and app.main's own setup_tracing is then a no-op. Spans are kept
only while a test holds the `spans` fixture.

Processes started by a test (a relay, a consumer) write their spans as JSON
lines with `file_exporter`, so a test can follow one trace across real
process boundaries. Spans from both sources are compared as the same dicts:
ReadableSpan.to_json(), parsed.
"""

from __future__ import annotations

import json
import os
import pathlib
import threading

from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor
from opentelemetry.sdk.trace.export import (
    ConsoleSpanExporter,
    SpanExporter,
    SpanExportResult,
)


class SpanCapture(SpanExporter):
    def __init__(self) -> None:
        self._spans: list[ReadableSpan] = []
        self._lock = threading.Lock()
        self.recording = False
        #: When True, every export raises, like an exporter whose backend is down.
        self.fail = False

    def export(self, spans) -> SpanExportResult:
        if self.fail:
            raise ConnectionError("injected span exporter failure")
        if self.recording:
            with self._lock:
                self._spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass

    def clear(self) -> None:
        with self._lock:
            self._spans.clear()

    def dicts(self) -> list[dict]:
        with self._lock:
            return [json.loads(span.to_json(indent=None)) for span in self._spans]


class SwitchableProcessor(SpanProcessor):
    """A slot for one extra span processor, filled and emptied by a test."""

    def __init__(self) -> None:
        self.inner: SpanProcessor | None = None

    def on_start(self, span, parent_context=None) -> None:
        if self.inner is not None:
            self.inner.on_start(span, parent_context)

    def on_end(self, span) -> None:
        if self.inner is not None:
            self.inner.on_end(span)

    def shutdown(self) -> None:
        if self.inner is not None:
            self.inner.shutdown()

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self.inner.force_flush(timeout_millis) if self.inner is not None else True


SPANS = SpanCapture()
EXTRA = SwitchableProcessor()


def file_exporter(path: str) -> ConsoleSpanExporter:
    """For a child process: one JSON span per line, written as each ends."""
    out = open(path, "w", buffering=1)  # noqa: SIM115 - closed at process exit
    return ConsoleSpanExporter(out=out, formatter=lambda s: s.to_json(indent=None) + os.linesep)


def read_spans(path: str | pathlib.Path) -> list[dict]:
    lines = pathlib.Path(path).read_text().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


# --- reading span dicts ------------------------------------------------------------


def trace_id(span: dict) -> str:
    return span["context"]["trace_id"].removeprefix("0x")


def span_id(span: dict) -> str:
    return span["context"]["span_id"].removeprefix("0x")


def parent_id(span: dict) -> str | None:
    parent = span.get("parent_id")
    return parent.removeprefix("0x") if parent else None


def service(span: dict) -> str:
    return span["resource"]["attributes"]["service.name"]


def in_trace(spans: list[dict], trace: str) -> list[dict]:
    return [s for s in spans if trace_id(s) == trace]


def named(spans: list[dict], name: str) -> list[dict]:
    return sorted((s for s in spans if s["name"] == name), key=lambda s: s["start_time"])
