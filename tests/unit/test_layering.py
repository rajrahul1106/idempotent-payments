"""docs/ARCHITECTURE.md §12 layering, checked rather than trusted.

Services and messaging must not depend on the web layer. The Phase 5
consumer will import the services, and it must not drag FastAPI and
app.api in with them. Each import runs in a fresh interpreter so the
modules this test process already loaded cannot hide a violation.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]

_PROBE = """
import importlib, sys
importlib.import_module(sys.argv[1])
web = sorted(
    name for name in sys.modules
    if name.split(".")[0] in ("fastapi", "starlette") or name.startswith("app.api")
)
print(",".join(web))
"""


@pytest.mark.parametrize(
    "module",
    [
        "app.services.payment",
        "app.services.idempotency",
        "app.messaging.relay",
        "app.messaging.consumer",
        "app.messaging.topic_setup",
        "app.services.payment_events",
        "app.messaging.retry_tier",
        "app.messaging.dlq_writer",
        "app.messaging.failures",
        "app.services.dlq",
        "app.db.errors",
        "app.core.telemetry",
        "app.services.reconciliation",
        "app.core.metrics",
        "app.core.healthcheck",
        "app.messaging.lag",
        "app.core.exceptions",
    ],
)
def test_module_does_not_load_the_web_layer(module):
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, module],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    loaded = result.stdout.strip()
    assert loaded == "", f"{module} loads the web layer: {loaded}"
