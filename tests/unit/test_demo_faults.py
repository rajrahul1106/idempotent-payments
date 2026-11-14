"""The demo fault switch (decision D27): local only, and what each fault does."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest
from pydantic import ValidationError

from app.core.config import Settings
from app.messaging import demo_faults, topics
from app.messaging.failures import FailureKind, classify
from app.services.payment_events import PaymentPosted
from tests.conftest import ROOT

REAL_SECRET = "a-real-secret-that-is-not-the-development-placeholder"
EVENT = PaymentPosted(
    event_id="0b6f6a1e-4f1e-4a8e-9a57-6f0d7d2b2a11",
    transaction_id="pay_demo",
    from_account_id="acc_a",
    to_account_id="acc_b",
    amount_minor=100,
    currency="INR",
)


# --- the setting -------------------------------------------------------------------


def test_off_by_default():
    assert Settings(_env_file=None).demo_faults_enabled is False


@pytest.mark.parametrize("environment", ["local", "ci"])
def test_allowed_in_a_local_environment(environment):
    assert Settings(_env_file=None, environment=environment, demo_faults_enabled=True).demo_faults_enabled


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_refused_outside_a_local_environment(environment):
    with pytest.raises(ValidationError, match="DEMO_FAULTS_ENABLED must be false outside local/ci"):
        Settings(_env_file=None, environment=environment, jwt_secret=REAL_SECRET, demo_faults_enabled=True)
    # The same environment with the switch off starts normally.
    Settings(_env_file=None, environment=environment, jwt_secret=REAL_SECRET, demo_faults_enabled=False)


@pytest.mark.parametrize("module", ["app.main", "app.messaging.consumer"])
def test_a_process_refuses_to_start_with_the_switch_on_outside_local(module):
    env = {
        **os.environ,
        "ENVIRONMENT": "production",
        "JWT_SECRET": REAL_SECRET,
        "DEMO_FAULTS_ENABLED": "true",
        "OTEL_ENABLED": "false",
    }
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "DEMO_FAULTS_ENABLED must be false outside local/ci" in result.stderr


# --- what each fault does ----------------------------------------------------------


@pytest.fixture
def fault(monkeypatch):
    def _set(value):
        monkeypatch.setattr(demo_faults, "fault_for", lambda transaction_id: value)

    return _set


def test_no_fault_does_nothing(fault):
    fault(None)
    for attempt in range(4):
        demo_faults.check(EVENT, attempt, {})


def test_fail_once_fails_attempt_0_only_and_is_transient(fault):
    fault("fail_once")
    with pytest.raises(demo_faults.DemoTransientFault) as caught:
        demo_faults.check(EVENT, 0, {})
    assert classify(caught.value) is FailureKind.TRANSIENT
    demo_faults.check(EVENT, 1, {})


def test_fail_twice_fails_attempts_0_and_1(fault):
    fault("fail_twice")
    for attempt in (0, 1):
        with pytest.raises(demo_faults.DemoTransientFault):
            demo_faults.check(EVENT, attempt, {})
    demo_faults.check(EVENT, 2, {})


def test_fail_permanent_is_permanent_on_every_attempt(fault):
    fault("fail_permanent")
    for attempt in (0, 1, 5):
        with pytest.raises(demo_faults.DemoPermanentFault) as caught:
            demo_faults.check(EVENT, attempt, {})
        assert classify(caught.value) is FailureKind.PERMANENT


@pytest.mark.parametrize("value", ["fail_once", "fail_twice", "fail_permanent"])
def test_an_operator_replay_is_never_faulted(fault, value):
    fault(value)
    demo_faults.check(EVENT, 0, {topics.HEADER_REPLAY_OF: "42"})
