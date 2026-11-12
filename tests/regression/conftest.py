"""Regression tests replay shrunk state-machine sequences: they need its
fixtures (a per-run topic ladder, one consumer group)."""

from tests.messaging.conftest import make_ladder  # noqa: F401 - registers the fixture
from tests.property.test_ledger_machine import machine_setup  # noqa: F401
