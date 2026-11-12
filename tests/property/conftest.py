"""The property tests reuse the messaging fixtures (a per-run topic ladder)."""

from tests.messaging.conftest import make_ladder  # noqa: F401 - registers the fixture
