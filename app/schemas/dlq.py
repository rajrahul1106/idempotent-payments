"""DLQ responses (blueprint §7)."""

from __future__ import annotations

import datetime as dt
from typing import Any

from pydantic import Field

from app.schemas.common import Model


class DlqMessageOut(Model):
    id: int
    event_id: str
    original_topic: str
    reason: str = Field(..., description="The last error: '<kind>: <ExceptionClass>: <message>'")
    attempts: int = Field(..., description="Failed processing attempts before quarantine")
    first_failed_at: dt.datetime
    created_at: dt.datetime
    replayed_at: dt.datetime | None
    payload: Any = Field(
        ...,
        description=(
            "The message body as stored. Non-integer numbers appear as strings, "
            "since a JSON float would be read as binary floating point."
        ),
    )


class DlqPage(Model):
    items: list[DlqMessageOut]
    next_after_id: int | None = Field(
        ..., description="Pass as after_id for the next page; null on the last page"
    )
