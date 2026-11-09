"""Payment request and response models."""

from __future__ import annotations

from typing import Any

from pydantic import Field, StrictInt, field_validator

from app.core.money import MAX_AMOUNT_MINOR
from app.schemas.common import CURRENCY_RE, Model

MAX_METADATA_BYTES = 2048


class PaymentRequest(Model):
    from_account_id: str = Field(..., min_length=1, max_length=64)
    to_account_id: str = Field(..., min_length=1, max_length=64)
    amount_minor: StrictInt = Field(
        ...,
        gt=0,
        le=MAX_AMOUNT_MINOR,
        description="Integer minor units. A float here is rejected, not coerced.",
    )
    currency: str = Field(..., min_length=3, max_length=3)
    reference: str | None = Field(default=None, max_length=64)
    metadata: dict[str, Any] | None = None

    @field_validator("currency")
    @classmethod
    def _iso_code(cls, v: str) -> str:
        v = v.upper()
        if not CURRENCY_RE.match(v):
            raise ValueError("currency must be a 3-letter ISO 4217 code")
        return v

    @field_validator("metadata")
    @classmethod
    def _metadata_size(cls, v: dict | None) -> dict | None:
        if v is None:
            return v
        import json

        if len(json.dumps(v).encode("utf-8")) > MAX_METADATA_BYTES:
            raise ValueError(f"metadata exceeds {MAX_METADATA_BYTES} bytes")
        return v
