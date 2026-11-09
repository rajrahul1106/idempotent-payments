"""Shared request and response models.

`StrictInt` on every monetary field is what stops a float reaching the
service layer. Pydantic would otherwise coerce 200000.0 to 200000, which
silently accepts exactly the input this system refuses to trust.
"""

from __future__ import annotations

import datetime as dt
import re

from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from app.core.money import MAX_AMOUNT_MINOR

CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AmountField(Model):
    amount_minor: StrictInt = Field(
        ..., gt=0, le=MAX_AMOUNT_MINOR, description="Integer minor units, e.g. paise"
    )
    currency: str = Field(..., min_length=3, max_length=3)

    @field_validator("currency")
    @classmethod
    def _upper_iso(cls, v: str) -> str:
        v = v.upper()
        if not CURRENCY_RE.match(v):
            raise ValueError("currency must be a 3-letter ISO 4217 code")
        return v


class AccountCreate(Model):
    holder_name: str = Field(..., min_length=1, max_length=120)
    currency: str = Field(..., min_length=3, max_length=3)
    account_type: str = Field(default="customer")

    @field_validator("currency")
    @classmethod
    def _upper_iso(cls, v: str) -> str:
        v = v.upper()
        if not CURRENCY_RE.match(v):
            raise ValueError("currency must be a 3-letter ISO 4217 code")
        return v

    @field_validator("account_type")
    @classmethod
    def _known_type(cls, v: str) -> str:
        if v not in ("customer", "system"):
            raise ValueError("account_type must be 'customer' or 'system'")
        return v


class AccountOut(Model):
    id: str
    holder_name: str
    currency: str
    account_type: str
    status: str
    balance: str = Field(..., description="Derived from postings, scale 4")
    balance_minor: int
    created_at: dt.datetime


class PostingOut(Model):
    id: int
    transaction_id: str
    event_id: str
    amount: str
    currency: str
    created_at: dt.datetime


class TokenRequest(Model):
    username: str
    password: str


class TokenOut(Model):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    role: str
