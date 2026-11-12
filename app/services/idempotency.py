"""Idempotency key handling: canonical fingerprint and the three branches.

docs/ARCHITECTURE.md §5.5. Canonicalisation is JSON with sorted keys, no
whitespace, UTF-8. The hash covers the request body only, never headers,
so a changed User-Agent does not look like a changed request.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

from sqlalchemy.orm import Session

from app.db.models.messaging import IdempotencyKey


def fingerprint(body: dict[str, Any]) -> str:
    """SHA-256 over the canonical JSON form of a request body."""
    canonical = json.dumps(
        body, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Outcome(Enum):
    NEW = "new"
    REPLAY = "replay"
    IN_PROGRESS = "in_progress"
    MISMATCH = "mismatch"


@dataclass(frozen=True)
class Lookup:
    outcome: Outcome
    record: IdempotencyKey | None = None


def inspect(session: Session, key: str, body_fingerprint: str) -> Lookup:
    """Classify a request against any existing record for this key.

    This is a pre-check only. It narrows the common cases without taking a
    lock, and the authoritative decision is the unique constraint when the
    row is inserted. Two concurrent requests with the same key can both
    pass this check; only one of them wins the INSERT.
    """
    record = session.get(IdempotencyKey, key)
    if record is None:
        return Lookup(Outcome.NEW)

    if record.fingerprint != body_fingerprint:
        return Lookup(Outcome.MISMATCH, record)

    if record.state == "completed":
        return Lookup(Outcome.REPLAY, record)

    if record.state == "in_progress":
        return Lookup(Outcome.IN_PROGRESS, record)

    # A failed attempt reserves nothing: the client may retry the same key.
    return Lookup(Outcome.NEW, record)


def reserve(
    session: Session, key: str, body_fingerprint: str
) -> IdempotencyKey:
    """Insert the key row. A unique violation here means a concurrent winner."""
    record = IdempotencyKey(
        key=key, fingerprint=body_fingerprint, state="in_progress"
    )
    session.add(record)
    session.flush()  # surfaces the unique violation now, not at commit
    return record


def complete(
    record: IdempotencyKey,
    *,
    response_body: dict,
    response_code: int,
    transaction_id: str,
) -> None:
    record.state = "completed"
    record.response_body = response_body
    record.response_code = response_code
    record.transaction_id = transaction_id
