"""Idempotency keys, outbox, processed events, DLQ and reconciliation.

These are the correctness tables. Apart from the payment_events projection,
none of them hold money; all of them exist to make sure money moves
exactly once.
"""

from __future__ import annotations

import datetime as dt
import decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models.base import Base
from app.db.models.ledger import MONEY


class IdempotencyKey(Base):
    """One row per client payment intent.

    `fingerprint` is SHA-256 over the canonical request body. A repeat with
    a matching fingerprint replays the stored response; a repeat with a
    different fingerprint is a client bug and returns 422.
    """

    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    response_body: Mapped[dict | None] = mapped_column(JSONB)
    response_code: Mapped[int | None] = mapped_column(Integer)
    transaction_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("transactions.id")
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now() + interval '24 hours'"),
    )

    __table_args__ = (
        CheckConstraint(
            "state IN ('in_progress', 'completed', 'failed')",
            name="ck_idempotency_state",
        ),
        Index("ix_idempotency_expires", "expires_at"),
    )


class OutboxEvent(Base):
    """An event committed with the business rows, published later by the relay.

    `published_at` stays NULL until the relay has an acknowledgement from
    the broker. The partial index means the relay only ever scans the
    backlog, not the full history.
    """

    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), nullable=False, unique=True
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="1"
    )
    partition_key: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    traceparent: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index("ix_outbox_created", "created_at"),
    )


class ProcessedEvent(Base):
    """The consumer's deduplication record.

    Written in the same transaction as the side effect. The composite
    primary key scopes dedup per consumer group, so a second consumer
    group can process the same events independently.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    consumer_group: Mapped[str] = mapped_column(Text, primary_key=True)
    processed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PaymentEventProjection(Base):
    """The consumer's side effect: a read model of applied payments."""

    __tablename__ = "payment_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(
        UUID(as_uuid=False), nullable=False, unique=True
    )
    transaction_id: Mapped[str] = mapped_column(Text, nullable=False)
    from_account_id: Mapped[str] = mapped_column(Text, nullable=False)
    to_account_id: Mapped[str] = mapped_column(Text, nullable=False)
    # NUMERIC(20,4), the same representation as postings.amount (migration
    # 0002). The consumer converts the event's amount_minor with
    # app.core.money.to_decimal, the only converter.
    amount: Mapped[decimal.Decimal] = mapped_column(MONEY, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    applied_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DlqMessage(Base):
    """A message that exhausted every retry tier."""

    __tablename__ = "dlq_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(UUID(as_uuid=False), nullable=False)
    original_topic: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    first_failed_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    replayed_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        Index(
            "ix_dlq_open",
            "event_id",
            postgresql_where=text("replayed_at IS NULL"),
        ),
    )


class ReconciliationRun(Base):
    """Result of one invariant check."""

    __tablename__ = "reconciliation_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    check_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    detail: Mapped[dict | None] = mapped_column(JSONB)
    ran_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('pass', 'warn', 'fail')", name="ck_reconciliation_status"
        ),
        Index("ix_reconciliation_ran_at", "ran_at"),
    )


class AuthUser(Base):
    """Seeded users for the local JWT issuer."""

    __tablename__ = "auth_users"

    username: Mapped[str] = mapped_column(Text, primary_key=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    account_id: Mapped[str | None] = mapped_column(Text, ForeignKey("accounts.id"))
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "role IN ('viewer', 'client', 'operator')", name="ck_auth_role"
        ),
    )
