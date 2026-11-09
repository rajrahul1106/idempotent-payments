"""Accounts, transactions and postings: the money tables.

Design rules from the blueprint:
  - `accounts` has no balance column. Balance is SUM(postings.amount).
  - `postings` is append-only, enforced by database rules in the migration.
  - Every monetary column is NUMERIC(20,4), mapped to Decimal in Python.
"""

from __future__ import annotations

import datetime as dt
import decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.models.base import Base

MONEY = Numeric(20, 4, asdecimal=True)

ACCOUNT_STATUS = Enum(
    "active", "frozen", "closed", name="account_status", create_type=False
)
ACCOUNT_TYPE = Enum("customer", "system", name="account_type", create_type=False)
TX_STATUS = Enum("posted", "reversed", name="tx_status", create_type=False)


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    holder_name: Mapped[str] = mapped_column(Text, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    account_type: Mapped[str] = mapped_column(
        ACCOUNT_TYPE, nullable=False, server_default="customer"
    )
    status: Mapped[str] = mapped_column(
        ACCOUNT_STATUS, nullable=False, server_default="active"
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    postings: Mapped[list["Posting"]] = relationship(back_populates="account")

    @property
    def allows_negative_balance(self) -> bool:
        """System accounts are the counterparty for external deposits.

        A deposit from outside the system must still produce two postings
        that sum to zero, so the system account legitimately runs negative.
        Blueprint section 4.
        """
        return self.account_type == "system"


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    status: Mapped[str] = mapped_column(
        TX_STATUS, nullable=False, server_default="posted"
    )
    amount: Mapped[decimal.Decimal] = mapped_column(MONEY, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    reference: Mapped[str | None] = mapped_column(String(64))
    metadata_json: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default="{}"
    )
    reverses_transaction_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("transactions.id")
    )
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_transactions_amount_positive"),
    )

    postings: Mapped[list["Posting"]] = relationship(back_populates="transaction")


class Posting(Base):
    """One leg of a transaction. Append-only, never updated or deleted."""

    __tablename__ = "postings"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(
        Text, ForeignKey("transactions.id"), nullable=False
    )
    account_id: Mapped[str] = mapped_column(
        Text, ForeignKey("accounts.id"), nullable=False
    )
    event_id: Mapped[str] = mapped_column(UUID(as_uuid=False), nullable=False)
    amount: Mapped[decimal.Decimal] = mapped_column(MONEY, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("amount <> 0", name="ck_postings_amount_nonzero"),
        Index("ix_postings_account_created", "account_id", "created_at"),
        Index("ix_postings_transaction", "transaction_id"),
        Index("ix_postings_event", "event_id"),
    )

    account: Mapped[Account] = relationship(back_populates="postings")
    transaction: Mapped[Transaction] = relationship(back_populates="postings")
