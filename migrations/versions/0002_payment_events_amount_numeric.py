"""payment_events money as NUMERIC(20,4).

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-10

Blueprint section 5.2: money in Postgres is NUMERIC(20,4). 0001 created
payment_events.amount_minor as BIGINT minor units, a second representation
of money in the database. This makes it NUMERIC(20,4), the same as
postings.amount, and renames it to amount because it no longer holds minor
units.

Nothing writes payment_events before the Phase 5 consumer, so the table is
empty. Converting rows would need each currency's minor-unit exponent, and
app/core/money.py is the only place allowed to do that conversion. Rather
than convert in SQL, both directions refuse to run on a non-empty table.
"""

from alembic import op
import sqlalchemy as sa

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def _require_empty_table() -> None:
    rows = op.get_bind().execute(sa.text("SELECT count(*) FROM payment_events")).scalar_one()
    if rows:
        raise RuntimeError(
            f"payment_events has {rows} row(s). Converting between minor units "
            "and NUMERIC needs app/core/money.py, so this migration only runs "
            "on an empty table."
        )


def upgrade() -> None:
    _require_empty_table()
    op.alter_column(
        "payment_events",
        "amount_minor",
        new_column_name="amount",
        type_=sa.Numeric(20, 4),
        existing_type=sa.BigInteger(),
        existing_nullable=False,
    )


def downgrade() -> None:
    _require_empty_table()
    op.alter_column(
        "payment_events",
        "amount",
        new_column_name="amount_minor",
        type_=sa.BigInteger(),
        existing_type=sa.Numeric(20, 4),
        existing_nullable=False,
    )
