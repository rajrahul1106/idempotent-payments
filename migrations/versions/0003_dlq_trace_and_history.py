"""dlq_messages keeps the trace context and the full failure history.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-11

Phase 7a, approved schema change. A dead letter used to keep only the last
error and an attempt count (§15, limitation 8). Now it keeps:

- traceparent: the W3C trace context of the attempt that sent it to the
  DLQ, so the operator can open its trace and a replay continues it.
- failure_history: one entry per failed attempt, {attempt, topic, error,
  at}, carried through the tiers in the failure_history header.

Existing rows get an empty history: what they lost was never recorded.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("dlq_messages", sa.Column("traceparent", sa.Text()))
    op.add_column(
        "dlq_messages",
        sa.Column(
            "failure_history",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )


def downgrade() -> None:
    op.drop_column("dlq_messages", "failure_history")
    op.drop_column("dlq_messages", "traceparent")
