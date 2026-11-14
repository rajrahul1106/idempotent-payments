"""delivery_failures: one row per failed attempt the consumer forwarded.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-15

The demo page shows a payment's retries as they happen (decision D27). Until
now the only record of a failed attempt was in Kafka headers, which reach
the database only if the message ends in the DLQ. The main consumer now
writes one row each time it forwards a failure to a retry tier or the DLQ,
in its own short transaction, after the offset commit, best-effort: a failed
insert is logged and never changes what happens to the message.

event_id is NULL when the message carried no usable event id (a malformed
message is still forwarded, and still recorded).
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "delivery_failures",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=False), nullable=True),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=False),
        sa.Column("sent_to", sa.Text(), nullable=False),
        sa.Column(
            "failed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("kind IN ('transient', 'permanent')", name="ck_delivery_failures_kind"),
        sa.CheckConstraint("attempt > 0", name="ck_delivery_failures_attempt"),
    )
    op.create_index("ix_delivery_failures_event", "delivery_failures", ["event_id", "id"])


def downgrade() -> None:
    op.drop_index("ix_delivery_failures_event", table_name="delivery_failures")
    op.drop_table("delivery_failures")
