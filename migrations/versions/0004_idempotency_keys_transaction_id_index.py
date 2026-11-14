"""Index idempotency_keys.transaction_id for the journey view.

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-14

GET /v1/payments/{id}/journey finds a payment's idempotency key by
transaction_id to date its "request received" stage. With no index that was
a sequential scan of every key (697,170 rows on the measurement database).
An index only: no column, constraint or data changes.

Built CONCURRENTLY, outside the migration's transaction, so it does not
block payments (which insert into this table) while it builds.
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None

INDEX = "ix_idempotency_transaction_id"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            INDEX,
            "idempotency_keys",
            ["transaction_id"],
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            INDEX,
            table_name="idempotency_keys",
            postgresql_concurrently=True,
            if_exists=True,
        )
