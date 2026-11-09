"""Initial schema: ledger, idempotency, outbox, dedup, DLQ, reconciliation.

Revision ID: 0001
Revises:
Create Date: 2026-11-10
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


# The deferred constraint trigger is the single most important object in
# this schema. It means an unbalanced transaction cannot commit, whatever
# the application code does. DEFERRABLE INITIALLY DEFERRED lets both legs
# be inserted before the check runs at COMMIT.
BALANCE_TRIGGER_FN = """
CREATE OR REPLACE FUNCTION assert_balanced_transaction() RETURNS TRIGGER AS $$
DECLARE
    total NUMERIC(20,4);
BEGIN
    SELECT COALESCE(SUM(amount), 0) INTO total
      FROM postings
     WHERE transaction_id = NEW.transaction_id;

    IF total <> 0 THEN
        RAISE EXCEPTION
            'postings for transaction % do not sum to zero (got %)',
            NEW.transaction_id, total
            USING ERRCODE = 'check_violation';
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

BALANCE_TRIGGER = """
CREATE CONSTRAINT TRIGGER postings_balance_check
    AFTER INSERT ON postings
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION assert_balanced_transaction();
"""

# Postings are append-only. A correction is a new reversing pair, never
# an edit. These rules make that structural rather than a convention.
APPEND_ONLY_RULES = """
CREATE RULE postings_no_update AS ON UPDATE TO postings DO INSTEAD NOTHING;
CREATE RULE postings_no_delete AS ON DELETE TO postings DO INSTEAD NOTHING;
"""


def upgrade() -> None:
    account_status = postgresql.ENUM(
        "active", "frozen", "closed", name="account_status"
    )
    account_type = postgresql.ENUM("customer", "system", name="account_type")
    tx_status = postgresql.ENUM("posted", "reversed", name="tx_status")

    bind = op.get_bind()
    account_status.create(bind, checkfirst=True)
    account_type.create(bind, checkfirst=True)
    tx_status.create(bind, checkfirst=True)

    op.create_table(
        "accounts",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column("holder_name", sa.Text(), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column(
            "account_type",
            postgresql.ENUM(name="account_type", create_type=False),
            nullable=False,
            server_default="customer",
        ),
        sa.Column(
            "status",
            postgresql.ENUM(name="account_status", create_type=False),
            nullable=False,
            server_default="active",
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "transactions",
        sa.Column("id", sa.Text(), primary_key=True),
        sa.Column(
            "status",
            postgresql.ENUM(name="tx_status", create_type=False),
            nullable=False,
            server_default="posted",
        ),
        sa.Column("amount", sa.Numeric(20, 4), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("reference", sa.String(64)),
        sa.Column(
            "metadata",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "reverses_transaction_id", sa.Text(), sa.ForeignKey("transactions.id")
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("amount > 0", name="ck_transactions_amount_positive"),
    )

    op.create_table(
        "postings",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "transaction_id",
            sa.Text(),
            sa.ForeignKey("transactions.id"),
            nullable=False,
        ),
        sa.Column(
            "account_id", sa.Text(), sa.ForeignKey("accounts.id"), nullable=False
        ),
        sa.Column("event_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("amount", sa.Numeric(20, 4), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("amount <> 0", name="ck_postings_amount_nonzero"),
    )
    op.create_index(
        "ix_postings_account_created",
        "postings",
        ["account_id", sa.text("created_at DESC")],
    )
    op.create_index("ix_postings_transaction", "postings", ["transaction_id"])
    op.create_index("ix_postings_event", "postings", ["event_id"])

    op.create_table(
        "idempotency_keys",
        sa.Column("key", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("fingerprint", sa.String(64), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("response_body", postgresql.JSONB()),
        sa.Column("response_code", sa.Integer()),
        sa.Column("transaction_id", sa.Text(), sa.ForeignKey("transactions.id")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "expires_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now() + interval '24 hours'"),
        ),
        sa.CheckConstraint(
            "state IN ('in_progress', 'completed', 'failed')",
            name="ck_idempotency_state",
        ),
    )
    op.create_index("ix_idempotency_expires", "idempotency_keys", ["expires_at"])

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "event_id", postgresql.UUID(as_uuid=False), nullable=False, unique=True
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column(
            "schema_version", sa.Integer(), nullable=False, server_default="1"
        ),
        sa.Column("partition_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("traceparent", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
    )
    # Partial index: the relay scans the backlog only, so the index stays
    # roughly the size of the unpublished set rather than growing forever.
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index("ix_outbox_created", "outbox", ["created_at"])

    op.create_table(
        "processed_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=False), primary_key=True),
        sa.Column("consumer_group", sa.Text(), primary_key=True),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "payment_events",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "event_id", postgresql.UUID(as_uuid=False), nullable=False, unique=True
        ),
        sa.Column("transaction_id", sa.Text(), nullable=False),
        sa.Column("from_account_id", sa.Text(), nullable=False),
        sa.Column("to_account_id", sa.Text(), nullable=False),
        sa.Column("amount_minor", sa.BigInteger(), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column(
            "applied_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "dlq_messages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("original_topic", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("first_failed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("replayed_at", sa.DateTime(timezone=True)),
    )
    op.create_index(
        "ix_dlq_open",
        "dlq_messages",
        ["event_id"],
        postgresql_where=sa.text("replayed_at IS NULL"),
    )

    op.create_table(
        "reconciliation_runs",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("check_name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("detail", postgresql.JSONB()),
        sa.Column(
            "ran_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "status IN ('pass', 'warn', 'fail')", name="ck_reconciliation_status"
        ),
    )
    op.create_index("ix_reconciliation_ran_at", "reconciliation_runs", ["ran_at"])

    op.create_table(
        "auth_users",
        sa.Column("username", sa.Text(), primary_key=True),
        sa.Column("password_hash", sa.Text(), nullable=False),
        sa.Column("role", sa.Text(), nullable=False),
        sa.Column("account_id", sa.Text(), sa.ForeignKey("accounts.id")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "role IN ('viewer', 'client', 'operator')", name="ck_auth_role"
        ),
    )

    op.execute(BALANCE_TRIGGER_FN)
    op.execute(BALANCE_TRIGGER)
    op.execute(APPEND_ONLY_RULES)


def downgrade() -> None:
    op.execute("DROP RULE IF EXISTS postings_no_delete ON postings")
    op.execute("DROP RULE IF EXISTS postings_no_update ON postings")
    op.execute("DROP TRIGGER IF EXISTS postings_balance_check ON postings")
    op.execute("DROP FUNCTION IF EXISTS assert_balanced_transaction()")

    op.drop_table("auth_users")
    op.drop_table("reconciliation_runs")
    op.drop_table("dlq_messages")
    op.drop_table("payment_events")
    op.drop_table("processed_events")
    op.drop_table("outbox")
    op.drop_table("idempotency_keys")
    op.drop_table("postings")
    op.drop_table("transactions")
    op.drop_table("accounts")

    bind = op.get_bind()
    postgresql.ENUM(name="tx_status").drop(bind, checkfirst=True)
    postgresql.ENUM(name="account_type").drop(bind, checkfirst=True)
    postgresql.ENUM(name="account_status").drop(bind, checkfirst=True)
