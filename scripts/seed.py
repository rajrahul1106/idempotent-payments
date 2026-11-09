"""Create system accounts, demo customer accounts and auth users.

System accounts are the counterparty for money entering the ledger from
outside. Without them a deposit has only one leg and the zero-sum
invariant cannot hold.
"""

from __future__ import annotations

import sys
from decimal import Decimal

from sqlalchemy import select

from app.core.money import to_decimal
from app.core.security import hash_password
from app.db.models.ledger import Account, Posting, Transaction
from app.db.models.messaging import AuthUser
from app.db.session import unit_of_work

SYSTEM_ACCOUNTS = [
    ("sys_settlement", "Bank Settlement", "INR"),
    ("sys_fees", "Fee Income", "INR"),
]

DEMO_ACCOUNTS = [
    ("acc_alice", "Alice Sharma", "INR"),
    ("acc_bob", "Bob Mehta", "INR"),
    ("acc_carol", "Carol Nair", "INR"),
]

USERS = [
    ("operator", "operator-dev-password", "operator", None),
    ("alice", "alice-dev-password", "client", "acc_alice"),
    ("auditor", "auditor-dev-password", "viewer", None),
]

OPENING_BALANCE_MINOR = 1_000_000  # 10,000.00 INR


def seed() -> None:
    with unit_of_work() as session:
        existing = set(session.execute(select(Account.id)).scalars().all())

        for acc_id, name, ccy in SYSTEM_ACCOUNTS:
            if acc_id not in existing:
                session.add(
                    Account(
                        id=acc_id,
                        holder_name=name,
                        currency=ccy,
                        account_type="system",
                    )
                )

        for acc_id, name, ccy in DEMO_ACCOUNTS:
            if acc_id not in existing:
                session.add(
                    Account(id=acc_id, holder_name=name, currency=ccy)
                )

        session.flush()

        # Opening balances as real double-entry pairs: the settlement
        # account goes negative, which is exactly what a system account
        # is for.
        import uuid

        for acc_id, _, ccy in DEMO_ACCOUNTS:
            funded = session.execute(
                select(Posting.id).where(Posting.account_id == acc_id).limit(1)
            ).first()
            if funded:
                continue

            amount = to_decimal(OPENING_BALANCE_MINOR, ccy)
            tx_id = f"tx_open_{acc_id}"
            event_id = str(uuid.uuid4())
            session.add(
                Transaction(
                    id=tx_id,
                    amount=amount,
                    currency=ccy,
                    reference="opening balance",
                    metadata_json={"source": "seed"},
                )
            )
            session.add_all(
                [
                    Posting(
                        transaction_id=tx_id,
                        account_id="sys_settlement",
                        event_id=event_id,
                        amount=-amount,
                        currency=ccy,
                    ),
                    Posting(
                        transaction_id=tx_id,
                        account_id=acc_id,
                        event_id=event_id,
                        amount=amount,
                        currency=ccy,
                    ),
                ]
            )

        for username, password, role, account_id in USERS:
            if session.get(AuthUser, username) is None:
                session.add(
                    AuthUser(
                        username=username,
                        password_hash=hash_password(password),
                        role=role,
                        account_id=account_id,
                    )
                )

    print("Seed complete.")
    print("  system accounts :", ", ".join(a[0] for a in SYSTEM_ACCOUNTS))
    print("  demo accounts   :", ", ".join(a[0] for a in DEMO_ACCOUNTS))
    print("  users           :", ", ".join(u[0] for u in USERS))
    print("  passwords are in scripts/seed.py and are development-only")


if __name__ == "__main__":
    try:
        seed()
    except Exception as exc:  # pragma: no cover
        print(f"seed failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
