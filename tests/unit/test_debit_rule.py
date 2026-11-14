"""The D4 debit rule, as one shared function (decision D4, docs/ARCHITECTURE.md §16).

GET /v1/accounts reports can_debit from this function, and the write path
enforces its two halves. These cases pin the rule itself.
"""

from __future__ import annotations

import pytest

from app.services.payment import Actor, can_debit, may_debit_account_type, may_debit_source

OPERATOR = Actor(role="operator")
ALICE = Actor(role="client", account_id="acc_alice")
VIEWER = Actor(role="viewer")
NO_ACCOUNT_CLIENT = Actor(role="client", account_id=None)


@pytest.mark.parametrize(
    "actor, account_id, account_type, expected",
    [
        (OPERATOR, "acc_bob", "customer", True),
        (OPERATOR, "sys_settlement", "system", True),
        (ALICE, "acc_alice", "customer", True),
        (ALICE, "acc_bob", "customer", False),
        (ALICE, "sys_settlement", "system", False),
        (VIEWER, "acc_alice", "customer", False),
        (VIEWER, "sys_settlement", "system", False),
        (NO_ACCOUNT_CLIENT, "acc_alice", "customer", False),
        # A client whose token names a system account still may not debit it.
        (Actor(role="client", account_id="sys_fees"), "sys_fees", "system", False),
    ],
)
def test_can_debit(actor, account_id, account_type, expected):
    assert can_debit(actor, account_id, account_type) is expected


def test_can_debit_is_exactly_both_halves():
    for actor in (OPERATOR, ALICE, VIEWER, NO_ACCOUNT_CLIENT):
        for account_id in ("acc_alice", "acc_bob", "sys_fees"):
            for account_type in ("customer", "system"):
                assert can_debit(actor, account_id, account_type) == (
                    may_debit_source(actor, account_id)
                    and may_debit_account_type(actor, account_type)
                )
