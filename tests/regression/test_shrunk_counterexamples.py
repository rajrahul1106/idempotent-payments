"""Sequences Hypothesis shrank to, committed so they run on every `make test`.

Each was found by tests/property/test_ledger_machine.py against a deliberate
break of the code (the Phase 8 mutation check), and each passes on the real
code. They are the smallest known sequences that expose those breaks, so they
stay as cheap, deterministic guards.
"""

from __future__ import annotations

import pytest

from tests.property.test_ledger_machine import LedgerMachine

pytestmark = pytest.mark.messaging


def _step(state: LedgerMachine) -> None:
    state.invariants_hold()
    state.model_matches_database()


def test_four_concurrent_withdrawals_of_the_whole_balance(machine_setup):
    """Broke I4 (no negative balance) when the balance check ran outside the
    account lock (mutation a): all four withdrawals of 0.01 from a balance of
    0.01 succeeded, leaving -0.03.

    Shrunk sequence: first_accounts; create_account; deposit(accounts_0, 1);
    concurrent_withdrawals(source=accounts_0, destination=accounts_2).
    """
    # A race: one replay can land with the requests effectively serialised,
    # so the shrunk sequence runs 20 times, on fresh accounts each time.
    for _ in range(20):
        state = LedgerMachine()
        accounts_0, _accounts_1 = state.first_accounts().values
        _step(state)
        accounts_2 = state.create_account()
        _step(state)
        state.deposit(account=accounts_0, amount=1)
        _step(state)
        state.concurrent_withdrawals(destination=accounts_2, source=accounts_0)  # [201, 422, 422, 422]
        _step(state)
        state.teardown()


def test_redelivery_after_a_crash_between_the_commits_is_skipped(machine_setup):
    """Broke I1 (each event_id applied at most once) when the side effect ran
    even though the processed_events insert inserted no row (mutation b): the
    redelivered event tried a second payment_events row. The UNIQUE constraint
    on payment_events.event_id refused it, so it surfaced as a retry instead of
    a skip. The real code skips it.

    Shrunk sequence: first_accounts; deposit(accounts_1, 1);
    consumer_crashes_between_commits_then_runs_again.
    """
    state = LedgerMachine()
    _accounts_0, accounts_1 = state.first_accounts().values
    _step(state)
    state.deposit(account=accounts_1, amount=1)
    _step(state)
    state.consumer_crashes_between_commits_then_runs_again()  # skipped >= 1, retried == 0
    _step(state)
    state.teardown()
