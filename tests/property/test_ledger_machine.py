"""Phase 8: a Hypothesis state machine against the real API, database and
broker (docs/ARCHITECTURE.md §10, property-based testing design).

A Python model holds every account's balance as Decimal. Hypothesis
generates sequences of operations, applies each to the real system through
the HTTP API, and after EVERY step checks that the model and the database
agree and that the invariants hold:

    model    every modelled account's balance equals SUM(postings)
    I1       no event applied twice (an event_id in two transactions), and
             every consumed event has exactly one payment_events row
    I2       SUM(postings.amount) = 0
    I3       every payment that answered 201 has exactly one outbox row
             (publishing is the relay's job; the consumer rule publishes)
    I4       no customer account below zero

Each example uses fresh accounts, created through the API, so nothing is
truncated between examples. Run with `make test-property` (kept out of
`make test`): 200 examples.
"""

from __future__ import annotations

import collections
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    initialize,
    invariant,
    multiple,
    precondition,
    rule,
    run_state_machine_as_test,
)
from sqlalchemy import text

from app.core.money import to_decimal
from app.core.security import issue_token
from app.db.session import engine, read_session, unit_of_work
from app.services.reconciliation import FAIL, run_checks

pytestmark = pytest.mark.property

EXAMPLES = int(os.environ.get("PROPERTY_EXAMPLES", 200))
STEPS = 25
amounts = st.integers(min_value=1, max_value=5_000_000)  # minor units, up to 50,000.00


#: Rule calls and examples across the run, printed and checked at the end:
#: Hypothesis prints no statistics for a state machine run this way.
CALLS: collections.Counter = collections.Counter()


class Crash(RuntimeError):
    """Raised between the database commit and the offset commit."""


class LedgerMachine(RuleBasedStateMachine):
    """Configured by the test below: client, ladder, producer, group."""

    client = None
    ladder = None
    producer = None
    group = None

    accounts = Bundle("accounts")
    payments = Bundle("payments")  # (key, body, token, response) of every 201

    def __init__(self) -> None:
        super().__init__()
        CALLS["examples"] += 1
        tag = uuid.uuid4().hex[:8]
        self.operator = {"Authorization": f"Bearer {issue_token(f'prop-op-{tag}', 'operator')[0]}"}
        self.model: dict[str, Decimal] = {}
        self.tokens: dict[str, str] = {}
        self.event_ids: list[str] = []  # this example's 201 payments
        self.consumed: set[str] = set()  # ...that the consumer rule has applied

    # --- helpers ---------------------------------------------------------------------

    def _owner(self, account: str) -> dict:
        if account not in self.tokens:
            self.tokens[account] = issue_token(f"prop-{account}", "client", account_id=account)[0]
        return {"Authorization": f"Bearer {self.tokens[account]}"}

    def _pay(self, headers: dict, body: dict, key: str | None = None):
        key = key or str(uuid.uuid4())
        response = self.client.post("/v1/payments", json=body,
                                    headers={**headers, "Idempotency-Key": key})
        return key, response

    @staticmethod
    def _body(source: str, destination: str, amount_minor: int) -> dict:
        return {"from_account_id": source, "to_account_id": destination,
                "amount_minor": amount_minor, "currency": "INR"}

    def _applied(self, source: str, destination: str, amount_minor: int, response) -> None:
        amount = to_decimal(amount_minor, "INR")
        if source in self.model:
            self.model[source] -= amount
        self.model[destination] += amount
        self.event_ids.append(response.json()["event_id"])

    def _minor(self, account: str) -> int:
        return int(self.model[account] * 100)

    # --- rules -----------------------------------------------------------------------

    @initialize(target=accounts)
    def first_accounts(self):
        CALLS["first_accounts"] += 1
        return multiple(self._create(), self._create())

    def _create(self) -> str:
        response = self.client.post("/v1/accounts", headers=self.operator,
                                    json={"holder_name": "property", "currency": "INR"})
        assert response.status_code == 201, response.text
        account = response.json()["id"]
        self.model[account] = Decimal("0")
        return account

    @rule(target=accounts)
    def create_account(self):
        CALLS["create_account"] += 1
        return self._create()

    @rule(target=payments, account=accounts, amount=amounts)
    def deposit(self, account, amount):
        CALLS["deposit"] += 1
        body = self._body("sys_settlement", account, amount)
        key, response = self._pay(self.operator, body)
        assert response.status_code == 201, response.text
        self._applied("sys_settlement", account, amount, response)
        return (key, body, self.operator, response.json())

    @rule(target=payments, source=accounts, destination=accounts, amount=amounts)
    def transfer(self, source, destination, amount):
        CALLS["transfer"] += 1
        if source == destination:
            return multiple()
        body = self._body(source, destination, amount)
        key, response = self._pay(self._owner(source), body)
        if amount <= self._minor(source):
            assert response.status_code == 201, response.text
            self._applied(source, destination, amount, response)
            return (key, body, self._owner(source), response.json())
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "insufficient_funds"
        return multiple()

    @rule(source=accounts, destination=accounts, extra=amounts)
    def overdraw(self, source, destination, extra):
        """More than the balance: 422, and nothing moves."""
        CALLS["overdraw"] += 1
        if source == destination:
            return
        before = dict(self.model)
        _, response = self._pay(self._owner(source),
                                self._body(source, destination, self._minor(source) + extra))
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "insufficient_funds"
        assert self.model == before

    @rule(payment=payments)
    def replay(self, payment):
        """Same key, same body: the stored response, not a second payment."""
        CALLS["replay"] += 1
        key, body, headers, original = payment
        _, response = self._pay(headers, body, key=key)
        assert response.status_code == 200, response.text
        assert response.headers["Idempotency-Replayed"] == "true"
        assert response.json() == original

    @rule(payment=payments, delta=st.integers(min_value=1, max_value=1000))
    def reuse_key_with_another_body(self, payment, delta):
        CALLS["reuse_key_with_another_body"] += 1
        key, body, headers, _ = payment
        _, response = self._pay(headers, {**body, "amount_minor": body["amount_minor"] + delta},
                                key=key)
        assert response.status_code == 422, response.text
        assert response.json()["error"]["code"] == "idempotency_key_reuse"

    @precondition(lambda self: any(v > 0 for v in self.model.values()))
    @rule(source=accounts, destination=accounts)
    def concurrent_withdrawals(self, source, destination):
        """Four simultaneous payments, each for the whole balance: exactly one
        may succeed. Only a balance check made under the account lock
        guarantees that."""
        CALLS["concurrent_withdrawals"] += 1
        if source == destination or self.model[source] <= 0:
            return
        amount = self._minor(source)
        headers = self._owner(source)
        barrier = threading.Barrier(4)

        def send(_):
            barrier.wait()
            return self._pay(headers, self._body(source, destination, amount))[1]

        with ThreadPoolExecutor(max_workers=4) as pool:
            responses = list(pool.map(send, range(4)))
        codes = sorted(r.status_code for r in responses)
        assert codes == [201, 422, 422, 422], codes
        winner = next(r for r in responses if r.status_code == 201)
        self._applied(source, destination, amount, winner)

    @precondition(lambda self: len(self.event_ids) > len(self.consumed))
    @rule()
    def consumer_crashes_between_commits_then_runs_again(self):
        """Publish this example's events, run a consumer that dies after its
        first database commit and before that offset commit, then run a fresh
        one: the redelivered event is skipped, nothing is retried, and every
        event is applied exactly once."""
        CALLS["consumer_crashes_between_commits_then_runs_again"] += 1
        from app.messaging import relay
        from app.messaging.consumer import PaymentEventConsumer

        with unit_of_work() as session:
            relay.publish_batch(session, self.producer, topic=self.ladder.main, limit=1000)

        def crash_after_first(_event, _applied):
            raise Crash("killed between the database commit and the offset commit")

        crashed = PaymentEventConsumer(ladder=self.ladder, group_id=self.group,
                                       config_overrides=FAST, after_db_commit=crash_after_first)
        outcome: list[BaseException] = []
        stop_crashed = threading.Event()

        def run_crashed():
            try:
                crashed.run(stop_crashed)
            except BaseException as exc:  # the injected crash ends the run
                outcome.append(exc)

        crasher = threading.Thread(target=run_crashed, daemon=True)
        crasher.start()
        crasher.join(timeout=30)
        stop_crashed.set()
        crasher.join(timeout=10)
        assert outcome and isinstance(outcome[0], Crash), f"consumer did not crash: {outcome}"

        survivor = PaymentEventConsumer(ladder=self.ladder, group_id=self.group,
                                        config_overrides=FAST)
        stop = threading.Event()
        worker = threading.Thread(target=survivor.run, args=(stop,), daemon=True)
        worker.start()
        stats = survivor.stats
        deadline = time.monotonic() + 30
        try:
            # Until every event is applied AND the redelivery has come round:
            # the crashed consumer's last event was committed to the database
            # but not to Kafka, so the survivor must receive it again.
            while time.monotonic() < deadline and not (
                _projected(self.event_ids) == len(self.event_ids)
                and stats.handled >= 1 and (stats.skipped >= 1 or stats.retried or stats.dead_lettered)
            ):
                time.sleep(0.05)
        finally:
            stop.set()
            worker.join(timeout=30)
        assert _projected(self.event_ids) == len(self.event_ids), "events not applied"
        assert stats.skipped >= 1, stats  # the crashed event, redelivered and skipped
        assert stats.retried == 0 and stats.dead_lettered == 0, stats
        self.consumed = set(self.event_ids)

    # --- after every step --------------------------------------------------------------

    @invariant()
    def model_matches_database(self):
        if not self.model:
            return
        with read_session() as session:
            actual = dict(session.execute(
                text("SELECT a.id, COALESCE(SUM(p.amount), 0) FROM accounts a "
                     "LEFT JOIN postings p ON p.account_id = a.id "
                     "WHERE a.id = ANY(:ids) GROUP BY a.id"),
                {"ids": list(self.model)},
            ).all())
        assert actual == self.model, {a: (self.model[a], actual.get(a))
                                      for a in self.model if actual.get(a) != self.model[a]}

    @invariant()
    def invariants_hold(self):
        """I1 to I4 for this example's accounts and events.

        Scoped to the example so a failure is a property of the steps that
        caused it and Hypothesis can shrink it: a violation committed by one
        example would otherwise fail every later example at its first step.
        The whole database is checked once more at the end of the run.
        """
        if not self.model:
            return
        accounts = list(self.model)
        with read_session() as session:
            # I2: every transaction touching these accounts nets to zero.
            unbalanced = session.execute(
                text("SELECT count(*) FROM (SELECT p.transaction_id FROM postings p "
                     "WHERE p.transaction_id IN (SELECT transaction_id FROM postings "
                     "WHERE account_id = ANY(:a)) GROUP BY p.transaction_id "
                     "HAVING sum(p.amount) <> 0) u"),
                {"a": accounts},
            ).scalar_one()
            # I4: none of these customer accounts below zero.
            negative = session.execute(
                text("SELECT count(*) FROM (SELECT account_id FROM postings "
                     "WHERE account_id = ANY(:a) GROUP BY account_id HAVING sum(amount) < 0) n"),
                {"a": accounts},
            ).scalar_one()
            outbox = projections = twice = 0
            if self.event_ids:
                # I1: no event in two transactions; consumed events applied once.
                twice, outbox, projections = session.execute(
                    text("SELECT (SELECT count(*) FROM (SELECT event_id FROM postings "
                         "WHERE event_id = ANY(CAST(:e AS uuid[])) GROUP BY event_id "
                         "HAVING count(DISTINCT transaction_id) > 1) d),"
                         " (SELECT count(*) FROM outbox WHERE event_id = ANY(CAST(:e AS uuid[]))),"
                         " (SELECT count(*) FROM payment_events WHERE event_id = ANY(CAST(:e AS uuid[])))"),
                    {"e": self.event_ids},
                ).one()
        assert unbalanced == 0, "I2: a transaction does not net to zero"
        assert negative == 0, "I4: a customer account is below zero"
        assert twice == 0, "I1: an event_id was applied in two transactions"
        assert projections == len(self.consumed), "I1: a consumed event not applied exactly once"
        assert outbox == len(self.event_ids), "I3: a 201 payment without exactly one outbox row"


FAST = {"session.timeout.ms": 6000, "heartbeat.interval.ms": 1000}


def _projected(event_ids) -> int:
    with read_session() as session:
        return session.execute(
            text("SELECT count(*) FROM payment_events WHERE event_id = ANY(CAST(:e AS uuid[]))"),
            {"e": list(event_ids)},
        ).scalar_one()


@pytest.fixture
def machine_setup(client, kafka, make_ladder):
    """One topic ladder and one consumer group for every example, so each
    event is consumed by this group exactly once across the whole run."""
    from app.messaging.producer import KafkaProducer

    # Earlier tests leave unpublished outbox rows (no relay runs against the
    # test database). Mark them published so the consumer rule sends only
    # this run's events.
    with engine.begin() as conn:
        conn.execute(text("UPDATE outbox SET published_at = now() WHERE published_at IS NULL"))
    producer = KafkaProducer(kafka, client_id="property")
    LedgerMachine.client = client
    LedgerMachine.ladder = make_ladder(delays=(1.0,))
    LedgerMachine.producer = producer
    LedgerMachine.group = f"property-{uuid.uuid4().hex[:8]}"
    yield
    producer.close()


def test_ledger_state_machine(machine_setup):
    run_state_machine_as_test(
        LedgerMachine,
        settings=settings(
            max_examples=EXAMPLES,
            stateful_step_count=STEPS,
            deadline=None,
            suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
            print_blob=True,
        ),
    )
    print("\nstate machine:", dict(sorted(CALLS.items())))
    # And the whole database, once, after every example: I1, I2 and I4.
    checks = {c.name: c.status for c in run_checks(record=False).checks}
    for name in ("no_duplicate_event_ids", "ledger_balances_to_zero", "no_negative_balances"):
        assert checks[name] != FAIL, checks
    assert CALLS["examples"] >= EXAMPLES, CALLS
    rules = [r for r in CALLS if r not in ("examples", "first_accounts")]
    assert len(rules) == 8 and all(CALLS[r] > 0 for r in rules), CALLS
