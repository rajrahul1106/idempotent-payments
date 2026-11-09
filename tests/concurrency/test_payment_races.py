"""Phase 3 gate. These are the tests the whole project exists to pass.

Every test here runs real threads against a real PostgreSQL. There is no
mocking: the behaviour under test is what two transactions do to each
other, which a mock cannot reproduce.
"""

from __future__ import annotations

import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import pytest
from sqlalchemy import text

from app.db.session import read_session

pytestmark = pytest.mark.integration


def _headers(token: str, key: str | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    if key:
        headers["Idempotency-Key"] = key
    return headers


def _balance(account_id: str) -> Decimal:
    with read_session() as session:
        return Decimal(
            session.execute(
                text(
                    "SELECT COALESCE(SUM(amount), 0) FROM postings "
                    "WHERE account_id = :a"
                ),
                {"a": account_id},
            ).scalar_one()
        )


def _transaction_count(from_account: str) -> int:
    with read_session() as session:
        return int(
            session.execute(
                text(
                    "SELECT count(DISTINCT transaction_id) FROM postings "
                    "WHERE account_id = :a AND amount < 0"
                ),
                {"a": from_account},
            ).scalar_one()
        )


def test_ten_concurrent_identical_requests_produce_one_transaction(
    client, client_token, make_account, ledger_total
):
    """The headline gate: a client retrying 10 times pays once."""
    source = make_account(opening_minor=1_000_000)
    destination = make_account(opening_minor=0)
    key = str(uuid.uuid4())
    body = {
        "from_account_id": source,
        "to_account_id": destination,
        "amount_minor": 200_000,
        "currency": "INR",
    }

    before = ledger_total()

    def send(_: int):
        return client.post(
            "/v1/payments", json=body, headers=_headers(client_token, key)
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(send, range(10)))

    codes = [r.status_code for r in responses]
    assert all(c in (200, 201) for c in codes), codes
    assert codes.count(201) == 1, f"expected exactly one creation, got {codes}"

    payment_ids = {r.json()["payment_id"] for r in responses}
    assert len(payment_ids) == 1, "all responses must describe the same payment"

    replayed = [r for r in responses if r.status_code == 200]
    assert all(r.headers.get("Idempotency-Replayed") == "true" for r in replayed)

    assert _transaction_count(source) == 1
    assert _balance(source) == Decimal("8000.0000")
    assert _balance(destination) == Decimal("2000.0000")
    assert ledger_total() == before  # I2: still sums to zero


def test_concurrent_withdrawals_cannot_overdraw(
    client, client_token, make_account, ledger_total
):
    """Two withdrawals of 800 from a balance of 1000: exactly one succeeds."""
    source = make_account(opening_minor=100_000)  # 1000.00
    destination = make_account(opening_minor=0)

    def send(_: int):
        return client.post(
            "/v1/payments",
            json={
                "from_account_id": source,
                "to_account_id": destination,
                "amount_minor": 80_000,  # 800.00
                "currency": "INR",
            },
            headers=_headers(client_token, str(uuid.uuid4())),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(send, range(2)))

    codes = sorted(r.status_code for r in responses)
    assert codes == [201, 422], codes

    failure = next(r for r in responses if r.status_code == 422)
    assert failure.json()["error"]["code"] == "insufficient_funds"

    # I4: balance never went negative
    assert _balance(source) == Decimal("200.0000")
    assert _balance(source) >= 0
    assert ledger_total() == Decimal("0.0000")


@pytest.mark.parametrize("iteration", range(10))
def test_overdraw_race_is_not_flaky(
    client, client_token, make_account, iteration
):
    """Repeat the race: a lock bug shows up as an occasional pass, not a
    consistent one, so one run proves nothing."""
    source = make_account(opening_minor=100_000)
    destination = make_account(opening_minor=0)

    def send(_: int):
        return client.post(
            "/v1/payments",
            json={
                "from_account_id": source,
                "to_account_id": destination,
                "amount_minor": 80_000,
                "currency": "INR",
            },
            headers=_headers(client_token, str(uuid.uuid4())),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        codes = sorted(r.status_code for r in pool.map(send, range(2)))

    assert codes == [201, 422]
    assert _balance(source) >= 0


def test_bidirectional_transfers_do_not_deadlock(
    client, client_token, make_account, ledger_total
):
    """A->B and B->A at the same time. Ordered locking prevents deadlock."""
    a = make_account(opening_minor=500_000)
    b = make_account(opening_minor=500_000)

    def transfer(pair):
        src, dst = pair
        return client.post(
            "/v1/payments",
            json={
                "from_account_id": src,
                "to_account_id": dst,
                "amount_minor": 10_000,
                "currency": "INR",
            },
            headers=_headers(client_token, str(uuid.uuid4())),
        )

    pairs = [(a, b), (b, a)] * 25  # 50 transfers, half in each direction

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(transfer, pairs))

    assert all(r.status_code == 201 for r in responses), [
        r.status_code for r in responses
    ]
    # 25 each way at the same amount nets to zero
    assert _balance(a) == Decimal("5000.0000")
    assert _balance(b) == Decimal("5000.0000")
    assert ledger_total() == Decimal("0.0000")


def test_many_operations_on_one_account_match_arithmetic(
    client, client_token, make_account, ledger_total
):
    """Per-account correctness under 10 concurrent clients."""
    source = make_account(opening_minor=10_000_000)  # 100,000.00
    destination = make_account(opening_minor=0)
    transfers = 100
    amount_minor = 5_000  # 50.00

    def send(_: int):
        return client.post(
            "/v1/payments",
            json={
                "from_account_id": source,
                "to_account_id": destination,
                "amount_minor": amount_minor,
                "currency": "INR",
            },
            headers=_headers(client_token, str(uuid.uuid4())),
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(send, range(transfers)))

    created = [r for r in responses if r.status_code == 201]
    assert len(created) == transfers, [
        r.json() for r in responses if r.status_code != 201
    ][:3]

    moved = Decimal(amount_minor * transfers) / 100
    assert _balance(destination) == moved
    assert _balance(source) == Decimal("100000.0000") - moved
    assert ledger_total() == Decimal("0.0000")
