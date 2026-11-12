"""Login must not reveal which usernames exist."""

from __future__ import annotations

import time

from argon2 import extract_parameters

from app.core.security import DUMMY_PASSWORD_HASH, _hasher, hash_password, verify_password


def _median_seconds(fn, runs: int = 5) -> float:
    times = []
    for _ in range(runs):
        started = time.perf_counter()
        fn()
        times.append(time.perf_counter() - started)
    return sorted(times)[runs // 2]


def test_dummy_hash_is_a_real_hash_with_the_hashers_parameters():
    assert extract_parameters(DUMMY_PASSWORD_HASH) == _hasher._parameters  # noqa: SLF001


def test_dummy_hash_matches_no_guess():
    assert verify_password("", DUMMY_PASSWORD_HASH) is False
    assert verify_password("x", DUMMY_PASSWORD_HASH) is False


def test_verifying_the_dummy_costs_what_a_real_hash_costs():
    real = hash_password("correct horse")
    against_real = _median_seconds(lambda: verify_password("wrong", real))
    against_dummy = _median_seconds(lambda: verify_password("wrong", DUMMY_PASSWORD_HASH))
    # Same parameters, same work: within a factor of two either way. The old
    # malformed placeholder failed to parse about 1,000 times faster.
    assert 0.5 < against_dummy / against_real < 2.0


def test_a_malformed_hash_is_a_failed_login_not_an_exception():
    assert verify_password("x", "$argon2id$v=19$m=65536,t=3,p=4$x$x") is False
