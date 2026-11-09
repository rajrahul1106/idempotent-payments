-- Phase 1 gate. Each block asserts the DATABASE rejects something, with no
-- application code involved. Run with: psql -f scripts/verify_phase1.sql
\set ON_ERROR_STOP off

\echo '=== setup ==='
INSERT INTO accounts (id, holder_name, currency) VALUES
  ('acc_a', 'Alice', 'INR'), ('acc_b', 'Bob', 'INR')
ON CONFLICT DO NOTHING;

\echo ''
\echo '=== TEST 1: duplicate idempotency key is rejected by the DB ==='
INSERT INTO idempotency_keys (key, fingerprint, state)
VALUES ('11111111-1111-1111-1111-111111111111', repeat('a',64), 'completed');
-- expect: ERROR duplicate key value violates unique constraint
INSERT INTO idempotency_keys (key, fingerprint, state)
VALUES ('11111111-1111-1111-1111-111111111111', repeat('b',64), 'completed');

\echo ''
\echo '=== TEST 2: unbalanced transaction cannot commit ==='
BEGIN;
INSERT INTO transactions (id, amount, currency) VALUES ('tx_bad', 100.0000, 'INR');
-- one leg only: sums to -100, not 0
INSERT INTO postings (transaction_id, account_id, event_id, amount, currency)
VALUES ('tx_bad', 'acc_a', gen_random_uuid(), -100.0000, 'INR');
-- expect: ERROR at COMMIT from the deferred constraint trigger
COMMIT;

\echo ''
\echo '=== TEST 3: balanced transaction commits ==='
BEGIN;
INSERT INTO transactions (id, amount, currency) VALUES ('tx_good', 100.0000, 'INR');
INSERT INTO postings (transaction_id, account_id, event_id, amount, currency)
VALUES ('tx_good', 'acc_a', '22222222-2222-2222-2222-222222222222', -100.0000, 'INR'),
       ('tx_good', 'acc_b', '22222222-2222-2222-2222-222222222222',  100.0000, 'INR');
COMMIT;
SELECT 'postings written: ' || count(*) FROM postings WHERE transaction_id = 'tx_good';

\echo ''
\echo '=== TEST 4: postings are append-only ==='
UPDATE postings SET amount = 999999 WHERE transaction_id = 'tx_good';
SELECT 'after UPDATE attempt, amounts are still: ' ||
       string_agg(amount::text, ', ' ORDER BY amount)
  FROM postings WHERE transaction_id = 'tx_good';

DELETE FROM postings WHERE transaction_id = 'tx_good';
SELECT 'after DELETE attempt, row count is still: ' || count(*)
  FROM postings WHERE transaction_id = 'tx_good';

\echo ''
\echo '=== TEST 5: zero-amount posting rejected ==='
BEGIN;
INSERT INTO transactions (id, amount, currency) VALUES ('tx_zero', 1.0000, 'INR');
INSERT INTO postings (transaction_id, account_id, event_id, amount, currency)
VALUES ('tx_zero', 'acc_a', gen_random_uuid(), 0.0000, 'INR');
ROLLBACK;

\echo ''
\echo '=== TEST 6: negative transaction amount rejected ==='
INSERT INTO transactions (id, amount, currency) VALUES ('tx_neg', -5.0000, 'INR');

\echo ''
\echo '=== INVARIANT I2: ledger sums to zero ==='
SELECT COALESCE(SUM(amount), 0) AS ledger_total FROM postings;

\echo ''
\echo '=== NUMERIC precision check: no float drift over 1000 rows of 0.1 ==='
SELECT SUM(v)::text AS numeric_sum
  FROM (SELECT 0.1000::NUMERIC(20,4) AS v FROM generate_series(1, 1000)) s;
