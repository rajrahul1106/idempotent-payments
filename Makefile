.PHONY: help up down logs topics migrate downgrade seed test test-fast test-unit test-integration test-messaging load chaos fmt check rules

# The test suite never reads DATABASE_URL. It points the app at
# TEST_DATABASE_URL and refuses any database whose name does not end in
# _test, so no test can touch the database `make up` serves. The default is
# a separate database on the same server; the suite creates it if missing.
TEST_DATABASE_URL ?= postgresql+psycopg://payments:payments@localhost:5433/payments_test
export TEST_DATABASE_URL

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

up:  ## Start the whole stack
	docker compose up -d --build
	@echo "API      http://localhost:8000/docs"
	@echo "Grafana  http://localhost:3000"
	@echo "Jaeger   http://localhost:16686"

down:  ## Stop the stack and drop volumes
	docker compose down -v

logs:  ## Tail logs for app services
	docker compose logs -f api relay consumer retry-5s retry-1m retry-10m dlq-writer reconciliation

topics:  ## Create Kafka topics (idempotent; also runs as a Compose service)
	PYTHONPATH=. python -m app.messaging.topic_setup

migrate:  ## Apply all migrations
	alembic upgrade head

downgrade:  ## Roll back one migration
	alembic downgrade -1

seed:  ## Create system and demo accounts
	PYTHONPATH=. python scripts/seed.py

test:  ## Full suite. Needs PostgreSQL; fails, never skips, without it
	pytest

test-fast:  ## Unit tests only, no infrastructure
	pytest tests/unit -m "not integration and not messaging and not chaos"

test-unit: test-fast  ## Same as test-fast

test-integration:  ## Needs PostgreSQL
	pytest tests/integration tests/api tests/concurrency tests/messaging -m "not messaging and not chaos"

test-messaging:  ## Needs Kafka
	pytest tests/messaging -m messaging

load:  ## Load generator against a running API
	PYTHONPATH=. python scripts/load_test.py

chaos:  ## Failure injection run
	PYTHONPATH=. python scripts/chaos_run.py

check:  ## Static checks, including the no-float-money rule
	PYTHONPATH=. python scripts/check_no_float_money.py

PROMTOOL = docker run --rm -v "$(CURDIR)/monitoring/prometheus:/p" --entrypoint promtool prom/prometheus:v2.54.1

rules:  ## Validate the alert rules and run their unit tests (promtool, via Docker)
	$(PROMTOOL) check rules /p/alerts.yml
	$(PROMTOOL) test rules /p/alerts_test.yml
