.PHONY: help setup lock up down logs topics migrate downgrade seed test test-property test-fast test-unit test-integration test-messaging chaos baseline check rules

# The test suite never reads DATABASE_URL. It points the app at
# TEST_DATABASE_URL and refuses any database whose name does not end in
# _test, so no test can touch the database `make up` serves. The default is
# a separate database on the same server; the suite creates it if missing.
TEST_DATABASE_URL ?= postgresql+psycopg://payments:payments@localhost:5433/payments_test
export TEST_DATABASE_URL

# Host-side commands (alembic, pytest, the scripts) run from .venv when
# `make setup` has created it, so it never needs activating.
PYTHON ?= python3.13
export PATH := $(CURDIR)/.venv/bin:$(PATH)

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

setup:  ## Create .venv with the locked dependencies (needs Python 3.13)
	$(PYTHON) -m venv .venv
	.venv/bin/pip install --quiet --upgrade pip
	.venv/bin/pip install --quiet --no-deps -r requirements.lock
	.venv/bin/pip check

lock:  ## Regenerate requirements.lock from requirements.txt, in the Docker base image
	docker run --rm -v "$(CURDIR)/requirements.txt:/r.txt:ro" python:3.13.2-slim sh -c \
	  'pip install -q --no-cache-dir --root-user-action=ignore -r /r.txt && pip freeze' > requirements.lock.new
	{ sed -n '1,/^[^#]/{/^#/p;}' requirements.lock; cat requirements.lock.new; } > requirements.lock.tmp
	mv requirements.lock.tmp requirements.lock && rm requirements.lock.new

up:  ## Build and start the stack, wait until it is healthy, migrate and seed
	docker compose up -d --build --wait
	$(MAKE) migrate seed
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

test:  ## Full suite except the property run. Needs PostgreSQL and Kafka; fails, never skips
	pytest -m "not property"

test-property:  ## Hypothesis state machine, 200 examples (minutes; kept out of make test)
	pytest tests/property -m property

test-fast:  ## Unit tests only, no infrastructure
	pytest tests/unit -m "not integration and not messaging and not chaos"

test-unit: test-fast  ## Same as test-fast

test-integration:  ## Needs PostgreSQL
	pytest tests/integration tests/api tests/concurrency tests/messaging -m "not messaging and not chaos"

test-messaging:  ## Needs Kafka
	pytest tests/messaging -m messaging

CHAOS_EVENTS ?= 20000

chaos:  ## The five chaos scenarios, CHAOS_EVENTS each (results in docs/results/)
	PYTHONPATH=. CHAOS_EVENTS=$(CHAOS_EVENTS) python scripts/chaos_run.py --scenario all

baseline:  ## Steady-state throughput, no faults: 4 to 64 clients, 2 minutes each
	PYTHONPATH=. python scripts/baseline_run.py

check:  ## Static checks, including the no-float-money rule
	PYTHONPATH=. python scripts/check_no_float_money.py

PROMTOOL = docker run --rm -v "$(CURDIR)/monitoring/prometheus:/p" --entrypoint promtool prom/prometheus:v2.54.1

rules:  ## Validate the alert rules and run their unit tests (promtool, via Docker)
	$(PROMTOOL) check rules /p/alerts.yml
	$(PROMTOOL) test rules /p/alerts_test.yml
