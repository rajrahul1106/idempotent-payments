.PHONY: help up down logs migrate downgrade seed test test-unit test-integration test-messaging load chaos fmt check

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
	docker compose logs -f api relay consumer

migrate:  ## Apply all migrations
	alembic upgrade head

downgrade:  ## Roll back one migration
	alembic downgrade -1

seed:  ## Create system and demo accounts
	PYTHONPATH=. python scripts/seed.py

test:  ## Full suite
	pytest

test-unit:  ## No infrastructure needed
	pytest tests/unit -m "not integration and not messaging and not chaos"

test-integration:  ## Needs PostgreSQL
	pytest tests/integration tests/api tests/concurrency -m "not messaging and not chaos"

test-messaging:  ## Needs Kafka
	pytest tests/messaging -m messaging

load:  ## Load generator against a running API
	PYTHONPATH=. python scripts/load_test.py

chaos:  ## Failure injection run
	PYTHONPATH=. python scripts/chaos_run.py

check:  ## Static checks, including the no-float-money rule
	PYTHONPATH=. python scripts/check_no_float_money.py
