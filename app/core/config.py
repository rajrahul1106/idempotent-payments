"""Application settings. Every value comes from the environment.

Nothing in this module has a production-safe default for a secret. The
JWT secret has a development placeholder that the app refuses to use when
ENVIRONMENT is not "local".
"""

from __future__ import annotations

from functools import lru_cache
from typing import Annotated, Literal

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- environment -----------------------------------------------------
    environment: Literal["local", "ci", "staging", "production"] = "local"
    log_level: str = "INFO"

    # --- database --------------------------------------------------------
    database_url: str = "postgresql+psycopg://payments:payments@localhost:5433/payments"
    db_pool_size: int = 20
    db_max_overflow: int = 5
    db_statement_timeout_ms: int = 5_000
    db_lock_timeout_ms: int = 3_000

    # --- kafka (Phase 4+) ------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:19092"
    kafka_main_topic: str = "payments.main"
    kafka_dlq_topic: str = "payments.dlq"
    kafka_partitions: int = 6
    kafka_consumer_group: str = "payments-ledger"
    # Each tier consumer's group is "<kafka_consumer_group>-retry-<tier>".
    kafka_dlq_writer_group: str = "payments-dlq-writer"

    # How long a consumer waits before retrying a message the database could
    # not take (decision D5, infrastructure errors). Doubles up to the max.
    consumer_db_backoff_initial_ms: int = 500
    consumer_db_backoff_max_ms: int = 10_000

    # Retry tiers, in seconds. Blueprint section 5.4.
    # NoDecode: read the env var as a plain string so the validator below
    # parses "5,60,600" rather than pydantic trying to JSON-decode it.
    retry_tier_seconds: Annotated[tuple[int, ...], NoDecode] = (5, 60, 600)

    # --- outbox relay ----------------------------------------------------
    outbox_poll_interval_ms: int = 200
    outbox_batch_size: int = 100

    # --- security --------------------------------------------------------
    jwt_secret: str = "dev-only-insecure-secret-change-me"
    jwt_algorithm: str = "HS256"
    jwt_expiry_minutes: int = 15
    rate_limit_per_minute: int = 100

    # --- idempotency -----------------------------------------------------
    idempotency_ttl_hours: int = 24

    @field_validator("retry_tier_seconds", mode="before")
    @classmethod
    def _parse_tiers(cls, v: object) -> object:
        if isinstance(v, str):
            return tuple(int(part) for part in v.split(",") if part.strip())
        return v

    @model_validator(mode="after")
    def _reject_dev_secret_outside_local(self) -> "Settings":
        if self.environment not in ("local", "ci"):
            if self.jwt_secret.startswith("dev-only"):
                raise ValueError(
                    "JWT_SECRET must be set to a real value outside local/ci"
                )
        return self

    @property
    def retry_topics(self) -> tuple[str, ...]:
        """Topic name per delay tier, e.g. payments.retry.5s."""
        return tuple(
            f"payments.retry.{self._humanise(seconds)}"
            for seconds in self.retry_tier_seconds
        )

    @staticmethod
    def _humanise(seconds: int) -> str:
        if seconds % 3600 == 0:
            return f"{seconds // 3600}h"
        if seconds % 60 == 0:
            return f"{seconds // 60}m"
        return f"{seconds}s"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
