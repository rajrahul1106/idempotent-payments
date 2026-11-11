"""The outbox relay.

Its whole job: take events the write path committed and get them onto
Kafka, losing none of them. It is allowed to publish the same event twice;
it is never allowed to publish zero times. That asymmetry is the design.
The consumer deduplicates on event_id, which is what lets this process
stay simple enough to reason about.

The loop, per tick:

    BEGIN
      claim up to N unpublished rows, FOR UPDATE SKIP LOCKED, id order
      publish them one at a time, stopping at the first failure
      stamp published_at on the ones that were acknowledged
    COMMIT

A crash anywhere in that block leaves the claimed rows unpublished in the
database (the transaction rolls back), so the next tick picks them up. A
crash after the broker acknowledged but before the commit republishes
those events, which is the duplicate the consumer is built to absorb.

Publishing stops at the first failure rather than skipping past it. Rows
are claimed in id order, which for any one account is commit order (see
outbox.claim_batch), so continuing past a failed row would publish a later
event for the same account before an earlier one and break per-account
ordering.

Exactly one relay instance runs (blueprint decision D1), and the database
enforces it: run_forever publishes only while it holds a Postgres advisory
lock, taken on a dedicated connection outside the pool and confirmed with
the server before every batch. A second relay logs that the lock is taken
and waits. It takes over when the first one unlocks on stop, or when the
first one's lock connection dies. The relay holds the claimed
outbox rows' locks while it publishes. Those locks never block
a payment, which only inserts new outbox rows, and each publish is bounded
by the producer's 10-second flush.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import signal
import threading
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from opentelemetry.trace import SpanKind, Status, StatusCode

from app.core import metrics
from app.core.config import settings
from app.core.telemetry import (
    context_from,
    current_traceparent,
    setup_tracing,
    shutdown_tracing,
    tracer,
)
from app.db.repositories import outbox as outbox_repo
from app.db.session import engine, unit_of_work
from app.messaging import topics
from app.messaging.producer import Producer, PublishError, build_producer

logger = logging.getLogger(__name__)


@dataclass
class RelayResult:
    """What one tick of the relay did."""

    claimed: int = 0
    published: int = 0
    failed_event_id: str | None = None
    error: str | None = None
    event_ids: list[str] = field(default_factory=list)

    @property
    def had_failure(self) -> bool:
        return self.error is not None


def _serialise(event) -> tuple[bytes, dict[str, str]]:
    """The wire format: JSON body, metadata in headers.

    Metadata goes in headers as well as the body so a consumer can route
    or filter without deserialising the payload.
    """
    value = json.dumps(event.payload, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )
    headers = {
        topics.HEADER_EVENT_ID: event.event_id,
        topics.HEADER_EVENT_TYPE: event.event_type,
        topics.HEADER_SCHEMA_VERSION: str(event.schema_version),
    }
    if event.traceparent:
        # Carried across the broker so the consumer's spans join the same
        # trace as the HTTP request that created the payment.
        headers[topics.HEADER_TRACEPARENT] = event.traceparent
    return value, headers


_tracer = tracer(__name__)


def _publish_one(producer: Producer, event, topic: str) -> str | None:
    """Publish one outbox row inside its own span. None if acknowledged,
    otherwise the error.

    The span is a child of the traceparent the API stored on the row, and
    the message carries this span's traceparent, so the consumer's span
    becomes its child: one trace from the HTTP request onwards.
    """
    value, headers = _serialise(event)
    with _tracer.start_as_current_span(
        "outbox publish",
        context=context_from(event.traceparent),
        kind=SpanKind.PRODUCER,
        attributes={
            "event_id": event.event_id,
            "account_id": event.partition_key,
            "messaging.system": "kafka",
            "messaging.destination.name": topic,
        },
    ) as span:
        traceparent = current_traceparent()
        if traceparent:
            headers[topics.HEADER_TRACEPARENT] = traceparent
        try:
            producer.publish(
                topic=topic,
                key=event.partition_key,
                value=value,
                headers=headers,
            )
        except PublishError as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, "not acknowledged"))
            logger.warning(
                "relay publish failed, stopping batch at event %s: %s",
                event.event_id,
                exc,
            )
            return str(exc)
    return None


def publish_batch(
    session: Session,
    producer: Producer,
    *,
    limit: int | None = None,
    topic: str = topics.MAIN_TOPIC,
) -> RelayResult:
    """Claim, publish and mark one batch inside the caller's transaction."""
    batch = outbox_repo.claim_batch(
        session, limit if limit is not None else settings.outbox_batch_size
    )
    result = RelayResult(claimed=len(batch))
    if not batch:
        return result

    acknowledged: list[int] = []

    for event in batch:
        error = _publish_one(producer, event, topic)
        if error is not None:
            # Leave this row and every row after it unpublished. They are
            # the next tick's first rows, in the same order.
            result.failed_event_id = event.event_id
            result.error = error
            break

        acknowledged.append(event.id)
        result.event_ids.append(event.event_id)
        if event.created_at is not None:
            delay = (dt.datetime.now(dt.timezone.utc) - event.created_at).total_seconds()
            metrics.OUTBOX_PUBLISH_DELAY.observe(max(delay, 0.0))

    result.published = outbox_repo.mark_published(session, acknowledged)

    if result.published != len(acknowledged):
        # Should be impossible: the rows are locked by this transaction.
        raise RuntimeError(
            f"marked {result.published} rows but acknowledged {len(acknowledged)}"
        )

    return result


def run_once(producer: Producer, *, limit: int | None = None) -> RelayResult:
    """One tick, in its own transaction."""
    with unit_of_work() as session:
        return publish_batch(session, producer, limit=limit)


# One relay per database (decision D1). Any fixed value works, as long as
# every relay uses the same one; advisory locks are scoped to a database.
RELAY_LOCK_ID = 506_032_705
LOCK_RETRY_SECONDS = 2.0

#: application_name of the relay's two dedicated connections, so an operator
#: (or a test) can find them in pg_stat_activity.
LOCK_APPLICATION_NAME = "payments-relay-lock"
LISTEN_APPLICATION_NAME = "payments-relay-listen"


def _conninfo() -> str:
    """libpq connection string for the relay's dedicated connections."""
    return engine.url.set(drivername="postgresql").render_as_string(hide_password=False)


class RelayLock:
    """The D1 advisory lock, on a connection that never enters the pool.

    A session-level advisory lock belongs to the connection that took it. A
    pooled connection outlives its checkout, so a lock taken on one would
    survive "closing" it and be silently inherited by the next code that
    checked that connection out, in the same process. This connection is
    opened for the lock alone, released with pg_advisory_unlock, and closed,
    never returned anywhere.
    """

    def __init__(self, conninfo: str | None = None) -> None:
        self._conninfo = conninfo or _conninfo()
        self._conn = None
        self.backend_pid: int | None = None

    def try_acquire(self) -> bool:
        import psycopg

        conn = psycopg.connect(
            self._conninfo,
            autocommit=True,
            connect_timeout=5,
            application_name=LOCK_APPLICATION_NAME,
        )
        try:
            acquired = conn.execute(
                "SELECT pg_try_advisory_lock(%s)", (RELAY_LOCK_ID,)
            ).fetchone()[0]
        except Exception:
            conn.close()
            raise
        if not acquired:
            conn.close()
            return False
        self._conn = conn
        self.backend_pid = conn.info.backend_pid
        return True

    def held(self) -> bool:
        """Ask the server, on the lock's own connection, whether it still holds
        the lock. False if the connection is gone, for instance terminated."""
        if self._conn is None or self._conn.closed:
            return False
        try:
            return bool(
                self._conn.execute(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' "
                    "AND objid = %s AND pid = pg_backend_pid() AND granted)",
                    (RELAY_LOCK_ID,),
                ).fetchone()[0]
            )
        except Exception as exc:
            logger.error("relay lock connection failed: %s", exc)
            return False

    def release(self) -> None:
        """pg_advisory_unlock, then close. Safe on a dead connection."""
        conn, self._conn = self._conn, None
        if conn is None:
            return
        try:
            if not conn.closed:
                conn.execute("SELECT pg_advisory_unlock(%s)", (RELAY_LOCK_ID,))
                logger.info("relay lock %d released", RELAY_LOCK_ID)
        except Exception as exc:
            # The connection is already gone, and the server released the
            # lock with it.
            logger.warning("relay lock connection was already closed: %s", exc)
        finally:
            conn.close()
            self.backend_pid = None


def _acquire_relay_lock(stop: threading.Event, retry_seconds: float) -> RelayLock | None:
    """Block until this process holds the relay lock, or until told to stop."""
    announced = False
    while not stop.is_set():
        metrics.heartbeat()
        lock = RelayLock()
        try:
            acquired = lock.try_acquire()
        except Exception:
            logger.exception("relay could not reach the database to take its lock")
            acquired = False
        if acquired:
            logger.info(
                "relay lock %d acquired (backend pid %s); this relay publishes",
                RELAY_LOCK_ID,
                lock.backend_pid,
            )
            metrics.RELAY_LOCK_HELD.set(1)
            return lock
        if not announced:
            logger.warning(
                "another relay holds the relay lock %d; waiting, not publishing",
                RELAY_LOCK_ID,
            )
            announced = True
        stop.wait(retry_seconds)
    return None


class OutboxListener:
    """LISTEN on the outbox channel, on a connection of its own.

    A wake-up, never a source of truth. `wait` returns as soon as a payment
    commits, or after the poll interval if nothing arrives, and the relay
    polls either way. A notification sent while this connection was down is
    simply missed: the next poll finds the row. If LISTEN cannot be set up,
    `wait` degrades to a plain sleep and retries LISTEN on the next call.

    It does not touch the advisory lock connection, so D1 is unchanged.
    """

    def __init__(self, conninfo: str | None = None) -> None:
        self._conninfo = conninfo or _conninfo()
        self._conn = None
        self.notifications = 0

    def _connect(self):
        import psycopg

        conn = psycopg.connect(
            self._conninfo,
            autocommit=True,
            connect_timeout=5,
            application_name=LISTEN_APPLICATION_NAME,
        )
        conn.execute(f"LISTEN {outbox_repo.NOTIFY_CHANNEL}")
        logger.info("relay listening on channel %r", outbox_repo.NOTIFY_CHANNEL)
        return conn

    def wait(self, timeout: float, stop: threading.Event) -> bool:
        """Block up to `timeout` seconds. True if a payment committed meanwhile."""
        import psycopg

        try:
            if self._conn is None or self._conn.closed:
                self._conn = self._connect()
            for _ in self._conn.notifies(timeout=timeout, stop_after=1):
                self.notifications += 1
                return True
            return False
        except psycopg.Error as exc:
            logger.warning("relay LISTEN unavailable, polling only for now: %s", exc)
            self.close()
            stop.wait(timeout)
            return False

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # pragma: no cover - closing a broken connection
                pass
            self._conn = None


def _publish_while_locked(
    lock: RelayLock,
    producer: Producer,
    stop: threading.Event,
    *,
    interval: float,
    listener: OutboxListener | None,
) -> None:
    backoff = interval

    def idle_wait(seconds: float) -> None:
        if listener is not None:
            listener.wait(seconds, stop)
        else:
            stop.wait(seconds)

    while not stop.is_set():
        metrics.heartbeat()
        # Confirmed with the server before every batch, never from a local
        # flag. If the lock's connection was terminated, another relay may
        # already hold the lock: stop here, before claiming anything.
        if not lock.held():
            logger.error(
                "relay lost lock %d; stopped publishing, waiting to reacquire",
                RELAY_LOCK_ID,
            )
            return
        try:
            result = run_once(producer)
        except Exception:
            # A database problem, not a publish problem. Back off and
            # keep the process alive; the rows are safe where they are.
            logger.exception("relay tick failed")
            stop.wait(min(backoff * 2, 5.0))
            backoff = min(backoff * 2, 5.0)
            continue

        backoff = interval

        if result.published:
            logger.info(
                "relay published %d/%d event(s)", result.published, result.claimed
            )
        if result.had_failure:
            # Broker trouble. Wait before hammering it again.
            stop.wait(min(interval * 5, 5.0))
        elif result.claimed == 0:
            # Idle. Sleep until the next poll, or until a payment commits.
            idle_wait(interval)


def run_forever(
    producer: Producer | None = None,
    stop: threading.Event | None = None,
    *,
    lock_retry_seconds: float = LOCK_RETRY_SECONDS,
    poll_interval: float | None = None,
    listen: bool | None = None,
) -> None:
    """Poll until told to stop, publishing only while holding the relay lock.

    Polling is the correctness guarantee: a missed notification must never
    strand a row. With `listen` (default: settings.outbox_listen_enabled)
    the idle wait between polls also ends the moment a payment commits,
    which cuts publish delay from up to one poll interval to milliseconds.

    run_once does not take the lock; it is the unit the tests drive.
    """
    stop = stop or threading.Event()
    interval = (
        poll_interval if poll_interval is not None else settings.outbox_poll_interval_ms / 1000.0
    )
    listen = settings.outbox_listen_enabled if listen is None else listen
    logger.info(
        "relay started: topic=%s batch=%d interval=%.3fs listen=%s",
        topics.MAIN_TOPIC,
        settings.outbox_batch_size,
        interval,
        listen,
    )
    listener = OutboxListener() if listen else None
    try:
        while not stop.is_set():
            lock = _acquire_relay_lock(stop, lock_retry_seconds)
            if lock is None:
                break
            try:
                # Built only once this relay is the one publishing.
                producer = producer or build_producer()
                _publish_while_locked(
                    lock, producer, stop, interval=interval, listener=listener
                )
            finally:
                lock.release()
                metrics.RELAY_LOCK_HELD.set(0)
    finally:
        if listener is not None:
            listener.close()
        if producer is not None:
            producer.close()
        logger.info("relay stopped")


def main() -> None:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop = threading.Event()

    def _handle(signum, _frame) -> None:
        logger.info("signal %s received, draining", signum)
        stop.set()

    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)

    setup_tracing("relay")
    metrics.start_metrics_server("relay")
    try:
        run_forever(stop=stop)
    finally:
        shutdown_tracing()


if __name__ == "__main__":
    main()
