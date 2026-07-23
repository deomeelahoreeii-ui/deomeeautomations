from __future__ import annotations

import argparse
import signal
import time

from sqlalchemy import text
from sqlmodel import Session

from antidengue_automation.scheduling import (
    advance_pending_executions,
    ensure_due_executions,
    recover_orphaned_executions,
)
from antidengue_automation.storage_lifecycle import evict_verified_antidengue_cache
from antidengue_automation.notifications import publish_pending_ntfy
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.database_identity import database_identity
from automation_core.logging_config import configure_service_logging
from automation_core.task_outbox import publish_pending_tasks
from automation_core.time import utcnow

_STOP = False
_settings = get_settings()
logger = configure_service_logging(level=_settings.log_level, log_format=_settings.log_format)


def _request_stop(signum: int, _frame) -> None:
    global _STOP
    _STOP = True
    logger.info(
        "scheduler.stop.requested",
        extra={"context": {"service": "antidengue-scheduler", "signal": signum}},
    )


def run_tick() -> dict[str, int]:
    """Advance durable state and publish only committed task-outbox rows."""
    # The advisory lock deliberately owns a different connection from every
    # unit of database work below. Binding a Session to this connection would
    # make its commits subordinate to the lock connection's outer transaction;
    # Celery could then receive a task before PostgreSQL committed its Job.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as lock_connection:
        acquired = True
        if lock_connection.dialect.name == "postgresql":
            acquired = bool(
                lock_connection.execute(text("SELECT pg_try_advisory_lock(7349202617)")).scalar()
            )
        if not acquired:
            return {"created": 0, "advanced": 0, "published": 0, "publish_failed": 0, "recovered": 0}
        try:
            # Existing durable publications are handled in their own session.
            with Session(engine) as session:
                recovery = recover_orphaned_executions(session)
                before = publish_pending_tasks(session)

            # Stage transitions commit their Job, execution link and outbox row
            # using an engine-owned transaction before any broker call occurs.
            with Session(engine) as session:
                created = ensure_due_executions(session, utcnow())
                advanced = advance_pending_executions(session)

            # A fresh connection can only see committed rows. Reaching Celery
            # from here is therefore an executable commit-before-publish guard.
            with Session(engine) as session:
                after = publish_pending_tasks(session)
            try:
                with Session(engine) as session:
                    notifications = publish_pending_ntfy(session)
            except Exception as exc:
                # Notification transport is downstream and must never block or
                # roll back report/preview/dispatch orchestration.
                logger.exception(
                    "scheduler.notification.failed",
                    extra={"context": {"error_type": type(exc).__name__}},
                )
                notifications = {"sent": 0, "failed": 1}
            return {
                "created": len(created),
                "advanced": advanced,
                "published": before["published"] + after["published"],
                "publish_failed": before["failed"] + after["failed"],
                "recovered": recovery["failed"] + recovery["requeued"],
                "notifications_sent": notifications["sent"],
                "notifications_failed": notifications["failed"],
            }
        finally:
            if lock_connection.dialect.name == "postgresql":
                lock_connection.execute(text("SELECT pg_advisory_unlock(7349202617)"))


def main() -> int:
    parser = argparse.ArgumentParser(description="PostgreSQL-backed AntiDengue schedule orchestrator")
    parser.add_argument("--interval", type=float, default=None, help="Polling interval in seconds")
    parser.add_argument("--once", action="store_true", help="Run one scheduler tick and exit")
    args = parser.parse_args()
    settings = _settings
    interval = float(args.interval if args.interval is not None else settings.antidengue_scheduler_interval_seconds)
    if interval < 1:
        parser.error("--interval must be at least 1 second")

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    identity = database_identity()
    logger.info(
        "scheduler.started",
        extra={
            "context": {
                "service": "antidengue-scheduler",
                "interval_seconds": interval,
                "timezone": "Asia/Karachi",
                "database_fingerprint": identity["fingerprint"],
            }
        },
    )
    next_retention_at = 0.0
    while not _STOP:
        try:
            stats = run_tick()
            monotonic_now = time.monotonic()
            if settings.antidengue_retention_enabled and monotonic_now >= next_retention_at:
                with Session(engine) as session:
                    retention = evict_verified_antidengue_cache(
                        session,
                        settings=settings,
                        apply=True,
                    )
                next_retention_at = monotonic_now + max(
                    60,
                    settings.antidengue_retention_interval_seconds,
                )
                if retention["evicted"] or retention["source_files_evicted"]:
                    logger.info(
                        "scheduler.antidengue.cache_evicted",
                        extra={"context": retention},
                    )
            if any(stats.values()):
                logger.info(
                    "scheduler.tick.changed",
                    extra={"context": {key: value for key, value in stats.items() if value}},
                )
        except Exception as exc:
            logger.exception(
                "scheduler.tick.failed",
                extra={"context": {"error_type": type(exc).__name__}},
            )
        if args.once:
            break
        deadline = time.monotonic() + interval
        while not _STOP and time.monotonic() < deadline:
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
    logger.info("scheduler.stopped", extra={"context": {"service": "antidengue-scheduler"}})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
