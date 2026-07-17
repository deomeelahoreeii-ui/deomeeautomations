from __future__ import annotations

import argparse
import signal
import sys
import time

from sqlalchemy import text
from sqlmodel import Session

from antidengue_automation.scheduling import (
    advance_pending_executions,
    ensure_due_executions,
    recover_orphaned_executions,
)
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.database_identity import database_identity
from automation_core.task_outbox import publish_pending_tasks
from automation_core.time import utcnow

_STOP = False


def _request_stop(signum: int, _frame) -> None:
    global _STOP
    _STOP = True
    print(f"AntiDengue scheduler received signal {signum}; stopping.", flush=True)


def run_tick() -> dict[str, int]:
    """Advance durable state and publish only committed task-outbox rows."""
    with engine.connect() as connection:
        acquired = True
        if connection.dialect.name == "postgresql":
            acquired = bool(connection.execute(text("SELECT pg_try_advisory_lock(7349202617)")).scalar())
        if not acquired:
            return {"created": 0, "advanced": 0, "published": 0, "publish_failed": 0, "recovered": 0}
        try:
            with Session(bind=connection) as session:
                recovery = recover_orphaned_executions(session)
                before = publish_pending_tasks(session)
                created = ensure_due_executions(session, utcnow())
                advanced = advance_pending_executions(session)
                after = publish_pending_tasks(session)
            return {
                "created": len(created),
                "advanced": advanced,
                "published": before["published"] + after["published"],
                "publish_failed": before["failed"] + after["failed"],
                "recovered": recovery["failed"] + recovery["requeued"],
            }
        finally:
            if connection.dialect.name == "postgresql":
                connection.execute(text("SELECT pg_advisory_unlock(7349202617)"))


def main() -> int:
    parser = argparse.ArgumentParser(description="PostgreSQL-backed AntiDengue schedule orchestrator")
    parser.add_argument("--interval", type=float, default=None, help="Polling interval in seconds")
    parser.add_argument("--once", action="store_true", help="Run one scheduler tick and exit")
    args = parser.parse_args()
    settings = get_settings()
    interval = float(args.interval if args.interval is not None else settings.antidengue_scheduler_interval_seconds)
    if interval < 1:
        parser.error("--interval must be at least 1 second")

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    identity = database_identity()
    print(
        "AntiDengue scheduler started: "
        f"interval={interval:g}s, timezone=Asia/Karachi, "
        f"database={identity['fingerprint']} ({identity['display']})",
        flush=True,
    )
    while not _STOP:
        try:
            stats = run_tick()
            if any(stats.values()):
                print(
                    "AntiDengue scheduler change: "
                    + ", ".join(f"{key}={value}" for key, value in stats.items() if value),
                    flush=True,
                )
        except Exception as exc:
            print(f"AntiDengue scheduler tick failed: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        if args.once:
            break
        deadline = time.monotonic() + interval
        while not _STOP and time.monotonic() < deadline:
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
    print("AntiDengue scheduler stopped.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
