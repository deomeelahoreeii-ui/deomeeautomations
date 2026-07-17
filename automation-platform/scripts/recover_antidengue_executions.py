#!/usr/bin/env python3
from __future__ import annotations

from sqlmodel import Session

from antidengue_automation.scheduling import recover_orphaned_executions
from automation_core.database import engine


def main() -> int:
    with Session(engine) as session:
        stats = recover_orphaned_executions(session)

    print(
        "AntiDengue recovery: "
        f"duplicates_cancelled={stats['duplicates']}, "
        f"orphaned_failed={stats['failed']}, queued_recovered={stats['requeued']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
