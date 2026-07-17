#!/usr/bin/env python3
from __future__ import annotations

from collections import defaultdict

from sqlmodel import Session, select

from antidengue_automation.models import AntiDengueScheduleExecution
from antidengue_automation.scheduling import (
    ACTIVE_EXECUTION_STATUSES,
    cancel_execution,
    recover_orphaned_executions,
)
from automation_core.database import engine


def main() -> int:
    with Session(engine) as session:
        active = list(
            session.exec(
                select(AntiDengueScheduleExecution)
                .where(AntiDengueScheduleExecution.status.in_(sorted(ACTIVE_EXECUTION_STATUSES)))
                .order_by(AntiDengueScheduleExecution.created_at)
            )
        )
        groups: dict[tuple, list[AntiDengueScheduleExecution]] = defaultdict(list)
        for item in active:
            groups[(item.schedule_id, item.dispatch_profile_id, item.dispatch_policy, item.login_mode)].append(item)
        cancelled = 0
        for items in groups.values():
            for duplicate in items[1:]:
                cancel_execution(
                    session,
                    duplicate,
                    reason=f"Recovered duplicate active execution; superseded by {items[0].execution_code}.",
                )
                cancelled += 1
        stats = recover_orphaned_executions(session)

    print(
        "AntiDengue recovery: "
        f"duplicates_cancelled={cancelled}, "
        f"orphaned_failed={stats['failed']}, queued_recovered={stats['requeued']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
