#!/usr/bin/env python3
from __future__ import annotations

import argparse

from sqlmodel import Session

from automation_core.database import engine
from crm_filters.job_recovery import fail_all_active_crm_jobs, fail_stale_crm_jobs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Close orphaned CRM jobs so they do not remain queued/running forever."
    )
    parser.add_argument(
        "--all-active",
        action="store_true",
        help="Close every active CRM job. Use only when restarting the local development stack.",
    )
    parser.add_argument(
        "--older-than-minutes",
        type=int,
        default=15,
        help="Otherwise close jobs that have not reported progress for this many minutes.",
    )
    args = parser.parse_args()

    with Session(engine) as session:
        if args.all_active:
            jobs = fail_all_active_crm_jobs(
                session,
                reason=(
                    "The local development services were restarted before this CRM run completed. "
                    "The previous run was closed automatically; start a new run."
                ),
            )
        else:
            jobs = fail_stale_crm_jobs(
                session,
                older_than_minutes=args.older_than_minutes,
            )

    if jobs:
        print(f"Recovered {len(jobs)} orphaned CRM job(s):")
        for job in jobs:
            print(f"  {job.id}  {job.type}  -> failed")
    else:
        print("No orphaned CRM jobs were found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
