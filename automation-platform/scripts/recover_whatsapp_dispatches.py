#!/usr/bin/env python3
from __future__ import annotations

import argparse

from sqlmodel import Session

from automation_core.database import engine
from whatsapp_gateway.dispatch.recovery import recover_active_dispatch_jobs


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Close orphaned WhatsApp dispatch attempts without risking a duplicate send."
        )
    )
    parser.add_argument(
        "--all-active",
        action="store_true",
        help="Close every active dispatch attempt during a confirmed stack restart.",
    )
    parser.add_argument("--older-than-minutes", type=int, default=20)
    args = parser.parse_args()

    with Session(engine) as session:
        stats = recover_active_dispatch_jobs(
            session,
            all_active=args.all_active,
            older_than_minutes=args.older_than_minutes,
            reason=(
                "The local development services restarted before this WhatsApp "
                "dispatch attempt completed."
                if args.all_active
                else None
            ),
        )
    print(
        "WhatsApp dispatch recovery: "
        + ", ".join(f"{key}={value}" for key, value in stats.items())
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
