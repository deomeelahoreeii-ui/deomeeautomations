#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from sqlmodel import Session

from automation_core.database import engine
from whatsapp_gateway.inbound.content_duplicates import (
    reconcile_existing_content_duplicates,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reconcile exact CRM evidence duplicates without deleting source captures."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the reconciliation. The default is a rolled-back dry run.",
    )
    args = parser.parse_args()
    with Session(engine) as session:
        report = reconcile_existing_content_duplicates(session)
        if args.apply:
            session.commit()
        else:
            session.rollback()
    print(
        json.dumps(
            {
                "mode": "applied" if args.apply else "dry-run",
                **report,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
