#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from sqlmodel import Session

from antidengue_automation.storage_lifecycle import evict_verified_antidengue_cache
from automation_core.config import get_settings
from automation_core.database import engine


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evict verified AntiDengue local cache while preserving RustFS objects."
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--older-than-hours", type=int)
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    with Session(engine) as session:
        result = evict_verified_antidengue_cache(
            session,
            settings=get_settings(),
            older_than_hours=args.older_than_hours,
            apply=args.apply,
            limit=max(1, args.limit),
        )
    print(json.dumps({"dry_run": not args.apply, **result}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
