from __future__ import annotations

import argparse
import asyncio
import re
from datetime import datetime
from pathlib import Path

from .membership_audit import fetch_group_snapshot, generate_not_member_report


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Create an Excel report of applicants not currently in a WhatsApp group.")
    result.add_argument("--file", required=True, type=Path)
    result.add_argument("--group-jid", required=True)
    result.add_argument("--output", type=Path)
    result.add_argument("--column", default="contact_no")
    result.add_argument("--sheet")
    result.add_argument("--country-code", default="92")
    result.add_argument("--trunk-prefix", default="0")
    result.add_argument("--nats-url", default="nats://localhost:4222")
    result.add_argument("--worker-id", default="default")
    result.add_argument("--health-subject")
    result.add_argument("--members-subject")
    result.add_argument("--worker-ready-timeout", type=int, default=120)
    return result


def run(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if not args.group_jid.endswith("@g.us"):
        print("ERROR: group JID must end with @g.us")
        return 2
    output = args.output
    health_subject = args.health_subject or (
        "whatsapp.worker.health"
        if args.worker_id == "default"
        else f"whatsapp.worker.{args.worker_id}.health"
    )
    members_subject = args.members_subject or (
        "whatsapp.worker.group-members"
        if args.worker_id == "default"
        else f"whatsapp.worker.{args.worker_id}.group-members"
    )
    if output is None:
        safe_group = re.sub(r"[^A-Za-z0-9_-]+", "-", args.group_jid.split("@", 1)[0])
        output = Path("reports") / f"{args.file.stem}-not-members-{safe_group}-{datetime.now():%Y%m%d-%H%M%S}.xlsx"
    try:
        snapshot = asyncio.run(
            fetch_group_snapshot(
                nats_url=args.nats_url,
                health_subject=health_subject,
                members_subject=members_subject,
                group_jid=args.group_jid,
                ready_timeout=args.worker_ready_timeout,
                progress=lambda message: print(message, flush=True),
            )
        )
        summary = generate_not_member_report(
            args.file.resolve(),
            output.resolve(),
            snapshot,
            column_name=args.column,
            sheet_name=args.sheet,
            country_code=args.country_code,
            trunk_prefix=args.trunk_prefix,
        )
    except Exception as exc:
        print(f"ERROR: membership audit failed: {exc}")
        return 1
    print(
        f"Audit complete: source={summary.source_rows}, members={summary.matched_members}, "
        f"not_members={summary.not_members}, invalid={summary.invalid_contacts}"
    )
    print(f"Report: {summary.output_path}")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
