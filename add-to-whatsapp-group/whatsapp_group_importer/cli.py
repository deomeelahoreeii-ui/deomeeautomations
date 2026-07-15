from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
from pathlib import Path

from .contacts import ContactInputError, load_contacts, select_contacts
from .publisher import publish_contacts
from .history import AttemptStore


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Select contacts from Excel and queue WhatsApp group additions."
    )
    result.add_argument("--file", required=True, type=Path, help="Input .xlsx file")
    result.add_argument("--group-jid", help="Target WhatsApp group JID ending in @g.us")
    result.add_argument("--column", default="contact_no")
    result.add_argument("--sheet")
    result.add_argument("--selection", choices=("last", "first", "range", "all"), default="last")
    result.add_argument("--count", type=int, default=50)
    result.add_argument("--start", type=int)
    result.add_argument("--end", type=int)
    result.add_argument("--country-code", default="92")
    result.add_argument("--trunk-prefix", default="0")
    result.add_argument("--nats-url", default="nats://localhost:4222")
    result.add_argument("--subject", default="whatsapp.pending")
    result.add_argument("--health-subject", default="whatsapp.worker.health")
    result.add_argument("--worker-ready-timeout", type=int, default=120)
    result.add_argument(
        "--database",
        type=Path,
        default=Path("group-import-history.sqlite3"),
        help="Persistent per-file/group attempt ledger",
    )
    result.add_argument("--delay-ms", type=int, default=15000)
    result.add_argument("--wait-timeout", type=int, default=1800)
    result.add_argument(
        "--consent-confirmed",
        action="store_true",
        help="Confirm recipients were informed and consented to group membership",
    )
    result.add_argument("--execute", action="store_true", help="Publish jobs; otherwise preview only")
    return result


def run(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        contacts, warnings = load_contacts(
            args.file.resolve(),
            column_name=args.column,
            sheet_name=args.sheet,
            country_code=args.country_code,
            trunk_prefix=args.trunk_prefix,
        )
        selected = select_contacts(contacts, args.selection, args.count, args.start, args.end)
    except ContactInputError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(f"Loaded {len(contacts)} valid unique contacts; selected {len(selected)}.")
    if selected:
        print(f"Selection: applicant positions {contacts.index(selected[0]) + 1}-{contacts.index(selected[-1]) + 1}, Excel rows {selected[0].source_row}-{selected[-1].source_row}")
    for warning in warnings:
        print(f"WARNING: {warning}")
    for contact in selected:
        print(f"  row {contact.source_row}: {contact.phone}")

    if not selected:
        print("ERROR: selection contains no contacts", file=sys.stderr)
        return 2
    pending = selected
    prior_attempts = {}
    if args.group_jid:
        with AttemptStore(args.database) as store:
            prior_attempts = store.attempted_contacts(
                args.file.resolve(), args.group_jid, selected
            )
        pending = [contact for contact in selected if contact.phone not in prior_attempts]
        if prior_attempts:
            print(
                f"Idempotency ledger skipped {len(prior_attempts)} previously attempted "
                f"contact(s); {len(pending)} contact(s) have never been attempted."
            )
            for contact in selected:
                previous = prior_attempts.get(contact.phone)
                if previous:
                    print(
                        f"  SKIP row {contact.source_row}: {contact.phone} "
                        f"(previous status: {previous.status})"
                    )
    print(
        f"Run plan: selected={len(selected)}, skipped={len(prior_attempts)}, "
        f"pending={len(pending)}, delay={args.delay_ms / 1000:g}s per contact.",
        flush=True,
    )
    if not args.execute:
        print("Preview only. Add --execute and --group-jid to queue these contacts.")
        return 0
    if not args.group_jid:
        print("ERROR: --group-jid is required with --execute", file=sys.stderr)
        return 2
    if os.environ.get("WHATSAPP_ALLOW_DIRECT_GROUP_ADDS", "").lower() not in {
        "1", "true", "yes", "on"
    }:
        print(
            "ERROR: direct group additions are disabled because WhatsApp invalidated "
            "linked sessions after participant-add operations. Run the membership audit "
            "and distribute an invite link instead.",
            file=sys.stderr,
        )
        return 2
    if not args.consent_confirmed:
        print(
            "ERROR: direct addition is blocked without --consent-confirmed. "
            "Use an office-distributed group invite link when recipients have not opted in.",
            file=sys.stderr,
        )
        return 2
    if args.delay_ms < 10_000:
        print("ERROR: execute mode requires a delay of at least 10000 ms", file=sys.stderr)
        return 2
    if args.wait_timeout <= 0:
        print("ERROR: execute mode requires a positive completion timeout", file=sys.stderr)
        return 2
    if args.worker_ready_timeout <= 0:
        print("ERROR: worker-ready timeout must be positive", file=sys.stderr)
        return 2
    if not pending:
        print(
            "Nothing to do: every selected contact already has an attempt record for "
            "this source file and group."
        )
        return 0

    try:
        with AttemptStore(args.database) as store:
            summary = asyncio.run(
                publish_contacts(
                    pending,
                    group_jid=args.group_jid,
                    nats_url=args.nats_url,
                    subject=args.subject,
                    delay_ms=args.delay_ms,
                    wait_timeout=args.wait_timeout,
                    attempt_store=store,
                    source_file=args.file.resolve(),
                    health_subject=args.health_subject,
                    worker_ready_timeout=args.worker_ready_timeout,
                    progress=lambda message: print(message, flush=True),
                )
            )
    except KeyboardInterrupt:
        print("Import stopped by operator; the active contact was recorded as interrupted.")
        return 130
    except Exception as exc:
        print(f"ERROR: could not queue group additions: {exc}", file=sys.stderr)
        return 1

    print(
        f"Batch {summary.batch_id}: requested={summary.requested}, queued={summary.queued}, "
        f"added/already-member={summary.delivered}, privacy-rejected={summary.rejected}, "
        f"failed={summary.failed}, "
        f"timeout={summary.timed_out}, halted/unqueued={summary.unqueued}"
    )
    if summary.rejected_contacts:
        report_path = Path(f"group-import-{summary.batch_id}-not-added.csv").resolve()
        with report_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(("excel_row", "phone", "reason"))
            for contact in summary.rejected_contacts:
                writer.writerow(
                    (contact.source_row, contact.phone, "WhatsApp privacy restriction (403); send invite link")
                )
        print(
            f"{summary.rejected} contact(s) could not be directly added because of WhatsApp "
            f"privacy restrictions. Invite report: {report_path}"
        )
    return 1 if summary.failed or summary.timed_out or summary.unqueued else 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
