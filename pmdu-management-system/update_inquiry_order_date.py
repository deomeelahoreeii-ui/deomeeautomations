from __future__ import annotations

import argparse
import sqlite3
from datetime import date, datetime
from pathlib import Path

from generate_inquiry_letters import add_working_days, parse_issue_date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update inquiry order issue/due dates with an audit event.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--complaint", help="Complaint number to update")
    target.add_argument("--all", action="store_true", help="Update all open inquiry orders")
    parser.add_argument("--issue-date", required=True, help="New issue date in YYYY-MM-DD format")
    parser.add_argument("--school-reply-working-days", type=int, default=2)
    parser.add_argument("--ddeo-report-working-days", type=int, default=3)
    parser.add_argument("--output-root", default="inquiry_letters_to_ddeos")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true", help="Apply the update")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.dry_run and not args.yes:
        raise SystemExit("Use --dry-run to preview or --yes to apply.")

    project_root = Path(__file__).resolve().parent
    db_path = project_root / args.output_root / "inquiry_letters.sqlite3"
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    issue: date = parse_issue_date(args.issue_date)
    school_due = add_working_days(issue, args.school_reply_working_days)
    ddeo_due = add_working_days(issue, args.ddeo_report_working_days)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        if args.all:
            rows = conn.execute(
                """
                SELECT *
                FROM inquiry_orders
                WHERE order_status = 'issued'
                  AND closed_at IS NULL
                ORDER BY complaint_code, version
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT *
                FROM inquiry_orders
                WHERE complaint_code = ?
                  AND order_status = 'issued'
                  AND closed_at IS NULL
                ORDER BY version DESC
                """,
                [args.complaint],
            ).fetchall()

        if not rows:
            print("No matching open inquiry orders found.")
            return

        event_at = datetime.now().astimezone().isoformat(timespec="seconds")
        for row in rows:
            print(
                f"{row['complaint_code']} v{row['version']}: "
                f"{row['issue_date']} -> {issue.isoformat()} | "
                f"school due {school_due.isoformat()} | DDEO due {ddeo_due.isoformat()}"
            )
            if args.dry_run:
                continue
            conn.execute(
                """
                UPDATE inquiry_orders
                SET issue_date = ?,
                    school_reply_due_date = ?,
                    ddeo_report_due_date = ?
                WHERE id = ?
                """,
                [issue.isoformat(), school_due.isoformat(), ddeo_due.isoformat(), row["id"]],
            )
            conn.execute(
                """
                INSERT INTO inquiry_events (
                    complaint_code, version, event_type, event_at, notes
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    row["complaint_code"],
                    row["version"],
                    "issue_date_updated",
                    event_at,
                    (
                        f"issue_date {row['issue_date']} -> {issue.isoformat()}; "
                        f"school_reply_due_date -> {school_due.isoformat()}; "
                        f"ddeo_report_due_date -> {ddeo_due.isoformat()}"
                    ),
                ],
            )
        if not args.dry_run:
            conn.commit()
            print(f"Updated {len(rows)} inquiry order(s).")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
