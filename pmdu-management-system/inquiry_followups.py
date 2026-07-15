from __future__ import annotations

import argparse
import csv
import html
import logging
import sqlite3
import subprocess
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import orjson

from main import load_paperless_settings
from notify_under_investigation import (
    OfficerRow,
    complaint_code,
    complaint_has_pmdu_source,
    complaint_matches_tehsil,
    decoded_custom_fields,
    download_documents_for_complaint,
    load_json,
    load_officers,
    normalize_tehsil,
    option_maps,
    preview_status_and_issues,
    publish_jobs,
    related_document_ids,
    recipient_target,
    render_template,
    staged_pdf_for_merge,
)
from paperless import PaperlessClient, clean_text

LOGGER_NAME = "pmdu_automation"
FOLLOWUP_TYPES = {"reminder", "warning", "final_warning"}
RECIPIENT_ROLES = {"ddeo", "aeo", "deo_mee"}


@dataclass(frozen=True)
class FollowupSettings:
    action: str
    followup_type: str
    recipient_role: str
    scope: str
    tehsil: str | None
    complaint: str | None
    max_cases: int | None
    force: bool
    allow_second_today: bool
    files: str
    policy_path: Path
    output_root: Path
    notification_config_path: Path


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(LOGGER_NAME)


def parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.combine(date.fromisoformat(text[:10]), time.min)


def now_local() -> datetime:
    return datetime.now().astimezone()


def elapsed_human(started_at: datetime, now: datetime) -> str:
    delta = now - started_at.astimezone(now.tzinfo) if started_at.tzinfo else now.replace(tzinfo=None) - started_at
    total_hours = max(0, int(delta.total_seconds() // 3600))
    days, hours = divmod(total_hours, 24)
    if days and hours:
        return f"{days} day(s) {hours} hour(s)"
    if days:
        return f"{days} day(s)"
    return f"{hours} hour(s)"


def init_followup_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS followup_runs (
            batch_id TEXT PRIMARY KEY,
            action TEXT NOT NULL,
            followup_type TEXT NOT NULL,
            recipient_role TEXT NOT NULL,
            scope TEXT NOT NULL,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            candidate_count INTEGER NOT NULL DEFAULT 0,
            job_count INTEGER NOT NULL DEFAULT 0,
            published_count INTEGER NOT NULL DEFAULT 0,
            settings_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS followup_candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            order_id INTEGER,
            complaint_code TEXT NOT NULL,
            recipient_role TEXT NOT NULL,
            recipient_name TEXT,
            recipient_target TEXT,
            status TEXT NOT NULL,
            reason TEXT,
            elapsed_hours INTEGER,
            reminders_today INTEGER NOT NULL DEFAULT 0,
            last_sent_at TEXT,
            message_hash TEXT,
            job_id TEXT,
            stored_tehsil TEXT,
            resolved_tehsil TEXT,
            recipient_resolution_source TEXT
        );

        CREATE TABLE IF NOT EXISTS followup_deliveries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            delivery_id TEXT NOT NULL UNIQUE,
            batch_id TEXT NOT NULL,
            order_id INTEGER,
            complaint_code TEXT NOT NULL,
            followup_type TEXT NOT NULL,
            recipient_role TEXT NOT NULL,
            recipient_name TEXT,
            recipient_target TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            delivery_enabled INTEGER NOT NULL,
            nats_subject TEXT,
            message_hash TEXT,
            job_payload TEXT NOT NULL,
            resolved_tehsil TEXT,
            recipient_resolution_source TEXT
        );
        """
    )
    candidate_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(followup_candidates)")
    }
    for column, definition in (
        ("stored_tehsil", "TEXT"),
        ("resolved_tehsil", "TEXT"),
        ("recipient_resolution_source", "TEXT"),
    ):
        if column not in candidate_columns:
            conn.execute(
                f"ALTER TABLE followup_candidates ADD COLUMN {column} {definition}"
            )
    delivery_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(followup_deliveries)")
    }
    for column, definition in (
        ("resolved_tehsil", "TEXT"),
        ("recipient_resolution_source", "TEXT"),
    ):
        if column not in delivery_columns:
            conn.execute(
                f"ALTER TABLE followup_deliveries ADD COLUMN {column} {definition}"
            )
    conn.commit()


def latest_order_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT *
        FROM inquiry_orders AS orders
        WHERE version = (
            SELECT MAX(version)
            FROM inquiry_orders
            WHERE complaint_code = orders.complaint_code
              AND letter_kind = orders.letter_kind
        )
          AND order_status = 'issued'
          AND closed_at IS NULL
          AND report_received_at IS NULL
        ORDER BY issue_date, complaint_code
        """
    ).fetchall()


def count_sent_today(
    conn: sqlite3.Connection,
    complaint: str,
    followup_type: str,
    recipient_role: str,
    target: str,
    now: datetime,
) -> int:
    start = datetime.combine(now.date(), time.min).astimezone(now.tzinfo).isoformat(timespec="seconds")
    end = datetime.combine(now.date() + timedelta(days=1), time.min).astimezone(now.tzinfo).isoformat(timespec="seconds")
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM followup_deliveries
        WHERE complaint_code = ?
          AND followup_type = ?
          AND recipient_role = ?
          AND recipient_target = ?
          AND delivery_enabled = 1
          AND sent_at >= ?
          AND sent_at < ?
        """,
        [complaint, followup_type, recipient_role, target, start, end],
    ).fetchone()
    return int(row[0] or 0)


def last_sent_at(
    conn: sqlite3.Connection,
    complaint: str,
    followup_type: str,
    recipient_role: str,
    target: str,
) -> datetime | None:
    row = conn.execute(
        """
        SELECT sent_at
        FROM followup_deliveries
        WHERE complaint_code = ?
          AND followup_type = ?
          AND recipient_role = ?
          AND recipient_target = ?
          AND delivery_enabled = 1
        ORDER BY sent_at DESC
        LIMIT 1
        """,
        [complaint, followup_type, recipient_role, target],
    ).fetchone()
    return parse_dt(row[0]) if row and row[0] else None


def has_previous_type(
    conn: sqlite3.Connection,
    complaint: str,
    previous_type: str,
    recipient_role: str,
    target: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM followup_deliveries
        WHERE complaint_code = ?
          AND followup_type = ?
          AND recipient_role = ?
          AND recipient_target = ?
          AND delivery_enabled = 1
        LIMIT 1
        """,
        [complaint, previous_type, recipient_role, target],
    ).fetchone()
    return bool(row)


def officer_recipient(
    tehsil: str,
    officers: list[OfficerRow],
    role: str,
) -> dict[str, str] | None:
    tehsil = normalize_tehsil(tehsil)
    officer = next((item for item in officers if item.tehsil == tehsil), None)
    if not officer:
        return None
    if role == "ddeo":
        name, number = officer.ddeo_name, officer.ddeo_number
    elif role == "aeo":
        name, number = officer.aeo_name, officer.aeo_number
    elif role == "deo_mee":
        name, number = officer.deo_name, officer.deo_number
    else:
        return None
    if not number:
        return None
    return {"role": role, "name": name or role.upper(), "target": recipient_target(number)}


async def current_paperless_documents(
    project_root: Path,
) -> tuple[
    dict[str, tuple[dict[str, Any], dict[str, Any]]],
    list[dict[str, Any]],
    dict[int, str],
]:
    settings = load_paperless_settings(project_root)
    results: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    async with PaperlessClient(settings) as client:
        custom_fields = await client.paginated_results("/api/custom_fields/?page_size=100")
        field_names, option_labels = option_maps(custom_fields)
        documents = await client.paginated_results("/api/documents/?page_size=100")
        for document in documents:
            fields = decoded_custom_fields(document, field_names, option_labels)
            if clean_text(fields.get("Document Role")).lower() != "main complaint":
                continue
            if not complaint_has_pmdu_source(fields):
                continue
            code = complaint_code(fields, document)
            if code:
                if code in results:
                    previous_document = results[code][0]
                    raise RuntimeError(
                        "Paperless contains multiple Main Complaint documents for "
                        f"complaint number {code} (document ids "
                        f"{previous_document.get('id')} and {document.get('id')}). "
                        "Follow-up routing refused."
                    )
                results[code] = (document, fields)
    return results, documents, field_names


async def build_combined_followup_package(
    client: PaperlessClient,
    main_document: dict[str, Any],
    all_documents: list[dict[str, Any]],
    field_names: dict[int, str],
    inquiry_order_path: Path,
    package_root: Path,
    code: str,
) -> list[dict[str, str]]:
    logger = logging.getLogger(LOGGER_NAME)
    documents_by_id = {int(document["id"]): document for document in all_documents}
    related = [
        documents_by_id[document_id]
        for document_id in related_document_ids(
            main_document, all_documents, field_names
        )
        if document_id in documents_by_id
    ]
    downloaded = await download_documents_for_complaint(
        client,
        main_document,
        related,
        package_root / "downloads",
        True,
        True,
        "",
        code,
    )
    input_pdfs: list[Path] = []
    if inquiry_order_path.exists():
        input_pdfs.append(inquiry_order_path)
    else:
        logger.warning(
            "Inquiry order PDF missing for follow-up package %s: %s",
            code,
            inquiry_order_path,
        )
    for document_payload in downloaded:
        staged = staged_pdf_for_merge(document_payload, package_root)
        if staged:
            input_pdfs.append(staged)
        else:
            logger.warning(
                "Skipping unsupported file in follow-up package %s: %s",
                code,
                document_payload.get("path"),
            )
    if not input_pdfs:
        return []
    combined_path = package_root / f"Complete_Inquiry_Package_{code}.pdf"
    combined_path.parent.mkdir(parents=True, exist_ok=True)
    command = ["qpdf", "--empty", "--pages"]
    for input_pdf in input_pdfs:
        command.extend([str(input_pdf), "1-z"])
    command.extend(["--", str(combined_path)])
    subprocess.run(command, check=True, capture_output=True)
    return [
        {
            "path": str(combined_path.resolve()),
            "filename": combined_path.name,
            "mimetype": "application/pdf",
        }
    ]


def eligible_paperless(fields: dict[str, Any]) -> bool:
    if clean_text(fields.get("Status")).lower() != "under investigation":
        return False
    return complaint_has_pmdu_source(fields)


def message_values(
    row: sqlite3.Row,
    recipient: dict[str, str],
    now: datetime,
    resolved_tehsil: str,
    paperless_fields: dict[str, Any],
) -> dict[str, str]:
    issue = parse_dt(row["issue_date"]) or now
    return {
        "recipient_name": recipient["name"],
        "recipient_role": recipient["role"].upper(),
        "complaint_code": clean_text(row["complaint_code"]),
        "school_name": clean_text(paperless_fields.get("The Respondent Name"))
        or clean_text(row["school_name"])
        or "School/Respondent Concerned",
        "tehsil": resolved_tehsil,
        "issue_date": clean_text(row["issue_date"]),
        "elapsed_human": elapsed_human(issue, now),
    }


def message_hash(text: str, documents: list[dict[str, str]]) -> str:
    import hashlib

    payload = {
        "text": text,
        "documents": [
            {"path": item.get("path"), "filename": item.get("filename")}
            for item in documents
        ],
    }
    return hashlib.sha256(orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)).hexdigest()


def documents_for_row(
    row: sqlite3.Row,
    policy: dict[str, Any],
    files: str,
) -> list[dict[str, str]]:
    if files == "none" or not policy.get("attach_inquiry_order", True):
        return []
    pdf_path = Path(clean_text(row["pdf_path"]))
    if not pdf_path.is_absolute():
        pdf_path = Path.cwd() / pdf_path
    return [
        {
            "path": str(pdf_path),
            "filename": pdf_path.name,
            "mimetype": "application/pdf",
        }
    ]


def candidate_decision(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    policy: dict[str, Any],
    settings: FollowupSettings,
    recipient: dict[str, str],
    now: datetime,
) -> tuple[str, str | None, int, int, datetime | None]:
    issue = parse_dt(row["issue_date"])
    if not issue:
        return "blocked", "inquiry order has no valid issue date", 0, 0, None
    elapsed_hours = int(max(0, (now.replace(tzinfo=None) - issue.replace(tzinfo=None)).total_seconds() // 3600))
    eligible_after = int(policy.get("eligible_after_hours", 0) or 0)
    if elapsed_hours < eligible_after and not settings.force:
        return "skipped", f"not due yet: {elapsed_hours}h elapsed, policy requires {eligible_after}h", elapsed_hours, 0, None

    previous_type = clean_text(policy.get("requires_previous_type"))
    if previous_type and not settings.force and not has_previous_type(
        conn, row["complaint_code"], previous_type, recipient["role"], recipient["target"]
    ):
        return "skipped", f"requires previous {previous_type}", elapsed_hours, 0, None

    today = count_sent_today(
        conn,
        row["complaint_code"],
        settings.followup_type,
        recipient["role"],
        recipient["target"],
        now,
    )
    max_per_day = int(policy.get("max_per_day", 1) or 1)
    if settings.allow_second_today:
        max_per_day = max(max_per_day, 2)
    if today >= max_per_day and not settings.force:
        return "skipped", f"daily limit reached: {today}/{max_per_day}", elapsed_hours, today, None

    last = last_sent_at(
        conn,
        row["complaint_code"],
        settings.followup_type,
        recipient["role"],
        recipient["target"],
    )
    min_hours = int(policy.get("min_hours_between_same_type", 0) or 0)
    if last and not settings.force:
        age = (now.replace(tzinfo=None) - last.replace(tzinfo=None)).total_seconds() / 3600
        if age < min_hours:
            return "skipped", f"sent too recently: {age:.1f}h ago, policy requires {min_hours}h", elapsed_hours, today, last

    return "ready", None, elapsed_hours, today, last


def preview_root(project_root: Path) -> Path:
    return project_root / "reports" / "pmdu" / "followup_previews"


def write_preview(
    project_root: Path,
    batch_id: str,
    settings: FollowupSettings,
    jobs: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> Path:
    now = now_local()
    batch_dir = preview_root(project_root) / now.strftime("%Y") / now.strftime("%m") / now.strftime("%d") / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    statuses = {str(job["job_id"]): preview_status_and_issues(job) for job in jobs}
    ready = sum(1 for status, _ in statuses.values() if status == "ready")
    warning = sum(1 for status, _ in statuses.values() if status == "warning")
    blocked = sum(1 for status, _ in statuses.values() if status == "blocked")
    skipped = sum(1 for item in candidates if item["status"] == "skipped")
    summary = {
        "batch_id": batch_id,
        "created_at": now.isoformat(timespec="seconds"),
        "action": settings.action,
        "followup_type": settings.followup_type,
        "recipient_role": settings.recipient_role,
        "scope": settings.scope,
        "job_count": len(jobs),
        "ready_count": ready,
        "warning_count": warning,
        "blocked_count": blocked,
        "skipped_count": skipped,
    }
    (batch_dir / "summary.json").write_bytes(orjson.dumps(summary, option=orjson.OPT_INDENT_2))
    with (batch_dir / "jobs.jsonl").open("w", encoding="utf-8") as handle:
        for job in jobs:
            status, issues = statuses[str(job["job_id"])]
            handle.write(orjson.dumps({**job, "preview_status": status, "preview_issues": issues}).decode("utf-8") + "\n")
    with (batch_dir / "candidates.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[
            "complaint_code", "recipient_role", "recipient_name", "recipient_target",
            "stored_tehsil", "resolved_tehsil", "recipient_resolution_source",
            "status", "reason", "elapsed_hours", "reminders_today", "last_sent_at",
        ])
        writer.writeheader()
        for item in candidates:
            writer.writerow({key: item.get(key, "") for key in writer.fieldnames})
    cards = []
    for job in jobs:
        status, issues = statuses[str(job["job_id"])]
        preview_text = clean_text(job.get("text"))
        if not preview_text and job.get("documents"):
            preview_text = clean_text((job["documents"][0] or {}).get("caption"))
        docs = "".join(
            f"<li><a href=\"{html.escape(Path(doc['path']).as_uri(), quote=True)}\">{html.escape(clean_text(doc.get('filename')))}</a>"
            f"<code>{html.escape(clean_text(doc.get('path')))}</code></li>"
            for doc in job.get("documents", [])
        )
        cards.append(
            f"<article class=\"job {status}\"><header><span>{html.escape(status.upper())}</span>"
            f"<strong>{html.escape(clean_text(job.get('recipient_name')))}</strong>"
            f"<code>{html.escape(clean_text(job.get('target')))}</code></header>"
            f"<pre>{html.escape(preview_text)}</pre>"
            f"<h3>Files</h3><ul>{docs or '<li>None</li>'}</ul>"
            f"<h3>Issues</h3><ul>{''.join(f'<li>{html.escape(issue)}</li>' for issue in issues) or '<li>None</li>'}</ul></article>"
        )
    rows = "".join(
        "<tr>"
        f"<td>{html.escape(clean_text(item.get('complaint_code')))}</td>"
        f"<td>{html.escape(clean_text(item.get('recipient_name')))}</td>"
        f"<td>{html.escape(clean_text(item.get('status')))}</td>"
        f"<td>{html.escape(clean_text(item.get('reason')))}</td>"
        f"<td>{html.escape(str(item.get('elapsed_hours', '')))}</td>"
        f"<td>{html.escape(str(item.get('reminders_today', '')))}</td>"
        "</tr>"
        for item in candidates
    )
    (batch_dir / "preview.html").write_text(
        f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Inquiry Follow-up Preview {html.escape(batch_id)}</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 24px; background: #f5f5f5; color: #1f2933; }}
.summary {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 16px 0 24px; }}
.summary div, .job {{ background: white; border: 1px solid #ddd; border-radius: 6px; padding: 12px; }}
.job {{ border-left: 6px solid #2f855a; margin: 12px 0; }}
.job.warning {{ border-left-color: #b7791f; }} .job.blocked {{ border-left-color: #c53030; }}
header {{ display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }}
pre {{ white-space: pre-wrap; background: #f8fafc; padding: 12px; border-radius: 6px; }}
code {{ display: block; color: #4b5563; overflow-wrap: anywhere; margin-top: 4px; }}
table {{ border-collapse: collapse; width: 100%; background: white; margin-top: 16px; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; vertical-align: top; }}
</style></head><body>
<h1>Inquiry Follow-up Preview</h1>
<p>Batch <code>{html.escape(batch_id)}</code></p>
<div class="summary">
<div>Total jobs: <strong>{summary['job_count']}</strong></div>
<div>Ready: <strong>{summary['ready_count']}</strong></div>
<div>Warnings: <strong>{summary['warning_count']}</strong></div>
<div>Blocked: <strong>{summary['blocked_count']}</strong></div>
<div>Skipped: <strong>{summary['skipped_count']}</strong></div>
</div>
{''.join(cards)}
<h2>Candidates</h2>
<table><thead><tr><th>Complaint</th><th>Recipient</th><th>Status</th><th>Reason</th><th>Elapsed Hours</th><th>Sent Today</th></tr></thead>
<tbody>{rows or '<tr><td colspan="6">None</td></tr>'}</tbody></table>
</body></html>""",
        encoding="utf-8",
    )
    latest = preview_root(project_root) / "LATEST"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(str(batch_dir), encoding="utf-8")
    return batch_dir


async def run(settings: FollowupSettings, project_root: Path) -> None:
    logger = configure_logging()
    policy_all = load_json(settings.policy_path)
    policy = policy_all.get(settings.followup_type)
    if not isinstance(policy, dict):
        raise ValueError(f"No policy configured for follow-up type: {settings.followup_type}")
    config = load_json(settings.notification_config_path)
    delivery = config.get("delivery", {})
    nats_url = str(delivery.get("nats_url", "nats://localhost:4222"))
    subject = str(delivery.get("subject", "whatsapp.pending"))
    delay_ms = int(delivery.get("delay_ms", 1500) or 0)
    officers = load_officers(project_root / str(config.get("officers_csv", "officers-data.csv")))
    conn = sqlite3.connect(settings.output_root / "inquiry_letters.sqlite3")
    conn.row_factory = sqlite3.Row
    init_followup_db(conn)
    batch_id = str(uuid.uuid4())
    started_at = now_local()
    conn.execute(
        """
        INSERT INTO followup_runs (
            batch_id, action, followup_type, recipient_role, scope, started_at, settings_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            batch_id,
            settings.action,
            settings.followup_type,
            settings.recipient_role,
            settings.scope,
            started_at.isoformat(timespec="seconds"),
            orjson.dumps(settings.__dict__, default=str).decode("utf-8"),
        ],
    )
    rows = latest_order_rows(conn)
    package_root: Path | None = None
    if settings.files == "combined-pdf":
        package_root = (
            project_root
            / "reports"
            / "pmdu"
            / "followup_packages"
            / started_at.strftime("%Y")
            / started_at.strftime("%m")
            / started_at.strftime("%d")
            / batch_id
        )
    current_docs, all_documents, field_names = await current_paperless_documents(
        project_root
    )
    combined_client: PaperlessClient | None = None
    if settings.files == "combined-pdf":
        combined_client = PaperlessClient(load_paperless_settings(project_root))
        await combined_client.__aenter__()
    jobs: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    try:
        for row in rows:
            code = clean_text(row["complaint_code"])
            if settings.complaint and code != settings.complaint:
                continue
            current = current_docs.get(code)
            if not current:
                candidates.append({"complaint_code": code, "status": "skipped", "reason": "not found in Paperless"})
                continue
            _, fields = current
            if not eligible_paperless(fields):
                candidates.append({"complaint_code": code, "status": "skipped", "reason": "Paperless status/source is no longer eligible"})
                continue
            if settings.tehsil and not complaint_matches_tehsil(fields, settings.tehsil):
                continue
            stored_tehsil = normalize_tehsil(row["tehsil"])
            resolved_tehsil = normalize_tehsil(fields.get("Tehsil"))
            if not resolved_tehsil:
                candidates.append(
                    {
                        "order_id": int(row["id"]),
                        "complaint_code": code,
                        "recipient_role": settings.recipient_role,
                        "stored_tehsil": stored_tehsil,
                        "resolved_tehsil": "",
                        "recipient_resolution_source": "paperless_live",
                        "status": "blocked",
                        "reason": "live Paperless Tehsil is empty; recipient routing refused",
                    }
                )
                continue
            if stored_tehsil != resolved_tehsil:
                logger.warning(
                    "Routing tehsil changed for %s: inquiry database=%s, Paperless=%s. "
                    "Using live Paperless value.",
                    code,
                    stored_tehsil or "<empty>",
                    resolved_tehsil,
                )
            recipient = officer_recipient(
                resolved_tehsil, officers, settings.recipient_role
            )
            if not recipient:
                candidates.append(
                    {
                        "order_id": int(row["id"]),
                        "complaint_code": code,
                        "recipient_role": settings.recipient_role,
                        "stored_tehsil": stored_tehsil,
                        "resolved_tehsil": resolved_tehsil,
                        "recipient_resolution_source": "paperless_live",
                        "status": "blocked",
                        "reason": (
                            f"no {settings.recipient_role.upper()} recipient mapping "
                            f"for live Paperless tehsil {resolved_tehsil}"
                        ),
                    }
                )
                continue
            values = message_values(
                row,
                recipient,
                started_at,
                resolved_tehsil,
                fields,
            )
            text = render_template(str(policy.get("message") or ""), values)
            status, reason, elapsed_hours, reminders_today, last = candidate_decision(
                conn, row, policy, settings, recipient, started_at
            )
            documents: list[dict[str, str]] = []
            if status == "ready" and settings.files == "combined-pdf":
                order_path = Path(clean_text(row["pdf_path"]))
                if not order_path.is_absolute():
                    order_path = project_root / order_path
                try:
                    assert combined_client is not None and package_root is not None
                    documents = await build_combined_followup_package(
                        combined_client,
                        current[0],
                        all_documents,
                        field_names,
                        order_path,
                        package_root / code,
                        code,
                    )
                except Exception:
                    logger.exception(
                        "Could not build combined follow-up package for %s", code
                    )
                if not documents:
                    status = "blocked"
                    reason = "combined PDF package could not be generated"
            elif status == "ready":
                documents = documents_for_row(row, policy, settings.files)
            job_id = str(uuid.uuid4()) if status == "ready" else ""
            candidate = {
                "order_id": int(row["id"]),
                "complaint_code": code,
                "recipient_role": recipient["role"],
                "recipient_name": recipient["name"],
                "recipient_target": recipient["target"],
                "stored_tehsil": stored_tehsil,
                "resolved_tehsil": resolved_tehsil,
                "recipient_resolution_source": "paperless_live",
                "status": status,
                "reason": reason or "",
                "elapsed_hours": elapsed_hours,
                "reminders_today": reminders_today,
                "last_sent_at": last.isoformat(timespec="seconds") if last else "",
                "job_id": job_id,
            }
            candidates.append(candidate)
            if status != "ready":
                continue
            job = {
                "job_id": job_id,
                "target": recipient["target"],
                "type": "contact",
                "recipient_name": recipient["name"],
                "attachment_text_mode": "separate",
                "text": text if not documents else None,
                "documents": [
                    {**doc, "caption": text}
                    for doc in documents
                ],
                "expected_document_count": len(documents),
                "delay_ms": delay_ms,
            }
            job["_followup"] = {
                "order_id": int(row["id"]),
                "complaint_code": code,
                "followup_type": settings.followup_type,
                "recipient_role": recipient["role"],
                "resolved_tehsil": resolved_tehsil,
                "recipient_resolution_source": "paperless_live",
                "message_hash": message_hash(text, documents),
            }
            jobs.append(job)
            if settings.max_cases and len(jobs) >= settings.max_cases:
                break
        for item in candidates:
            conn.execute(
                """
                INSERT INTO followup_candidates (
                    batch_id, order_id, complaint_code, recipient_role, recipient_name,
                    recipient_target, status, reason, elapsed_hours, reminders_today,
                    last_sent_at, message_hash, job_id, stored_tehsil,
                    resolved_tehsil, recipient_resolution_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    batch_id,
                    item.get("order_id"),
                    item.get("complaint_code"),
                    item.get("recipient_role", settings.recipient_role),
                    item.get("recipient_name"),
                    item.get("recipient_target"),
                    item.get("status"),
                    item.get("reason"),
                    item.get("elapsed_hours"),
                    item.get("reminders_today", 0),
                    item.get("last_sent_at"),
                    "",
                    item.get("job_id"),
                    item.get("stored_tehsil"),
                    item.get("resolved_tehsil"),
                    item.get("recipient_resolution_source"),
                ],
            )
        if settings.action == "send" and jobs:
            payload_jobs = [{k: v for k, v in job.items() if k != "_followup"} for job in jobs]
            await publish_jobs(nats_url, subject, payload_jobs)
            for job in jobs:
                meta = job["_followup"]
                conn.execute(
                    """
                    INSERT INTO followup_deliveries (
                        delivery_id, batch_id, order_id, complaint_code, followup_type,
                        recipient_role, recipient_name, recipient_target, sent_at,
                        delivery_enabled, nats_subject, message_hash, job_payload,
                        resolved_tehsil, recipient_resolution_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        str(uuid.uuid4()),
                        batch_id,
                        meta["order_id"],
                        meta["complaint_code"],
                        meta["followup_type"],
                        meta["recipient_role"],
                        job["recipient_name"],
                        job["target"],
                        now_local().isoformat(timespec="seconds"),
                        1,
                        subject,
                        meta["message_hash"],
                        orjson.dumps({k: v for k, v in job.items() if k != "_followup"}).decode("utf-8"),
                        meta["resolved_tehsil"],
                        meta["recipient_resolution_source"],
                    ],
                )
            logger.info("Published %s inquiry follow-up job(s) to %s.", len(jobs), subject)
        else:
            preview_dir = write_preview(project_root, batch_id, settings, jobs, candidates)
            logger.info("Preview only; no follow-up jobs were published.")
            logger.info("Inquiry follow-up preview written: %s", preview_dir)
            logger.info("Open preview: %s", preview_dir / "preview.html")
        conn.execute(
            """
            UPDATE followup_runs
            SET completed_at = ?, candidate_count = ?, job_count = ?, published_count = ?
            WHERE batch_id = ?
            """,
            [
                now_local().isoformat(timespec="seconds"),
                len(candidates),
                len(jobs),
                len(jobs) if settings.action == "send" else 0,
                batch_id,
            ],
        )
        conn.commit()
        logger.info("Inquiry follow-up complete: candidates=%s jobs=%s", len(candidates), len(jobs))
    finally:
        if combined_client is not None:
            await combined_client.__aexit__(None, None, None)
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview/send PMDU inquiry follow-up WhatsApp jobs.")
    parser.add_argument("action", choices=("preview", "send"))
    parser.add_argument("--type", choices=sorted(FOLLOWUP_TYPES), default="reminder")
    parser.add_argument("--recipient", choices=sorted(RECIPIENT_ROLES), default="ddeo")
    parser.add_argument("--scope", choices=("eligible", "all"), default="eligible")
    parser.add_argument("--tehsil")
    parser.add_argument("--complaint")
    parser.add_argument("--max-cases", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-second-today", action="store_true")
    parser.add_argument(
        "--files",
        choices=("inquiry-order", "combined-pdf", "none"),
        default="inquiry-order",
    )
    parser.add_argument("--policy", default="inquiry_followup_policy.json")
    parser.add_argument("--output-root", default="inquiry_letters_to_ddeos")
    parser.add_argument("--notification-config", default="notification_config.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    settings = FollowupSettings(
        action=args.action,
        followup_type=args.type,
        recipient_role=args.recipient,
        scope=args.scope,
        tehsil=args.tehsil,
        complaint=args.complaint,
        max_cases=args.max_cases,
        force=args.force,
        allow_second_today=args.allow_second_today,
        files=args.files,
        policy_path=(project_root / args.policy).resolve(),
        output_root=(project_root / args.output_root).resolve(),
        notification_config_path=(project_root / args.notification_config).resolve(),
    )
    import asyncio

    asyncio.run(run(settings, project_root))


if __name__ == "__main__":
    main()
