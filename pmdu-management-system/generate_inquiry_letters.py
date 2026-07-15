from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import shutil
import sqlite3
import subprocess
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

from main import load_paperless_settings
from notify_under_investigation import (
    clean_text,
    complaint_code as paperless_complaint_code,
    complaint_matches,
    decoded_custom_fields,
)
from notify_under_investigation import (
    load_json,
    normalize_label,
    normalize_tehsil,
    option_maps,
)
from paperless import PaperlessClient


LOGGER_NAME = "pmdu_automation"
LETTER_KIND = "ddeeo_inquiry_letter"

ODT_NAMESPACES = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "draw": "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0",
    "style": "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
    "fo": "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0",
    "svg": "urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0",
    "xlink": "http://www.w3.org/1999/xlink",
    "loext": "urn:org:documentfoundation:names:experimental:office:xmlns:loext:1.0",
}
for prefix, uri in ODT_NAMESPACES.items():
    ET.register_namespace(prefix, uri)


@dataclass(frozen=True)
class Officer:
    tehsil: str
    ddeo_name: str
    ddeo_number: str
    deo_name: str
    deo_number: str


@dataclass(frozen=True)
class ComplaintItem:
    complaint_code: str
    version: int
    snapshot_path: Path
    snapshot: dict[str, Any]
    status: str
    paperless_fields: dict[str, Any]
    paperless_document_id: int | None = None


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger(LOGGER_NAME)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate DDEO inquiry letters for under-investigation PMDU complaints."
    )
    parser.add_argument("--source", choices=("paperless", "artifacts"), default="paperless")
    parser.add_argument("--artifact-dir", default="artifacts")
    parser.add_argument("--output-root", default="inquiry_letters_to_ddeos")
    parser.add_argument("--officers-csv", default="officers-data.csv")
    parser.add_argument("--notification-config", default="notification_config.json")
    parser.add_argument("--status", default="Under Investigation")
    parser.add_argument("--max-cases", type=int, default=0)
    parser.add_argument("--issue-date", default="")
    parser.add_argument("--school-reply-working-days", type=int, default=2)
    parser.add_argument("--ddeo-report-working-days", type=int, default=3)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def latest_snapshots(artifact_dir: Path) -> dict[str, Path]:
    snapshots: dict[str, Path] = {}
    for complaint_dir in sorted(path for path in artifact_dir.iterdir() if path.is_dir()):
        versions: list[tuple[int, Path]] = []
        for version_dir in complaint_dir.glob("v*"):
            if not version_dir.is_dir():
                continue
            try:
                version = int(version_dir.name.removeprefix("v"))
            except ValueError:
                continue
            snapshot_path = version_dir / "snapshot.json"
            if snapshot_path.exists():
                versions.append((version, snapshot_path))
        if versions:
            snapshots[complaint_dir.name] = max(versions, key=lambda item: item[0])[1]
    return snapshots


def load_snapshot(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_officers(path: Path) -> dict[str, Officer]:
    officers: dict[str, Officer] = {}
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            tehsil = normalize_tehsil(row.get("Tehsil"))
            if not tehsil:
                continue
            officers[tehsil] = Officer(
                tehsil=tehsil,
                ddeo_name=clean_text(row.get("DDEO Name")),
                ddeo_number=clean_text(row.get("DDEO CELL NUMBER")),
                deo_name=clean_text(row.get("DEO MEE Focal Person Name")),
                deo_number=clean_text(row.get("DEO MEE Focal Person Number")),
            )
    return officers


def add_working_days(start: date, days: int) -> date:
    current = start
    remaining = days
    while remaining > 0:
        current += timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def parse_issue_date(value: str) -> date:
    if not value:
        return date.today()
    return datetime.strptime(value, "%Y-%m-%d").date()


def safe_name(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return value.strip("_") or "unknown"


def subject_school_line(identity: dict[str, Any]) -> str:
    level_two = clean_text(identity.get("level_two"))
    if level_two and normalize_label(level_two) != normalize_label("Private Schools"):
        return level_two
    return "Institution Concerned"


def inquiry_address_line() -> str:
    return "School Name / Address / Area"


def respondent_name_line(fields: dict[str, Any]) -> str:
    return clean_text(fields.get("The Respondent Name")) or inquiry_address_line()


def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS generation_runs (
            run_id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            started_at TEXT NOT NULL,
            issue_date TEXT NOT NULL,
            status_filter TEXT NOT NULL,
            generated_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inquiry_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            complaint_code TEXT NOT NULL,
            version INTEGER NOT NULL,
            letter_kind TEXT NOT NULL,
            status TEXT NOT NULL,
            tehsil TEXT NOT NULL,
            school_name TEXT,
            ddeo_name TEXT,
            ddeo_number TEXT,
            paperless_document_id INTEGER,
            snapshot_path TEXT NOT NULL,
            editable_path TEXT NOT NULL,
            pdf_path TEXT NOT NULL,
            issue_date TEXT NOT NULL,
            school_reply_due_date TEXT NOT NULL,
            ddeo_report_due_date TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            last_generated_run_id TEXT NOT NULL,
            order_status TEXT NOT NULL DEFAULT 'issued',
            response_received_at TEXT,
            report_received_at TEXT,
            closed_at TEXT,
            UNIQUE (complaint_code, version, letter_kind)
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(inquiry_orders)").fetchall()}
    if "school_name" not in columns:
        conn.execute("ALTER TABLE inquiry_orders ADD COLUMN school_name TEXT")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inquiry_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            complaint_code TEXT NOT NULL,
            version INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            event_at TEXT NOT NULL,
            notes TEXT
        )
        """
    )
    return conn


def already_generated(conn: sqlite3.Connection, item: ComplaintItem) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM inquiry_orders
        WHERE complaint_code = ?
          AND version = ?
          AND letter_kind = ?
          AND order_status != 'void'
        LIMIT 1
        """,
        [item.complaint_code, item.version, LETTER_KIND],
    ).fetchone()
    return bool(row)


def record_order(
    conn: sqlite3.Connection,
    item: ComplaintItem,
    officer: Officer,
    school_name: str,
    editable_path: Path,
    pdf_path: Path,
    issue: date,
    school_due: date,
    ddeo_due: date,
    run_id: str,
) -> None:
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO inquiry_orders (
            complaint_code,
            version,
            letter_kind,
            status,
            tehsil,
            school_name,
            ddeo_name,
            ddeo_number,
            paperless_document_id,
            snapshot_path,
            editable_path,
            pdf_path,
            issue_date,
            school_reply_due_date,
            ddeo_report_due_date,
            generated_at,
            last_generated_run_id,
            order_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'issued')
        ON CONFLICT(complaint_code, version, letter_kind) DO UPDATE SET
            status = excluded.status,
            tehsil = excluded.tehsil,
            school_name = excluded.school_name,
            ddeo_name = excluded.ddeo_name,
            ddeo_number = excluded.ddeo_number,
            paperless_document_id = excluded.paperless_document_id,
            snapshot_path = excluded.snapshot_path,
            editable_path = excluded.editable_path,
            pdf_path = excluded.pdf_path,
            issue_date = excluded.issue_date,
            school_reply_due_date = excluded.school_reply_due_date,
            ddeo_report_due_date = excluded.ddeo_report_due_date,
            generated_at = excluded.generated_at,
            last_generated_run_id = excluded.last_generated_run_id,
            order_status = 'issued'
        """,
        [
            item.complaint_code,
            item.version,
            LETTER_KIND,
            item.status,
            officer.tehsil,
            school_name,
            officer.ddeo_name,
            officer.ddeo_number,
            item.paperless_document_id,
            str(item.snapshot_path),
            str(editable_path),
            str(pdf_path),
            issue.isoformat(),
            school_due.isoformat(),
            ddeo_due.isoformat(),
            now,
            run_id,
        ],
    )
    conn.execute(
        """
        INSERT INTO inquiry_events (
            complaint_code, version, event_type, event_at, notes
        )
        VALUES (?, ?, 'letter_generated', ?, ?)
        """,
        [
            item.complaint_code,
            item.version,
            now,
            f"Generated {editable_path.name} and {pdf_path.name}",
        ],
    )


def xml_escape(value: Any) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def fill_subject_complaint_blank(content: str, complaint_code: str) -> str:
    root = ET.fromstring(content.encode("utf-8"))
    text_p = f"{{{ODT_NAMESPACES['text']}}}p"
    changed = False
    for paragraph in root.iter(text_p):
        visible = "".join(paragraph.itertext()).strip()
        normalized_visible = re.sub(r"\s+", " ", visible).upper()
        if "COMPLAINT NO" not in normalized_visible or "_" not in visible:
            continue
        replaced = False
        for node in paragraph.iter():
            if node.text and "_" in node.text:
                if not replaced:
                    node.text = re.sub(r"_+", complaint_code, node.text, count=1)
                    replaced = True
                else:
                    node.text = re.sub(r"_+", "", node.text)
            if node.tail and "_" in node.tail:
                if not replaced:
                    node.tail = re.sub(r"_+", complaint_code, node.tail, count=1)
                    replaced = True
                else:
                    node.tail = re.sub(r"_+", "", node.tail)
        if replaced:
            changed = True
        else:
            logging.getLogger(LOGGER_NAME).warning(
                "Complaint-number paragraph was found, but no underline blank was available."
            )
        changed = True
        break
    if not changed:
        logging.getLogger(LOGGER_NAME).warning(
            "Template complaint-number blank was not found."
        )
    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")


def render_odt_template(
    template_path: Path,
    output_path: Path,
    replacements: dict[str, str],
    complaint_code: str,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(template_path, "r") as template_zip:
        with zipfile.ZipFile(output_path, "w") as output_zip:
            for item in template_zip.infolist():
                content = template_zip.read(item.filename)
                if item.filename == "content.xml":
                    content_text = content.decode("utf-8")
                    for placeholder, value in replacements.items():
                        content_text = content_text.replace(placeholder, xml_escape(value))
                    content_text = fill_subject_complaint_blank(
                        content_text, complaint_code
                    )
                    content = content_text.encode("utf-8")
                compress_type = zipfile.ZIP_STORED if item.filename == "mimetype" else zipfile.ZIP_DEFLATED
                output_zip.writestr(item, content, compress_type=compress_type)


def convert_with_libreoffice(input_path: Path, output_dir: Path, fmt: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    executable = shutil.which("libreoffice") or shutil.which("soffice")
    if not executable:
        raise RuntimeError("LibreOffice/soffice is required for document conversion.")
    subprocess.run(
        [
            executable,
            "--headless",
            "--convert-to",
            fmt,
            "--outdir",
            str(output_dir),
            str(input_path),
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    suffix = ".pdf" if fmt == "pdf" else f".{fmt}"
    output_path = output_dir / f"{input_path.stem}{suffix}"
    if not output_path.exists():
        raise FileNotFoundError(f"LibreOffice did not create expected file: {output_path}")
    return output_path


async def select_from_paperless(
    project_root: Path,
    artifact_snapshots: dict[str, Path],
    config_path: Path,
    status: str,
) -> list[ComplaintItem]:
    config = load_json(config_path)
    config.setdefault("paperless", {})["status"] = status
    settings = load_paperless_settings(project_root)
    selected: list[ComplaintItem] = []
    async with PaperlessClient(settings) as client:
        custom_fields = await client.paginated_results("/api/custom_fields/?page_size=100")
        names, options = option_maps(custom_fields)
        documents = await client.paginated_results("/api/documents/?page_size=100")
        for document in documents:
            fields = decoded_custom_fields(document, names, options)
            if not complaint_matches(fields, config):
                continue
            code = paperless_complaint_code(fields, document)
            snapshot_path = artifact_snapshots.get(code)
            if not snapshot_path:
                logging.getLogger(LOGGER_NAME).warning(
                    "No local PMDU snapshot found for under-investigation complaint %s.",
                    code,
                )
                continue
            snapshot = load_snapshot(snapshot_path)
            selected.append(
                ComplaintItem(
                    complaint_code=code,
                    version=int(snapshot.get("version") or 1),
                    snapshot_path=snapshot_path,
                    snapshot=snapshot,
                    status=clean_text(fields.get("Status")) or status,
                    paperless_fields=fields,
                    paperless_document_id=int(document["id"]),
                )
            )
    return selected


def select_from_artifacts(artifact_snapshots: dict[str, Path], status: str) -> list[ComplaintItem]:
    selected: list[ComplaintItem] = []
    for code, snapshot_path in sorted(artifact_snapshots.items()):
        snapshot = load_snapshot(snapshot_path)
        identity = snapshot.get("identity", {})
        current_status = clean_text(identity.get("current_status"))
        if current_status and "in progress" not in current_status.lower():
            continue
        selected.append(
            ComplaintItem(
                complaint_code=code,
                version=int(snapshot.get("version") or 1),
                snapshot_path=snapshot_path,
                snapshot=snapshot,
                status=status if status else current_status,
                paperless_fields={},
            )
        )
    return selected


def output_dirs(root: Path, issue: date, tehsil: str) -> tuple[Path, Path]:
    base = root / issue.strftime("%Y") / issue.strftime("%m") / issue.strftime("%d") / safe_name(tehsil)
    return base / "editable", base / "pdfs"


def generate_letters(args: argparse.Namespace) -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    artifact_dir = (project_root / args.artifact_dir).resolve()
    output_root = (project_root / args.output_root).resolve()
    template_path = output_root / "Inquiry_Letter_to_ddeo_sample.odt"
    if not template_path.exists():
        raise FileNotFoundError(f"Inquiry letter template not found: {template_path}")
    officers = load_officers((project_root / args.officers_csv).resolve())
    snapshots = latest_snapshots(artifact_dir)
    issue = parse_issue_date(args.issue_date)
    school_due = add_working_days(issue, args.school_reply_working_days)
    ddeo_due = add_working_days(issue, args.ddeo_report_working_days)
    run_id = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")

    if args.source == "paperless":
        items = asyncio.run(
            select_from_paperless(
                project_root,
                snapshots,
                (project_root / args.notification_config).resolve(),
                args.status,
            )
        )
    else:
        items = select_from_artifacts(snapshots, args.status)

    if args.max_cases and args.max_cases > 0:
        items = items[: args.max_cases]

    conn = init_db(output_root / "inquiry_letters.sqlite3")
    generated = 0
    skipped = 0
    try:
        for index, item in enumerate(items, start=1):
            fields = item.paperless_fields
            identity = item.snapshot.get("identity", {})
            tehsil = normalize_tehsil(fields.get("Tehsil") or identity.get("tehsil"))
            officer = officers.get(tehsil)
            if not officer:
                logger.warning("No DDEO mapping for %s (%s). Skipping.", item.complaint_code, tehsil)
                skipped += 1
                continue
            if already_generated(conn, item) and not args.force:
                logger.info("Inquiry letter already exists for %s; use --force to regenerate.", item.complaint_code)
                skipped += 1
                continue

            editable_dir, pdf_dir = output_dirs(output_root, issue, officer.tehsil)
            filename = f"Inquiry_Letter_{safe_name(item.complaint_code)}_v{item.version}"
            tehsil_display = clean_text(fields.get("Tehsil")) or officer.tehsil.title()
            school_name = respondent_name_line(fields)

            if args.dry_run:
                logger.info("DRY RUN: would generate %s for %s.", filename, officer.tehsil)
                generated += 1
                continue

            editable_dir.mkdir(parents=True, exist_ok=True)
            pdf_dir.mkdir(parents=True, exist_ok=True)
            editable_path = editable_dir / f"{filename}.odt"
            replacements = {
                "[School Name / Address / Area]": school_name,
                "[Tehsil Name]": tehsil_display,
                "Complaint No. _____________": f"Complaint No. {item.complaint_code}",
                "(M-EE) [Name of Tehsil]": f"(M-EE) {tehsil_display}",
                "(M-EE), [Name of Tehsil]": f"(M-EE), {tehsil_display}",
            }
            render_odt_template(
                template_path,
                editable_path,
                replacements,
                item.complaint_code,
            )
            pdf_path = convert_with_libreoffice(editable_path, pdf_dir, "pdf")

            record_order(
                conn,
                item,
                officer,
                school_name,
                editable_path,
                pdf_path,
                issue,
                school_due,
                ddeo_due,
                run_id,
            )
            generated += 1
            logger.info("Generated inquiry letter for %s -> %s", item.complaint_code, pdf_path)

        conn.execute(
            """
            INSERT INTO generation_runs (
                run_id, source, started_at, issue_date, status_filter,
                generated_count, skipped_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                args.source,
                datetime.now().astimezone().isoformat(timespec="seconds"),
                issue.isoformat(),
                args.status,
                generated,
                skipped,
            ],
        )
        conn.commit()
    finally:
        conn.close()

    logger.info("Inquiry letter generation complete: generated=%s skipped=%s", generated, skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(generate_letters(parse_args()))
