from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from main import configure_logging
from send_compliances import (
    NATS_WHATSAPP_SUBJECT,
    ensure_nats_connected,
    ensure_whatsapp_worker_ready,
)


LOGGER_NAME = "pmdu_automation"
COMPLAINT_COL_NAME = "Complaint No"
MOBILE_COL_NAME = "Mobile No"
NAME_COL_NAME = "Person Name"
DEFAULT_FILTERED_DIR = "phase1-crm/unprocessed-crm/filtered"
DEFAULT_DISPATCH_SUBDIR = "dispatch"
STATUS_CHOICES = ("all", "submitted", "not-relevant")
CRM_COMPLAINT_RE = re.compile(r"\b\d{3}-\d{4,}\b")


@dataclass(frozen=True)
class NoticeSource:
    status: str
    label: str
    path: Path


def resolve_project_path(project_root: Path, value: str | None) -> Path:
    path = Path(value or "").expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def input_stem(value: str | None) -> str:
    if not value:
        return ""
    return Path(value).stem


def status_sources(output_dir: Path, status: str, stem: str = "") -> list[NoticeSource]:
    candidates: list[NoticeSource] = []
    if status in {"all", "submitted"}:
        folder = output_dir / "submitted"
        pattern = f"submitted_{stem}.xlsx" if stem else "submitted_*.xlsx"
        candidates.extend(
            NoticeSource("submitted", "Submitted", path)
            for path in sorted(folder.glob(pattern))
        )
    if status in {"all", "not-relevant"}:
        folder = output_dir / "uploaded" / "not-relevant"
        pattern = (
            f"uploaded_not_relevant_{stem}.xlsx"
            if stem
            else "uploaded_not_relevant_*.xlsx"
        )
        candidates.extend(
            NoticeSource("not_relevant", "Not Relevant", path)
            for path in sorted(folder.glob(pattern))
        )
    return candidates


def normalize_phone(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return ""
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0"):
        digits = digits[1:]
    if not digits.startswith("92"):
        digits = "92" + digits
    return digits if len(digits) >= 11 else ""


def complaint_number(value: Any) -> str:
    match = CRM_COMPLAINT_RE.search("" if value is None else str(value))
    return match.group(0) if match else ""


def first_text(row: dict[str, Any], key: str) -> str:
    value = row.get(key, "")
    if pd.isna(value):
        return ""
    return str(value).strip()


def notice_message(status: str, complaint_no: str, name: str) -> str:
    greeting_name = f" {name}" if name else ""
    if status == "submitted":
        return (
            f"Assalam-o-Alaikum{greeting_name}. CRM complaint {complaint_no} is "
            "already marked Submitted in the department record, so it was not "
            "included again for pending upload."
        )
    return (
        f"Assalam-o-Alaikum{greeting_name}. CRM complaint {complaint_no} is "
        "already marked Not Relevant in the department record, so it was not "
        "included again for pending upload."
    )


def read_notice_rows(source: NoticeSource) -> list[dict[str, str]]:
    dataframe = pd.read_excel(source.path)
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for raw_row in dataframe.to_dict(orient="records"):
        complaint_no = complaint_number(raw_row.get(COMPLAINT_COL_NAME))
        target = normalize_phone(raw_row.get(MOBILE_COL_NAME))
        if not complaint_no or not target:
            continue
        name = first_text(raw_row, NAME_COL_NAME)
        key = (source.status, complaint_no, target)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "status": source.status,
                "status_label": source.label,
                "complaint_no": complaint_no,
                "person_name": name,
                "mobile_no": first_text(raw_row, MOBILE_COL_NAME),
                "target": target,
                "message": notice_message(source.status, complaint_no, name),
                "source_file": str(source.path),
            }
        )
    return rows


def dispatch_csv_path(dispatch_dir: Path, source: NoticeSource) -> Path:
    suffix = source.path.stem
    return dispatch_dir / f"{suffix}_notice_dispatch.csv"


def write_notice_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "status",
        "status_label",
        "complaint_no",
        "person_name",
        "mobile_no",
        "target",
        "message",
        "source_file",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def prepare_notice_lists(
    project_root: Path,
    *,
    input_file: str,
    output_dir_value: str,
    status: str,
    dispatch_dir_value: str,
) -> list[Path]:
    logger = logging.getLogger(LOGGER_NAME)
    output_dir = resolve_project_path(project_root, output_dir_value or DEFAULT_FILTERED_DIR)
    dispatch_dir = resolve_project_path(
        project_root, dispatch_dir_value or str(output_dir / DEFAULT_DISPATCH_SUBDIR)
    )
    sources = status_sources(output_dir, status, input_stem(input_file))
    if not sources:
        logger.warning("No filtered submitted/not-relevant sheets found in %s.", output_dir)
        return []

    written: list[Path] = []
    for source in sources:
        rows = read_notice_rows(source)
        path = dispatch_csv_path(dispatch_dir, source)
        write_notice_csv(path, rows)
        written.append(path)
        logger.info(
            "Prepared %d %s notice row(s): %s",
            len(rows),
            source.label,
            path,
        )
    return written


def discover_dispatch_csvs(
    project_root: Path,
    *,
    input_file: str,
    output_dir_value: str,
    status: str,
    dispatch_dir_value: str,
    dispatch_csv_value: str,
) -> list[Path]:
    if dispatch_csv_value:
        return [resolve_project_path(project_root, dispatch_csv_value)]
    output_dir = resolve_project_path(project_root, output_dir_value or DEFAULT_FILTERED_DIR)
    dispatch_dir = resolve_project_path(
        project_root, dispatch_dir_value or str(output_dir / DEFAULT_DISPATCH_SUBDIR)
    )
    sources = status_sources(output_dir, status, input_stem(input_file))
    return [dispatch_csv_path(dispatch_dir, source) for source in sources]


def read_dispatch_rows(paths: list[Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in paths:
        if not path.exists():
            logging.getLogger(LOGGER_NAME).warning("Dispatch CSV not found: %s", path)
            continue
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows.extend(dict(row) for row in csv.DictReader(handle))
    return rows


async def send_notice_lists(
    project_root: Path,
    *,
    paths: list[Path],
    dry_run: bool,
) -> int:
    logger = logging.getLogger(LOGGER_NAME)
    rows = [
        row
        for row in read_dispatch_rows(paths)
        if row.get("target", "").strip() and row.get("message", "").strip()
    ]
    if not rows:
        logger.warning("No sendable notice rows found.")
        return 0

    if dry_run:
        for row in rows:
            logger.info(
                "DRY RUN: Would send %s notice for %s to %s.",
                row.get("status_label") or row.get("status"),
                row.get("complaint_no"),
                row.get("target"),
            )
        return len(rows)

    nc = await ensure_nats_connected(project_root, logger)
    if nc is None:
        return 0
    try:
        js = nc.jetstream()
        if not await ensure_whatsapp_worker_ready(project_root, js, logger):
            return 0
        batch_id = f"crm_duplicate_notice_{uuid.uuid4().hex[:12]}"
        for index, row in enumerate(rows, start=1):
            complaint_no = row.get("complaint_no", "").strip()
            target = row.get("target", "").strip()
            job_id = f"{batch_id}_{index}_{complaint_no}_{target}"
            payload = {
                "batchId": batch_id,
                "complaintCode": complaint_no,
                "jobId": job_id,
                "target": target,
                "type": "contact",
                "text": row.get("message", "").strip(),
                "delayMs": 1500,
            }
            ack = await js.publish(
                NATS_WHATSAPP_SUBJECT,
                json.dumps(payload).encode("utf-8"),
                headers={"Nats-Msg-Id": job_id},
            )
            logger.info(
                "Queued %s notice for %s to %s (stream=%s seq=%s).",
                row.get("status_label") or row.get("status"),
                complaint_no,
                target,
                ack.stream,
                ack.seq,
            )
        return len(rows)
    finally:
        await nc.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare and send CRM duplicate status notices from filtered sheet outputs."
    )
    parser.add_argument("action", choices=("prepare", "send"))
    parser.add_argument("--input-file", default="", help="Original CRM sheet file used for filtering.")
    parser.add_argument("--output-dir", default=DEFAULT_FILTERED_DIR, help="Filtered output folder.")
    parser.add_argument("--status", choices=STATUS_CHOICES, default="all")
    parser.add_argument("--dispatch-dir", default="", help="Folder for generated notice CSV files.")
    parser.add_argument("--dispatch-csv", default="", help="Specific notice CSV to send.")
    parser.add_argument("--dry-run", action="store_true", help="Preview send without queueing WhatsApp jobs.")
    return parser.parse_args()


def main() -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    args = parse_args()
    try:
        if args.action == "prepare":
            paths = prepare_notice_lists(
                project_root,
                input_file=args.input_file,
                output_dir_value=args.output_dir,
                status=args.status,
                dispatch_dir_value=args.dispatch_dir,
            )
            return 0 if paths else 1

        paths = discover_dispatch_csvs(
            project_root,
            input_file=args.input_file,
            output_dir_value=args.output_dir,
            status=args.status,
            dispatch_dir_value=args.dispatch_dir,
            dispatch_csv_value=args.dispatch_csv,
        )
        count = asyncio.run(send_notice_lists(project_root, paths=paths, dry_run=args.dry_run))
        logger.info("Notice send complete: %d row(s).", count)
        return 0 if count else 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        return 130
    except Exception:
        logger.exception("CRM duplicate notice command failed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
