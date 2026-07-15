"""
Compliance Reports Dispatcher & Audit Logger
Save this file as send_compliances.py in deomeeautomations/crm-management-system/
Run with: uv run python send_compliances.py --help
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import nats
from nats.aio.client import Client as NATSClient
from nats.errors import TimeoutError as NATSTimeoutError
from pypdf import PdfWriter

# Import existing configurations and clients from your project
from main import configure_logging, load_paperless_settings
from paperless import (
    PaperlessClient,
    PaperlessSettings,
    resolve_metadata,
    select_option_id,
)

LOGGER_NAME = "pmdu_automation"
CRM_COMPLAINT_RE = re.compile(r"\b\d{3}-\d{4,}\b")

# ---------------------------------------------------------
# CONSTANTS & CONFIGURATION
# ---------------------------------------------------------
COMPLIANCE_DELIVERED_CF_ID = 27  # The boolean custom field ID in Paperless
NATS_SERVER_URL = "nats://localhost:4222"
NATS_WHATSAPP_SUBJECT = "whatsapp.pending"
NATS_WHATSAPP_STATUS_SUBJECT = "whatsapp.status"
NATS_WHATSAPP_STREAM = "pending_stream"
NATS_WHATSAPP_CONSUMER = "whatsapp_worker_1"
DEFAULT_DELIVERY_TIMEOUT_SECONDS = 900
DEFAULT_WORKER_READY_TIMEOUT_SECONDS = 15
DEFAULT_NATS_READY_TIMEOUT_SECONDS = 15
DEFAULT_SERVICE_START_TIMEOUT_SECONDS = 45
ATTACHMENT_MODES = ("combined", "separate")


def compliance_preview_root(project_root: Path) -> Path:
    return project_root / "reports" / "crm" / "compliance_dispatch_previews"


def delivery_document(path: Path, filename: str) -> dict[str, str]:
    return {
        "path": str(path.absolute()),
        "filename": filename,
        "mimetype": "application/pdf",
    }


async def prepare_dispatch_documents(
    manager: "DeliveryManager",
    complaint_code: str,
    report_doc_ids: dict[str, int | None],
    output_dir: Path | None = None,
) -> list[dict[str, str]]:
    merge_order = [
        ("deo", "DEO Report"),
        ("ddeo", "DDEO Report"),
        ("institute", "Institute Report"),
        ("main", "Main Complaint"),
    ]
    downloaded: list[tuple[str, Path]] = []
    for key, label in merge_order:
        doc_id = report_doc_ids[key]
        if not doc_id:
            continue
        path = await manager.download_report_pdf(
            doc_id,
            label.replace(" ", ""),
            output_dir=output_dir,
        )
        if path:
            downloaded.append((label, path))

    if not downloaded:
        return []
    return [
        delivery_document(path, f"{complaint_code} - {label}.pdf")
        for label, path in downloaded
    ]


def write_dispatch_preview(
    project_root: Path,
    batch_id: str,
    attachment_mode: str,
    records: list[dict[str, Any]],
    skipped: list[dict[str, str]],
) -> Path:
    now = datetime.now().astimezone()
    batch_dir = (
        compliance_preview_root(project_root)
        / now.strftime("%Y")
        / now.strftime("%m")
        / now.strftime("%d")
        / batch_id
    )
    batch_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "batch_id": batch_id,
        "created_at": now.isoformat(timespec="seconds"),
        "attachment_mode": attachment_mode,
        "complaint_count": sum(int(item.get("complaint_count", 1)) for item in records),
        "document_count": sum(len(item["documents"]) for item in records),
        "recipient_count": sum(len(item["recipients"]) for item in records),
        "skipped_count": len(skipped),
    }
    (batch_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (batch_dir / "dispatch.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["complaint_numbers", "source", "attachment_mode", "documents", "recipients"])
        for item in records:
            complaint_numbers = item.get("complaint_codes") or [item["complaint_code"]]
            writer.writerow(
                [
                    " | ".join(complaint_numbers),
                    item["source"],
                    attachment_mode,
                    " | ".join(doc["filename"] for doc in item["documents"]),
                    " | ".join(item["recipients"]),
                ]
            )
    with (batch_dir / "jobs.jsonl").open("w", encoding="utf-8") as handle:
        for item in records:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")
    with (batch_dir / "skipped.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["complaint_number", "reason"])
        writer.writeheader()
        writer.writerows(skipped)

    cards: list[str] = []
    for item in records:
        complaint_numbers = item.get("complaint_codes") or [item["complaint_code"]]
        document_items = []
        for document in item["documents"]:
            path = Path(document["path"])
            document_items.append(
                f'<li><a href="{html.escape(path.as_uri(), quote=True)}">'
                f'{html.escape(document["filename"])}</a>'
                f'<code>{html.escape(str(path))}</code></li>'
            )
        recipients = "".join(f"<li>{html.escape(phone)}</li>" for phone in item["recipients"])
        complaint_items = "".join(
            f"<li><code>{html.escape(number)}</code></li>" for number in complaint_numbers
        )
        cards.append(
            "<article>"
            f'<header><strong>{html.escape(item["complaint_code"])}</strong>'
            f'<span>{html.escape(item["source"])}</span></header>'
            f'<p>{html.escape(item["text"])}</p>'
            f'<h3>Files ({len(item["documents"])})</h3><ul>{"".join(document_items)}</ul>'
            f'<h3>Recipients ({len(item["recipients"])})</h3><ul>{recipients}</ul>'
            f'<details><summary>Complaints ({len(complaint_numbers)})</summary><ul>{complaint_items}</ul></details>'
            "</article>"
        )
    skipped_rows = "".join(
        f'<tr><td>{html.escape(row["complaint_number"])}</td>'
        f'<td>{html.escape(row["reason"])}</td></tr>'
        for row in skipped
    )
    (batch_dir / "preview.html").write_text(
        f"""<!doctype html>
<html><head><meta charset="utf-8"><title>CRM Compliance Dispatch Preview</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 24px; background: #f4f6f8; color: #20252b; }}
.summary {{ display: flex; gap: 12px; flex-wrap: wrap; margin: 18px 0; }}
.summary div, article {{ background: white; border: 1px solid #d9dee3; border-radius: 6px; padding: 12px 16px; }}
article {{ margin: 12px 0; border-left: 5px solid #248a5b; }}
header {{ display: flex; gap: 14px; align-items: center; }} h3 {{ margin-bottom: 4px; }}
ul {{ margin-top: 4px; }} code {{ display: block; color: #59636e; overflow-wrap: anywhere; }}
table {{ width: 100%; border-collapse: collapse; background: white; }} th,td {{ border: 1px solid #d9dee3; padding: 8px; text-align: left; }}
</style></head><body>
<h1>CRM Compliance Dispatch Preview</h1>
<p>Batch <code>{html.escape(batch_id)}</code> | Mode: <strong>{html.escape(attachment_mode.title())}</strong></p>
<div class="summary"><div>Complaints: <strong>{summary['complaint_count']}</strong></div>
<div>PDF files: <strong>{summary['document_count']}</strong></div>
<div>Recipient deliveries: <strong>{summary['recipient_count']}</strong></div>
<div>Skipped: <strong>{summary['skipped_count']}</strong></div></div>
{''.join(cards) or '<p>No dispatches are ready.</p>'}
<h2>Skipped</h2><table><thead><tr><th>Complaint</th><th>Reason</th></tr></thead>
<tbody>{skipped_rows or '<tr><td colspan="2">None</td></tr>'}</tbody></table>
</body></html>""",
        encoding="utf-8",
    )
    latest = compliance_preview_root(project_root) / "LATEST"
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(str(batch_dir), encoding="utf-8")
    return batch_dir


def find_pnpm() -> str:
    pnpm = shutil.which("pnpm")
    if pnpm:
        return pnpm

    nvm_versions = Path.home() / ".nvm" / "versions" / "node"
    if nvm_versions.exists():
        candidates = sorted(nvm_versions.glob("*/bin/pnpm"), reverse=True)
        for candidate in candidates:
            if candidate.exists():
                return str(candidate)

    return "pnpm"


def service_log_path(project_root: Path, service_name: str) -> Path:
    logs_dir = project_root / "service_logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir / f"{service_name}.log"


def start_background_process(
    command: list[str],
    cwd: Path,
    log_path: Path,
    logger: logging.Logger,
    env: dict[str, str] | None = None,
) -> subprocess.Popen[bytes] | None:
    try:
        log_file = open(log_path, "ab")
    except OSError as exc:
        logger.error("Could not open service log %s: %s", log_path, exc)
        return None

    try:
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except Exception as exc:
        log_file.close()
        logger.error("Failed to start %s in %s: %s", " ".join(command), cwd, exc)
        return None
    finally:
        if not log_file.closed:
            log_file.close()

    logger.info(
        "Started background service: %s (pid=%s, log=%s)",
        " ".join(command),
        process.pid,
        log_path,
    )
    return process


def start_nats_server(project_root: Path, logger: logging.Logger) -> bool:
    base_dir = project_root.parent
    nats_dir = base_dir / "nats-server"
    nats_bin = nats_dir / "nats-server"
    if not nats_bin.exists():
        logger.error("NATS binary not found at %s", nats_bin)
        return False

    process = start_background_process(
        [str(nats_bin), "-js"],
        nats_dir,
        service_log_path(project_root, "nats-server"),
        logger,
    )
    return process is not None


def start_whatsapp_worker(project_root: Path, logger: logging.Logger) -> bool:
    base_dir = project_root.parent
    whatsapp_dir = base_dir / "whatsappbot"
    if not whatsapp_dir.exists():
        logger.error("WhatsApp worker directory not found at %s", whatsapp_dir)
        return False

    pnpm = find_pnpm()
    env = os.environ.copy()
    pnpm_parent = str(Path(pnpm).resolve().parent) if Path(pnpm).exists() else ""
    if pnpm_parent:
        env["PATH"] = f"{pnpm_parent}{os.pathsep}{env.get('PATH', '')}"

    process = start_background_process(
        [pnpm, "run", "worker"],
        whatsapp_dir,
        service_log_path(project_root, "whatsapp-worker"),
        logger,
        env=env,
    )
    return process is not None


def make_dispatch_batch_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def parse_send_at(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip().replace("T", " ")
    if not cleaned:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    raise ValueError(
        f"--send-at must be formatted as YYYY-MM-DD HH:MM, got {value!r}"
    )


def wait_until_send_at(send_at: datetime, dry_run: bool) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    now = datetime.now()
    wait_seconds = int((send_at - now).total_seconds())
    if wait_seconds <= 0:
        logger.warning(
            "Scheduled dispatch time %s is not in the future; running now.",
            send_at.strftime("%Y-%m-%d %H:%M"),
        )
        return

    if dry_run:
        logger.info(
            "DRY RUN: Scheduled dispatch time is %s; not waiting.",
            send_at.strftime("%Y-%m-%d %H:%M"),
        )
        return

    logger.info(
        "Dispatch scheduled for %s. Waiting %d minute(s).",
        send_at.strftime("%Y-%m-%d %H:%M"),
        max(1, (wait_seconds + 59) // 60),
    )
    while wait_seconds > 0:
        sleep_for = min(wait_seconds, 60)
        time.sleep(sleep_for)
        wait_seconds = int((send_at - datetime.now()).total_seconds())


async def wait_for_delivery_statuses(
    subscription: Any,
    expected_job_ids: set[str],
    timeout_seconds: int,
    logger: logging.Logger,
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    statuses: dict[str, dict[str, Any]] = {}
    pending = set(expected_job_ids)
    deadline = time.monotonic() + timeout_seconds

    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break

        try:
            msg = await subscription.next_msg(timeout=remaining)
        except NATSTimeoutError:
            break
        except Exception as exc:
            logger.error("Failed while waiting for WhatsApp worker receipt: %s", exc)
            break

        try:
            payload = json.loads(msg.data.decode("utf-8"))
        except Exception as exc:
            logger.warning("Ignoring invalid WhatsApp status payload: %s", exc)
            continue

        job_id = str(payload.get("jobId") or payload.get("job_id") or "")
        if job_id not in expected_job_ids:
            continue

        statuses[job_id] = payload
        pending.discard(job_id)
        logger.info(
            "WhatsApp worker receipt: %s for %s -> %s",
            payload.get("status", "unknown"),
            payload.get("complaintCode") or payload.get("complaint_code"),
            payload.get("target"),
        )

    return statuses, pending


async def connect_to_nats(
    logger: logging.Logger,
    timeout_seconds: int = DEFAULT_NATS_READY_TIMEOUT_SECONDS,
) -> NATSClient | None:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""

    while time.monotonic() < deadline:
        try:
            return await nats.connect(NATS_SERVER_URL)
        except Exception as exc:
            last_error = str(exc)
            await asyncio.sleep(0.5)

    logger.error(
        "Could not connect to NATS at %s after %s seconds: %s",
        NATS_SERVER_URL,
        timeout_seconds,
        last_error or "connection timed out",
    )
    return None


async def wait_for_whatsapp_worker_ready(
    js: Any,
    logger: logging.Logger,
    timeout_seconds: int = DEFAULT_WORKER_READY_TIMEOUT_SECONDS,
) -> bool:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""

    while time.monotonic() < deadline:
        try:
            stream_info = await js.stream_info(NATS_WHATSAPP_STREAM)
            if NATS_WHATSAPP_SUBJECT not in (stream_info.config.subjects or []):
                logger.error(
                    "NATS stream %s does not own subject %s.",
                    NATS_WHATSAPP_STREAM,
                    NATS_WHATSAPP_SUBJECT,
                )
                return False

            consumer_info = await js.consumer_info(
                NATS_WHATSAPP_STREAM,
                NATS_WHATSAPP_CONSUMER,
            )
            if int(getattr(consumer_info, "num_waiting", 0) or 0) > 0:
                logger.info(
                    "WhatsApp worker is ready (stream=%s, consumer=%s, waiting=%s).",
                    NATS_WHATSAPP_STREAM,
                    NATS_WHATSAPP_CONSUMER,
                    consumer_info.num_waiting,
                )
                return True

            last_error = (
                f"consumer {NATS_WHATSAPP_CONSUMER} exists but is not waiting"
            )
        except Exception as exc:
            last_error = str(exc)

        await asyncio.sleep(0.5)

    logger.error(
        "WhatsApp worker is not ready after %s seconds: %s",
        timeout_seconds,
        last_error or "consumer is not attached",
    )
    return False


async def ensure_nats_connected(
    project_root: Path,
    logger: logging.Logger,
) -> NATSClient | None:
    nc = await connect_to_nats(logger, timeout_seconds=2)
    if nc:
        logger.info("Connected to NATS Server.")
        return nc

    logger.info("NATS is not running; starting NATS JetStream automatically.")
    if not start_nats_server(project_root, logger):
        return None

    nc = await connect_to_nats(
        logger,
        timeout_seconds=DEFAULT_SERVICE_START_TIMEOUT_SECONDS,
    )
    if nc:
        logger.info("NATS JetStream started and is reachable.")
        return nc

    return None


async def ensure_whatsapp_worker_ready(
    project_root: Path,
    js: Any,
    logger: logging.Logger,
) -> bool:
    if await wait_for_whatsapp_worker_ready(
        js,
        logger,
        timeout_seconds=DEFAULT_WORKER_READY_TIMEOUT_SECONDS,
    ):
        return True

    logger.info("WhatsApp worker is not listening; starting it automatically.")
    if not start_whatsapp_worker(project_root, logger):
        return False

    return await wait_for_whatsapp_worker_ready(
        js,
        logger,
        timeout_seconds=DEFAULT_SERVICE_START_TIMEOUT_SECONDS,
    )


class DeliveryManager:
    def __init__(
        self, client: PaperlessClient, settings: PaperlessSettings, project_root: Path
    ):
        self.client = client
        self.settings = settings
        self.project_root = project_root
        self.logger = logging.getLogger(LOGGER_NAME)

        # Database setup
        self.db_path = self.project_root / "compliance_delivery.duckdb"
        self._init_duckdb()

        # Directories
        self.dispatch_dir = self.project_root / "dispatch_temp"
        self.dispatch_dir.mkdir(parents=True, exist_ok=True)

        # State
        self.metadata: dict[str, Any] = {}
        self.fields_by_name: dict[str, dict[str, Any]] = {}
        self.broadcast_phones: list[str] = []

    def _init_duckdb(self) -> None:
        """Initialize the separate audit log database."""
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS delivery_logs (
                    complaint_code TEXT,
                    recipient_phone TEXT,
                    sent_at TIMESTAMP,
                    report_document_id INTEGER,
                    delivery_status TEXT,
                    error_message TEXT
                )
            """)

    def log_delivery(
        self, complaint_code: str, phone: str, doc_id: int, status: str, error: str = ""
    ) -> None:
        """Insert an audit record into DuckDB."""
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                """
                INSERT INTO delivery_logs (complaint_code, recipient_phone, sent_at, report_document_id, delivery_status, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    complaint_code,
                    phone,
                    datetime.now(timezone.utc).replace(tzinfo=None),
                    doc_id,
                    status,
                    error,
                ],
            )

    async def setup(self) -> None:
        """Load Paperless metadata and CSV recipients."""
        self.logger.info("Resolving Paperless metadata for dispatch...")
        self.metadata = await resolve_metadata(self.client, self.settings)
        self.fields_by_name = self.metadata["custom_fields"]
        self._load_recipients()

    def _load_recipients(self) -> None:
        """Load the flat list of broadcast recipients from the CSV file."""
        csv_path = self.project_root / "crm-compliances-recepients.csv"
        if not csv_path.exists():
            self.logger.warning(
                f"Recipients file not found at {csv_path}. Cannot broadcast."
            )
            return

        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw_phone = row.get("Cell No", row.get("Phone Number", "")).strip()
                if not raw_phone:
                    continue

                clean_phone = re.sub(r"\D", "", raw_phone)
                if clean_phone.startswith("0"):
                    clean_phone = clean_phone[1:]
                if clean_phone and not clean_phone.startswith("92"):
                    clean_phone = "92" + clean_phone

                if clean_phone and clean_phone not in self.broadcast_phones:
                    self.broadcast_phones.append(clean_phone)

        self.logger.info(
            f"Loaded {len(self.broadcast_phones)} broadcast numbers from CSV."
        )

    def _get_field_id(self, field_name: str) -> int | None:
        field = self.fields_by_name.get(field_name.lower())
        return int(field["id"]) if field else None

    def _get_option_label(self, field_name: str, option_id: Any) -> str | None:
        """Extracts the text label for a given select option ID (e.g., PMDU or CRM)."""
        if not option_id:
            return None
        field = self.fields_by_name.get(field_name.lower())
        if not field:
            return None

        extra_data = field.get("extra_data") or {}
        if isinstance(extra_data, str):
            try:
                extra_data = json.loads(extra_data)
            except Exception:
                pass

        if isinstance(extra_data, dict):
            for opt in extra_data.get("select_options", []):
                if str(opt.get("id")) == str(option_id):
                    return opt.get("label")
        return None

    def _extract_doc_id(self, val: Any) -> int | None:
        """Safely extract the document ID from a custom field value."""
        if isinstance(val, list) and val:
            return int(val[-1])
        elif val:
            return int(val)
        return None

    async def fetch_pending_complaints(
        self,
        limit: int | None = None,
        specific_date: str | None = None,
        target_complaints: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Query Paperless for Main Complaints."""
        query_url = (
            f"/api/documents/?document_type__id={self.metadata['complaint_type_id']}"
        )
        if specific_date:
            query_url += f"&created__date={specific_date}"

        self.logger.info(f"Fetching candidates from Paperless API...")
        all_main_complaints = await self.client.paginated_results(query_url)

        status_field_id = self._get_field_id("Status")
        status_submitted_id = select_option_id(
            self.fields_by_name.get("status", {}), "Submitted"
        )

        candidates = []
        for doc in all_main_complaints:
            title = str(doc.get("title", ""))
            match = CRM_COMPLAINT_RE.search(title)
            complaint_code = match.group(0) if match else title.split("-")[0].strip()

            if (
                target_complaints is not None
                and complaint_code not in target_complaints
            ):
                continue

            cfs = doc.get("custom_fields", [])
            is_submitted = False
            is_delivered = False

            for cf in cfs:
                if int(cf.get("field", 0)) == status_field_id and str(
                    cf.get("value")
                ) == str(status_submitted_id):
                    is_submitted = True
                if (
                    int(cf.get("field", 0)) == COMPLIANCE_DELIVERED_CF_ID
                    and cf.get("value") is True
                ):
                    is_delivered = True

            if is_submitted:
                if (target_complaints is not None) or (not is_delivered):
                    candidates.append(doc)
                    if limit and len(candidates) >= limit:
                        break

        return candidates

    async def download_report_pdf(
        self,
        report_doc_id: int,
        file_suffix: str,
        output_dir: Path | None = None,
    ) -> Path | None:
        """Downloads the auto-converted PDF preview from Paperless-ngx."""
        download_url = self.client.url(f"/api/documents/{report_doc_id}/preview/")
        target_dir = output_dir or self.dispatch_dir
        target_dir.mkdir(parents=True, exist_ok=True)
        local_path = target_dir / f"{report_doc_id}_{file_suffix}.pdf"

        async with self.client.session.get(
            download_url, headers=self.client.headers
        ) as response:
            if response.status == 200:
                with open(local_path, "wb") as f:
                    f.write(await response.read())
                return local_path
            else:
                self.logger.error(
                    f"Failed to download PDF preview for {report_doc_id}: Status {response.status}"
                )
                return None

    def merge_pdfs(self, pdf_paths: list[Path], output_path: Path) -> Path | None:
        """Merges multiple PDFs into a single document."""
        try:
            writer = PdfWriter()
            for path in pdf_paths:
                if path and path.exists() and path.stat().st_size > 0:
                    try:
                        writer.append(path)
                    except Exception as e:
                        self.logger.warning(f"Failed to append PDF {path.name}: {e}")

            with open(output_path, "wb") as out_file:
                writer.write(out_file)
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to merge PDFs: {e}")
            return None

    async def mark_as_delivered(
        self, main_doc_id: int, existing_cfs: list[dict[str, Any]]
    ) -> None:
        """Updates the Paperless document setting Compliance Delivered (ID 27) to True."""
        updated_cfs = []
        field_exists = False

        for cf in existing_cfs:
            if int(cf.get("field", 0)) == COMPLIANCE_DELIVERED_CF_ID:
                cf["value"] = True
                field_exists = True
            updated_cfs.append(cf)

        if not field_exists:
            updated_cfs.append({"field": COMPLIANCE_DELIVERED_CF_ID, "value": True})

        await self.client.patch_json(
            f"/api/documents/{main_doc_id}/", {"custom_fields": updated_cfs}
        )


async def run_dispatch(args: argparse.Namespace, project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    settings = load_paperless_settings(project_root)

    # ---------------------------------------------------------
    # TRACKING VARIABLES FOR SUMMARY REPORT
    # ---------------------------------------------------------
    target_complaints = None
    dispatched_success = set()
    dry_run_ready = set()
    missing_deo_reports = set()
    failed_downloads = set()
    failed_nats = set()
    failed_delivery = set()
    queued_success = set()
    found_candidates_codes = set()
    preview_records: list[dict[str, Any]] = []
    preview_skipped: list[dict[str, str]] = []
    combined_items: list[dict[str, Any]] = []
    dispatch_batch_id = make_dispatch_batch_id()
    status_subject = f"{NATS_WHATSAPP_STATUS_SUBJECT}.{dispatch_batch_id}"
    status_subscription = None
    logger.info("Dispatch batch id: %s", dispatch_batch_id)

    # 1. Process the CSV file if provided
    if args.csv:
        csv_path = project_root / args.csv
        if not csv_path.exists():
            logger.error(f"Target CSV file not found: {csv_path}")
            return

        target_complaints = set()
        with open(csv_path, mode="r", encoding="utf-8") as f:
            content = f.read().splitlines()
            if content:
                has_header = "complaint" in content[0].lower()
                reader = csv.reader(content)

                if has_header:
                    headers = next(reader)
                    col_idx = next(
                        (i for i, h in enumerate(headers) if "complaint" in h.lower()),
                        0,
                    )
                else:
                    col_idx = 0

                for row in reader:
                    if len(row) > col_idx:
                        val = row[col_idx].strip()
                        if val:
                            target_complaints.add(val)

        logger.info(
            f"Loaded {len(target_complaints)} targeted complaints from {args.csv}"
        )
        if args.date:
            logger.info(
                "CSV target list provided; ignoring --date filter so selected complaints are not excluded by main-document date."
            )

    # 2. Connect to NATS only for a real dispatch. Dry-run still queries Paperless,
    # but it must not require the message broker or publish anything.
    nc = None
    js = None
    if args.dry_run or args.preview:
        logger.info("%s: Skipping NATS connection.", "PREVIEW" if args.preview else "DRY RUN")
    else:
        try:
            nc = await ensure_nats_connected(project_root, logger)
            if nc is None:
                return
            js = nc.jetstream()
            if not await ensure_whatsapp_worker_ready(project_root, js, logger):
                await nc.close()
                return
            if not args.queue_only:
                status_subscription = await nc.subscribe(status_subject)
                await nc.flush()
                logger.info(
                    "Waiting for WhatsApp worker receipts on %s", status_subject
                )
        except Exception as exc:
            logger.error("Could not connect to NATS at %s: %s", NATS_SERVER_URL, exc)
            logger.error("Start NATS/WhatsApp worker before running a real dispatch.")
            return

    async with PaperlessClient(settings) as client:
        manager = DeliveryManager(client, settings, project_root)
        await manager.setup()
        combined_staging_dir = (
            manager.dispatch_dir / ".staging" / dispatch_batch_id
            if args.attachment_mode == "combined"
            else None
        )

        if not manager.broadcast_phones:
            logger.error("No valid phones found in CSV. Halting dispatch.")
            if nc:
                await nc.close()
            return

        candidates = await manager.fetch_pending_complaints(
            limit=args.limit,
            specific_date=None if target_complaints is not None else args.date,
            target_complaints=target_complaints,
        )

        if not candidates:
            logger.info("No matching compliance reports found to dispatch.")
            if args.preview:
                if target_complaints is not None:
                    preview_skipped.extend(
                        {
                            "complaint_number": complaint_code,
                            "reason": "No matching Submitted complaint ready for dispatch.",
                        }
                        for complaint_code in sorted(target_complaints)
                    )
                preview_dir = write_dispatch_preview(
                    project_root,
                    dispatch_batch_id,
                    args.attachment_mode,
                    [],
                    preview_skipped,
                )
                logger.info("Empty compliance dispatch preview written: %s", preview_dir)
                logger.info("Open preview: %s", preview_dir / "preview.html")
            if nc:
                await nc.close()
            return

        logger.info(f"Found {len(candidates)} compliance report(s) ready for dispatch.")

        deo_report_field_id = manager._get_field_id("DEO MEE Report")
        ddeo_report_field_id = manager._get_field_id("DDEO Report")
        inst_report_field_id = manager._get_field_id("Institute Report")
        source_field_id = manager._get_field_id("Source")

        for doc in candidates:
            title = str(doc.get("title", ""))
            match = CRM_COMPLAINT_RE.search(title)
            complaint_code = (
                match.group(0)
                if match
                else title.split("-")[0].replace("Main Complaint", "").strip()
            )

            found_candidates_codes.add(complaint_code)

            report_doc_ids = {
                "deo": None,
                "ddeo": None,
                "institute": None,
                "main": doc["id"],
            }
            source_val = "PMDU/CRM"

            for cf in doc.get("custom_fields", []):
                field_id = int(cf.get("field", 0))
                val = cf.get("value")

                if field_id == source_field_id:
                    label = manager._get_option_label("Source", val)
                    if label:
                        source_val = label
                elif field_id == deo_report_field_id:
                    report_doc_ids["deo"] = manager._extract_doc_id(val)
                elif field_id == ddeo_report_field_id:
                    report_doc_ids["ddeo"] = manager._extract_doc_id(val)
                elif field_id == inst_report_field_id:
                    report_doc_ids["institute"] = manager._extract_doc_id(val)

            if not report_doc_ids["deo"]:
                logger.warning(
                    f"Main complaint {complaint_code} has no DEO MEE Report. Skipping."
                )
                missing_deo_reports.add(complaint_code)
                preview_skipped.append(
                    {
                        "complaint_number": complaint_code,
                        "reason": "No linked DEO MEE Report in Paperless.",
                    }
                )
                continue

            if args.attachment_mode == "combined":
                logger.info("Staging %s for the combined batch PDF.", complaint_code)
            else:
                logger.info(
                    "Processing dispatch for %s -> %d recipient(s).",
                    complaint_code,
                    len(manager.broadcast_phones),
                )

            if args.dry_run:
                logger.info(
                    "DRY RUN: Would prepare %s PDF delivery from docs %s and send.",
                    args.attachment_mode,
                    report_doc_ids,
                )
                dry_run_ready.add(complaint_code)
                continue

            documents = await prepare_dispatch_documents(
                manager,
                complaint_code,
                report_doc_ids,
                output_dir=combined_staging_dir,
            )
            if not documents:
                logger.error(
                    f"Failed to download valid PDFs for {complaint_code}. Halting this dispatch."
                )
                failed_downloads.add(complaint_code)
                preview_skipped.append(
                    {
                        "complaint_number": complaint_code,
                        "reason": "Could not download or prepare valid PDF files.",
                    }
                )
                continue

            message_text = f"Compliance report for {source_val} Complaint: {complaint_code}"
            if args.attachment_mode == "combined":
                combined_items.append(
                    {
                        "complaint_code": complaint_code,
                        "source": source_val,
                        "documents": documents,
                        "paperless_document_ids": report_doc_ids,
                        "main_document": doc,
                    }
                )
                continue

            if args.preview:
                preview_records.append(
                    {
                        "complaint_code": complaint_code,
                        "source": source_val,
                        "text": message_text,
                        "attachment_mode": args.attachment_mode,
                        "documents": documents,
                        "recipients": list(manager.broadcast_phones),
                        "paperless_document_ids": report_doc_ids,
                    }
                )
                dry_run_ready.add(complaint_code)
                continue

            # 3. Publish to NATS for WhatsApp Bot
            has_nats_error = False
            queued_jobs: dict[str, str] = {}
            for phone in manager.broadcast_phones:
                job_dedup_id = (
                    f"compliance_{dispatch_batch_id}_{complaint_code}_{phone}"
                )

                payload = {
                    "batchId": dispatch_batch_id,
                    "complaintCode": complaint_code,
                    "jobId": job_dedup_id,
                    "target": phone,
                    "type": "contact",
                    "text": message_text,
                    "attachmentTextMode": "caption",
                    "delayMs": 1500,
                }
                payload["documents"] = documents
                payload["attachmentTextMode"] = "separate"
                if not args.queue_only:
                    payload["statusSubject"] = status_subject

                try:
                    pub_ack = await js.publish(
                        NATS_WHATSAPP_SUBJECT,
                        json.dumps(payload).encode("utf-8"),
                        headers={"Nats-Msg-Id": job_dedup_id},
                    )
                    if getattr(pub_ack, "duplicate", False):
                        has_nats_error = True
                        message = (
                            "NATS rejected duplicate publish for "
                            f"{complaint_code} to {phone}; "
                            f"stream={pub_ack.stream} seq={pub_ack.seq}"
                        )
                        logger.error(message)
                        manager.log_delivery(
                            complaint_code,
                            phone,
                            report_doc_ids["main"],
                            "duplicate_not_queued",
                            message,
                        )
                        continue

                    logger.info(
                        "NATS Stored: Compliance %s queued for %s (stream=%s seq=%s)",
                        complaint_code,
                        phone,
                        pub_ack.stream,
                        pub_ack.seq,
                    )
                    manager.log_delivery(
                        complaint_code, phone, report_doc_ids["main"], "queued_to_nats"
                    )
                    queued_jobs[job_dedup_id] = phone
                except Exception as e:
                    has_nats_error = True
                    logger.error(f"Failed to queue NATS message for {phone}: {e}")
                    manager.log_delivery(
                        complaint_code, phone, report_doc_ids["main"], "failed", str(e)
                    )

            if has_nats_error:
                failed_nats.add(complaint_code)
                continue

            if len(queued_jobs) != len(manager.broadcast_phones):
                failed_nats.add(complaint_code)
                logger.error(
                    "Queued %d/%d WhatsApp jobs for %s; not marking delivered.",
                    len(queued_jobs),
                    len(manager.broadcast_phones),
                    complaint_code,
                )
                continue

            if args.queue_only:
                queued_success.add(complaint_code)
                logger.info(
                    "Queued %s for %d recipient(s); not marking Paperless delivered in queue-only mode.",
                    complaint_code,
                    len(queued_jobs),
                )
                continue

            if status_subscription is None:
                failed_delivery.add(complaint_code)
                logger.error(
                    "No WhatsApp receipt subscription is active; not marking %s delivered.",
                    complaint_code,
                )
                continue

            statuses, pending_jobs = await wait_for_delivery_statuses(
                status_subscription,
                set(queued_jobs),
                args.delivery_timeout,
                logger,
            )
            failed_jobs = {
                job_id: payload
                for job_id, payload in statuses.items()
                if payload.get("status") != "delivered"
            }

            for job_id, payload in statuses.items():
                status = str(payload.get("status") or "unknown")
                error = str(payload.get("error") or "")
                phone = queued_jobs.get(job_id, "")
                manager.log_delivery(
                    complaint_code,
                    phone,
                    report_doc_ids["main"],
                    f"whatsapp_{status}",
                    error,
                )

            for job_id in pending_jobs:
                phone = queued_jobs.get(job_id, "")
                manager.log_delivery(
                    complaint_code,
                    phone,
                    report_doc_ids["main"],
                    "whatsapp_receipt_timeout",
                    f"No worker receipt within {args.delivery_timeout} seconds.",
                )

            if pending_jobs or failed_jobs:
                failed_delivery.add(complaint_code)
                logger.error(
                    "WhatsApp delivery incomplete for %s: delivered=%d failed=%d timeout=%d",
                    complaint_code,
                    len(queued_jobs) - len(failed_jobs) - len(pending_jobs),
                    len(failed_jobs),
                    len(pending_jobs),
                )
                continue

            dispatched_success.add(complaint_code)

            # 4. Mark as Delivered in Paperless only after WhatsApp worker receipts.
            try:
                await manager.mark_as_delivered(doc["id"], doc.get("custom_fields", []))
                logger.info(
                    f"Paperless Updated: {complaint_code} marked as Compliance Delivered."
                )
            except Exception as e:
                logger.error(
                    f"Failed to update Paperless status for {complaint_code}: {e}"
                )
            else:
                logger.info(
                    "Confirmed %s delivered to all %d recipient(s).",
                    complaint_code,
                    len(queued_jobs),
                )

        if args.attachment_mode == "combined" and combined_items:
            complaint_codes = [item["complaint_code"] for item in combined_items]
            all_paths = [
                Path(document["path"])
                for item in combined_items
                for document in item["documents"]
            ]
            master_path = manager.dispatch_dir / f"All_CRM_Compliances_{dispatch_batch_id}.pdf"
            master_pdf = manager.merge_pdfs(all_paths, master_path)
            if combined_staging_dir is not None:
                shutil.rmtree(combined_staging_dir, ignore_errors=True)
            if master_pdf is None or not master_pdf.exists() or master_pdf.stat().st_size == 0:
                logger.error("Failed to build the combined compliance batch PDF.")
                failed_downloads.update(complaint_codes)
            else:
                master_document = delivery_document(
                    master_pdf,
                    f"CRM Compliance Reports - {len(complaint_codes)} Complaints.pdf",
                )
                batch_message = (
                    f"Combined CRM compliance package for {len(complaint_codes)} complaints."
                )
                if args.preview:
                    preview_records.append(
                        {
                            "complaint_code": f"Combined batch - {len(complaint_codes)} complaints",
                            "complaint_codes": complaint_codes,
                            "complaint_count": len(complaint_codes),
                            "source": "CRM Portal",
                            "text": batch_message,
                            "attachment_mode": "combined",
                            "documents": [master_document],
                            "recipients": list(manager.broadcast_phones),
                        }
                    )
                    dry_run_ready.update(complaint_codes)
                else:
                    has_nats_error = False
                    queued_jobs: dict[str, str] = {}
                    for phone in manager.broadcast_phones:
                        job_id = f"compliance_{dispatch_batch_id}_combined_{phone}"
                        payload = {
                            "batchId": dispatch_batch_id,
                            "complaintCode": "CRM-COMPLIANCE-BATCH",
                            "jobId": job_id,
                            "target": phone,
                            "type": "contact",
                            "documentPath": master_document["path"],
                            "documentFilename": master_document["filename"],
                            "documentMimetype": master_document["mimetype"],
                            "text": batch_message,
                            "attachmentTextMode": "caption",
                            "delayMs": 1500,
                        }
                        if not args.queue_only:
                            payload["statusSubject"] = status_subject
                        try:
                            ack = await js.publish(
                                NATS_WHATSAPP_SUBJECT,
                                json.dumps(payload).encode("utf-8"),
                                headers={"Nats-Msg-Id": job_id},
                            )
                            if getattr(ack, "duplicate", False):
                                raise RuntimeError(
                                    f"NATS rejected duplicate batch publish (stream={ack.stream} seq={ack.seq})."
                                )
                            queued_jobs[job_id] = phone
                            logger.info(
                                "Queued combined PDF containing %d complaints to %s (stream=%s seq=%s).",
                                len(complaint_codes),
                                phone,
                                ack.stream,
                                ack.seq,
                            )
                            for item in combined_items:
                                manager.log_delivery(
                                    item["complaint_code"],
                                    phone,
                                    item["paperless_document_ids"]["main"],
                                    "queued_to_nats_combined_batch",
                                )
                        except Exception as exc:
                            has_nats_error = True
                            logger.error("Failed to queue combined PDF for %s: %s", phone, exc)

                    if has_nats_error or len(queued_jobs) != len(manager.broadcast_phones):
                        failed_nats.update(complaint_codes)
                    elif args.queue_only:
                        queued_success.update(complaint_codes)
                    elif status_subscription is None:
                        failed_delivery.update(complaint_codes)
                        logger.error("No WhatsApp receipt subscription is active for combined batch.")
                    else:
                        statuses, pending_jobs = await wait_for_delivery_statuses(
                            status_subscription,
                            set(queued_jobs),
                            args.delivery_timeout,
                            logger,
                        )
                        failed_jobs = {
                            job_id: payload
                            for job_id, payload in statuses.items()
                            if payload.get("status") != "delivered"
                        }
                        for job_id, payload in statuses.items():
                            phone = queued_jobs.get(job_id, "")
                            delivery_status = str(payload.get("status") or "unknown")
                            error = str(payload.get("error") or "")
                            for item in combined_items:
                                manager.log_delivery(
                                    item["complaint_code"],
                                    phone,
                                    item["paperless_document_ids"]["main"],
                                    f"whatsapp_{delivery_status}_combined_batch",
                                    error,
                                )
                        for job_id in pending_jobs:
                            phone = queued_jobs.get(job_id, "")
                            for item in combined_items:
                                manager.log_delivery(
                                    item["complaint_code"],
                                    phone,
                                    item["paperless_document_ids"]["main"],
                                    "whatsapp_receipt_timeout_combined_batch",
                                    f"No worker receipt within {args.delivery_timeout} seconds.",
                                )
                        if pending_jobs or failed_jobs:
                            failed_delivery.update(complaint_codes)
                            logger.error(
                                "Combined batch delivery incomplete: delivered=%d failed=%d timeout=%d",
                                len(queued_jobs) - len(failed_jobs) - len(pending_jobs),
                                len(failed_jobs),
                                len(pending_jobs),
                            )
                        else:
                            for item in combined_items:
                                complaint_code = item["complaint_code"]
                                main_doc = item["main_document"]
                                try:
                                    await manager.mark_as_delivered(
                                        main_doc["id"], main_doc.get("custom_fields", [])
                                    )
                                except Exception as exc:
                                    logger.error(
                                        "Failed to mark %s delivered after combined batch: %s",
                                        complaint_code,
                                        exc,
                                    )
                                else:
                                    dispatched_success.add(complaint_code)
                            logger.info(
                                "Confirmed combined PDF delivered to all %d recipient(s) for %d complaints.",
                                len(queued_jobs),
                                len(dispatched_success),
                            )

        if combined_staging_dir is not None and combined_staging_dir.exists():
            shutil.rmtree(combined_staging_dir, ignore_errors=True)

    if nc:
        await nc.close()

    if args.preview:
        preview_dir = write_dispatch_preview(
            project_root,
            dispatch_batch_id,
            args.attachment_mode,
            preview_records,
            preview_skipped,
        )
        logger.info("Compliance dispatch preview written: %s", preview_dir)
        logger.info("Open preview: %s", preview_dir / "preview.html")

    # ---------------------------------------------------------
    # PRINT SUMMARY REPORT
    # ---------------------------------------------------------
    logger.info("")
    logger.info("==================================================")
    logger.info("             DISPATCH SUMMARY REPORT              ")
    logger.info("==================================================")

    if target_complaints is not None:
        logger.info(f"Total Targeted (from CSV): {len(target_complaints)}")
        not_found = target_complaints - found_candidates_codes if not args.limit else set()

        if args.dry_run or args.preview:
            logger.info(f"Ready for Dispatch       : {len(dry_run_ready)}")
        elif args.queue_only:
            logger.info(f"Successfully Queued      : {len(queued_success)}")
        else:
            logger.info(f"Successfully Dispatched  : {len(dispatched_success)}")

        if not_found:
            logger.warning(
                f"➔ Not Found / Not 'Submitted' ({len(not_found)}): {', '.join(not_found)}"
            )
            logger.warning(
                "   (Fix: Ensure these exist in Paperless and Status = 'Submitted')"
            )
        elif args.limit and len(target_complaints) > len(found_candidates_codes):
            logger.info(
                "Limit applied: %d additional CSV target(s) were not evaluated in this run.",
                len(target_complaints) - len(found_candidates_codes),
            )
    else:
        logger.info(f"Total Found Pending      : {len(candidates)}")
        if args.dry_run or args.preview:
            logger.info(f"Ready for Dispatch       : {len(dry_run_ready)}")
        elif args.queue_only:
            logger.info(f"Successfully Queued      : {len(queued_success)}")
        else:
            logger.info(f"Successfully Dispatched  : {len(dispatched_success)}")

    if missing_deo_reports:
        logger.warning(
            f"➔ Skipped (No DEO Report) ({len(missing_deo_reports)}): {', '.join(missing_deo_reports)}"
        )
        logger.warning(
            "   (Fix: Upload and link a DEO Report to these main complaints)"
        )

    if failed_downloads:
        logger.error(
            f"➔ Failed (PDF Error)      ({len(failed_downloads)}): {', '.join(failed_downloads)}"
        )

    if failed_nats:
        logger.error(
            f"➔ Failed (NATS Error)     ({len(failed_nats)}): {', '.join(failed_nats)}"
        )

    if failed_delivery:
        logger.error(
            f"➔ Failed (WhatsApp Confirm) ({len(failed_delivery)}): {', '.join(failed_delivery)}"
        )

    logger.info("==================================================")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dispatch Compliance Reports via WhatsApp"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of reports to send in this run.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Filter by Paperless creation date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the process without sending or updating Paperless.",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Download and prepare the exact dispatch files, then write an HTML preview without sending.",
    )
    parser.add_argument(
        "--attachment-mode",
        choices=ATTACHMENT_MODES,
        default="combined",
        help="Send one combined PDF per complaint or each report PDF separately.",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="Path to a CSV file containing Complaint Numbers to specifically dispatch.",
    )
    parser.add_argument(
        "--send-at",
        type=str,
        default=None,
        help="Wait until local date/time before dispatching (YYYY-MM-DD HH:MM).",
    )
    parser.add_argument(
        "--delivery-timeout",
        type=int,
        default=DEFAULT_DELIVERY_TIMEOUT_SECONDS,
        help="Seconds to wait for WhatsApp worker receipts per complaint.",
    )
    parser.add_argument(
        "--queue-only",
        action="store_true",
        help="Only queue NATS jobs; do not wait for worker receipts or mark Paperless delivered.",
    )

    args = parser.parse_args()
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent

    try:
        send_at = parse_send_at(args.send_at)
        if args.preview and args.dry_run:
            raise ValueError("--preview and --dry-run cannot be used together.")
        if args.delivery_timeout <= 0:
            raise ValueError("--delivery-timeout must be greater than zero.")
        logger.info(
            f"Starting Dispatch Process (Limit: {args.limit}, Date: {args.date}, Target CSV: {args.csv}, Send At: {args.send_at}, Attachment Mode: {args.attachment_mode}, Preview: {args.preview}, Dry-Run: {args.dry_run}, Queue-Only: {args.queue_only}, Delivery Timeout: {args.delivery_timeout}s)"
        )
        if send_at:
            wait_until_send_at(send_at, args.dry_run)
        asyncio.run(run_dispatch(args, project_root))
        logger.info("Dispatch process completed.")
        return 0
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        return 130
    except Exception:
        logger.exception("Critical error during dispatch process.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
