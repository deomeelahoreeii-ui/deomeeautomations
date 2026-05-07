from __future__ import annotations

import asyncio
import csv
import hashlib
import logging
import mimetypes
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import Message
from pathlib import Path
from typing import Any

import duckdb
import orjson
from nats.aio.client import Client as NATS

from paperless import PaperlessClient, PaperlessSettings, clean_text

LOGGER_NAME = "pmdu_automation"
MAX_WHATSAPP_CAPTION_CHARS = 900


@dataclass(frozen=True)
class NotifySettings:
    paperless: PaperlessSettings
    config_path: Path
    duckdb_path: Path
    action: str = "preview"
    command: str = "notify-preview"
    recipient_roles: frozenset[str] | None = None
    include_group_summary: bool = False
    files: str = "none"
    scope: str = "all"
    tehsil: str | None = None
    message_mode: str | None = None


@dataclass(frozen=True)
class OfficerRow:
    district: str
    wing: str
    tehsil: str
    deo_name: str
    deo_number: str
    ddeo_name: str
    ddeo_number: str
    aeo_name: str
    aeo_number: str


@dataclass(frozen=True)
class WhatsAppGroup:
    name: str
    type: str
    target: str


def load_json(path: Path) -> dict[str, Any]:
    data = orjson.loads(path.read_bytes())
    if not isinstance(data, dict):
        raise ValueError(f"Notification config must be a JSON object: {path}")
    return data


def normalize_label(value: Any) -> str:
    value = clean_text("" if value is None else str(value)).lower()
    value = value.replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "", value)


def normalize_tehsil(value: Any) -> str:
    text = clean_text("" if value is None else str(value))
    if not text:
        return ""
    text = re.sub(r"^lahore\s+", "", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip().upper()


def normalize_phone(value: str) -> str:
    digits = re.sub(r"\D+", "", value)
    if not digits:
        return ""
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0"):
        digits = digits[1:]
    if digits.startswith("92"):
        return digits
    if digits.startswith("3") and len(digits) == 10:
        return f"92{digits}"
    return digits


def option_maps(
    custom_fields: list[dict[str, Any]],
) -> tuple[dict[int, str], dict[tuple[int, str], str]]:
    field_names = {
        int(field["id"]): str(field.get("name", "")) for field in custom_fields
    }
    option_labels: dict[tuple[int, str], str] = {}
    for field in custom_fields:
        field_id = int(field["id"])
        for option in field.get("extra_data", {}).get("select_options", []):
            option_labels[(field_id, str(option.get("id")))] = str(
                option.get("label", "")
            )
    return field_names, option_labels


def decoded_custom_fields(
    document: dict[str, Any],
    field_names: dict[int, str],
    option_labels: dict[tuple[int, str], str],
) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for item in document.get("custom_fields", []):
        field_id = int(item["field"])
        name = field_names.get(field_id, str(field_id))
        value = item.get("value")
        if isinstance(value, str):
            value = option_labels.get((field_id, value), value)
        decoded[name] = value
    return decoded


def raw_custom_field(
    document: dict[str, Any], field_name: str, field_names: dict[int, str]
) -> Any:
    wanted = normalize_label(field_name)
    for item in document.get("custom_fields", []):
        if normalize_label(field_names.get(int(item["field"]), "")) == wanted:
            return item.get("value")
    return None


def load_officers(path: Path) -> list[OfficerRow]:
    rows: list[OfficerRow] = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                OfficerRow(
                    district=clean_text(row.get("District")),
                    wing=clean_text(row.get("Wing")),
                    deo_name=clean_text(row.get("DEO MEE Focal Person Name")),
                    deo_number=normalize_phone(
                        clean_text(row.get("DEO MEE Focal Person Number"))
                    ),
                    tehsil=normalize_tehsil(row.get("Tehsil")),
                    ddeo_name=clean_text(row.get("DDEO Name")),
                    ddeo_number=normalize_phone(
                        clean_text(row.get("DDEO CELL NUMBER"))
                    ),
                    aeo_name=clean_text(row.get("AEO NAME")),
                    aeo_number=normalize_phone(clean_text(row.get("AEO CELL NUMBER"))),
                )
            )
    return rows


def load_whatsapp_groups(path: Path) -> list[WhatsAppGroup]:
    if not path.exists():
        return []

    groups: list[WhatsAppGroup] = []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            enabled = clean_text(row.get("enabled")).lower()
            if enabled not in {"1", "true", "yes", "y", "on"}:
                continue
            target_type = clean_text(row.get("type")) or "group"
            target = clean_text(row.get("target"))
            if not target:
                continue
            groups.append(
                WhatsAppGroup(
                    name=clean_text(row.get("name")) or target,
                    type=target_type,
                    target=target,
                )
            )
    return groups


def complaint_code(fields: dict[str, Any], document: dict[str, Any]) -> str:
    candidates = [
        clean_text(fields.get("Complaint Number")),
        clean_text(document.get("title")),
        str(document["id"]),
    ]
    for candidate in candidates:
        match = re.search(r"\b(?:\d{3,}-\d{4,}|PU\d{6}-\d{6,})\b", candidate)
        if match:
            return match.group(0)
    fallback = candidates[0] or candidates[1] or candidates[2]
    fallback = re.sub(
        r"\s+-\s+Main Complaint(?:\s+-\s+v\d+)?\s*$", "", fallback, flags=re.IGNORECASE
    )
    return clean_text(fallback)


def complaint_source_label(code: str) -> str:
    if re.fullmatch(r"\d{3,}-\d{4,}", code):
        return "CRM Portal"
    if re.fullmatch(r"PU\d{6}-\d{6,}", code, flags=re.IGNORECASE):
        return "PM Citizen Portal"
    return "Other"


def complaint_matches(document_fields: dict[str, Any], config: dict[str, Any]) -> bool:
    paperless_config = config.get("paperless", {})
    wanted_status = paperless_config.get("status", "Under Investigation")
    wanted_role = paperless_config.get("document_role", "Main Complaint")
    wanted_source = paperless_config.get("source")
    require_tehsil = bool(paperless_config.get("require_tehsil", False))

    if normalize_label(document_fields.get("Status")) != normalize_label(wanted_status):
        return False
    if normalize_label(document_fields.get("Document Role")) != normalize_label(
        wanted_role
    ):
        return False
    if wanted_source and normalize_label(
        document_fields.get("Source")
    ) != normalize_label(wanted_source):
        return False
    if require_tehsil and not clean_text(document_fields.get("Tehsil")):
        return False
    return True


def init_notification_db(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_batches (
            batch_id TEXT PRIMARY KEY,
            command TEXT,
            action TEXT,
            scope TEXT,
            recipient_roles TEXT,
            include_group_summary BOOLEAN,
            files_profile TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            matched_complaints INTEGER,
            prepared_jobs INTEGER,
            published_jobs INTEGER,
            config JSON
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS complaint_status_history (
            complaint_document_id INTEGER PRIMARY KEY,
            complaint_number TEXT,
            status TEXT,
            observed_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS whatsapp_notifications (
            notification_id TEXT PRIMARY KEY,
            batch_id TEXT,
            complaint_document_id INTEGER,
            complaint_number TEXT,
            recipient_role TEXT,
            recipient_name TEXT,
            recipient_target TEXT,
            message_mode TEXT,
            detail_level TEXT,
            sent_at TIMESTAMP,
            delivery_enabled BOOLEAN,
            nats_subject TEXT,
            job_payload JSON,
            fingerprint TEXT
        )
        """
    )
    columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info('whatsapp_notifications')"
        ).fetchall()
    }
    if "batch_id" not in columns:
        conn.execute("ALTER TABLE whatsapp_notifications ADD COLUMN batch_id TEXT")
    if "fingerprint" not in columns:
        conn.execute("ALTER TABLE whatsapp_notifications ADD COLUMN fingerprint TEXT")


def record_batch_start(
    conn: duckdb.DuckDBPyConnection,
    settings: NotifySettings,
    config: dict[str, Any],
) -> str:
    batch_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO notification_batches
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, ?)
        """,
        [
            batch_id,
            settings.command,
            settings.action,
            settings.scope,
            ",".join(sorted(settings.recipient_roles or [])),
            settings.include_group_summary,
            settings.files,
            datetime.now(timezone.utc).replace(tzinfo=None),
            orjson.dumps(config).decode("utf-8"),
        ],
    )
    return batch_id


def record_batch_complete(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    matched_complaints: int,
    prepared_jobs: int,
    published_jobs: int,
) -> None:
    conn.execute(
        """
        UPDATE notification_batches
        SET completed_at = ?,
            matched_complaints = ?,
            prepared_jobs = ?,
            published_jobs = ?
        WHERE batch_id = ?
        """,
        [
            datetime.now(timezone.utc).replace(tzinfo=None),
            matched_complaints,
            prepared_jobs,
            published_jobs,
            batch_id,
        ],
    )


def previous_status(
    conn: duckdb.DuckDBPyConnection,
    document_id: int,
) -> str | None:
    row = conn.execute(
        """
        SELECT status
        FROM complaint_status_history
        WHERE complaint_document_id = ?
        """,
        [document_id],
    ).fetchone()
    return clean_text(row[0]) if row and row[0] else None


def record_observed_statuses(
    conn: duckdb.DuckDBPyConnection,
    complaints: list[tuple[dict[str, Any], dict[str, Any]]],
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    for document, fields in complaints:
        conn.execute(
            """
            INSERT OR REPLACE INTO complaint_status_history
            VALUES (?, ?, ?, ?)
            """,
            [
                int(document["id"]),
                complaint_code(fields, document),
                clean_text(fields.get("Status")),
                now,
            ],
        )


def last_sent_at(
    conn: duckdb.DuckDBPyConnection,
    document_id: int,
    recipient_role: str,
    recipient_target: str,
) -> datetime | None:
    row = conn.execute(
        """
        SELECT max(sent_at)
        FROM whatsapp_notifications
        WHERE complaint_document_id = ?
          AND recipient_role = ?
          AND recipient_target = ?
          AND delivery_enabled = true
        """,
        [document_id, recipient_role, recipient_target],
    ).fetchone()
    return row[0] if row and row[0] else None


def should_send(
    conn: duckdb.DuckDBPyConnection,
    document_id: int,
    recipient_role: str,
    recipient_target: str,
    min_minutes: int,
) -> bool:
    if min_minutes <= 0:
        return True
    sent_at = last_sent_at(conn, document_id, recipient_role, recipient_target)
    if not sent_at:
        return True
    if sent_at.tzinfo is None:
        sent_at = sent_at.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - sent_at >= timedelta(minutes=min_minutes)


def latest_sent_fingerprint(
    conn: duckdb.DuckDBPyConnection,
    document_id: int,
    recipient_role: str,
    recipient_target: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT fingerprint
        FROM whatsapp_notifications
        WHERE complaint_document_id = ?
          AND recipient_role = ?
          AND recipient_target = ?
          AND delivery_enabled = true
        ORDER BY sent_at DESC
        LIMIT 1
        """,
        [document_id, recipient_role, recipient_target],
    ).fetchone()
    return clean_text(row[0]) if row and row[0] else None


def should_send_for_scope(
    conn: duckdb.DuckDBPyConnection,
    document_id: int,
    current_status: str,
    recipient_role: str,
    recipient_target: str,
    min_minutes: int,
    scope: str,
    fingerprint: str,
) -> bool:
    if not should_send(
        conn, document_id, recipient_role, recipient_target, min_minutes
    ):
        return False
    if scope == "all":
        return True
    previous_fingerprint = latest_sent_fingerprint(
        conn, document_id, recipient_role, recipient_target
    )
    if scope == "new":
        return previous_fingerprint is None
    if scope == "new-or-updated":
        return previous_fingerprint != fingerprint
    if scope == "newly-under-investigation":
        old_status = previous_status(conn, document_id)
        return bool(old_status) and normalize_label(old_status) != normalize_label(
            current_status
        )
    raise ValueError(
        "--scope must be one of: all, new, new-or-updated, newly-under-investigation"
    )


def record_notification(
    conn: duckdb.DuckDBPyConnection,
    batch_id: str,
    document_id: int,
    complaint_number: str,
    recipient_role: str,
    recipient_name: str,
    recipient_target: str,
    message_mode: str,
    detail_level: str,
    delivery_enabled: bool,
    nats_subject: str,
    payload: dict[str, Any],
    fingerprint: str,
) -> None:
    conn.execute(
        """
        INSERT INTO whatsapp_notifications (
            notification_id,
            batch_id,
            complaint_document_id,
            complaint_number,
            recipient_role,
            recipient_name,
            recipient_target,
            message_mode,
            detail_level,
            sent_at,
            delivery_enabled,
            nats_subject,
            job_payload,
            fingerprint
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(uuid.uuid4()),
            batch_id,
            document_id,
            complaint_number,
            recipient_role,
            recipient_name,
            recipient_target,
            message_mode,
            detail_level,
            datetime.now(timezone.utc).replace(tzinfo=None),
            delivery_enabled,
            nats_subject,
            orjson.dumps(payload).decode("utf-8"),
            fingerprint,
        ],
    )


def related_document_ids(
    main_document: dict[str, Any],
    all_documents: list[dict[str, Any]],
    field_names: dict[int, str],
) -> list[int]:
    ids: set[int] = set()
    direct = raw_custom_field(main_document, "Parent Case", field_names)
    if isinstance(direct, list):
        ids.update(int(item) for item in direct if item)

    main_id = int(main_document["id"])
    for document in all_documents:
        if int(document["id"]) == main_id:
            continue
        parent_value = raw_custom_field(document, "Parent Case", field_names)
        if isinstance(parent_value, list) and main_id in {
            int(item) for item in parent_value if item
        }:
            ids.add(int(document["id"]))
    return sorted(ids)


def filename_from_content_disposition(header: str | None) -> str | None:
    if not header:
        return None
    message = Message()
    message["content-disposition"] = header
    filename = message.get_filename()
    return clean_text(filename) if filename else None


def safe_filename(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._ -]+", "_", value).strip()
    return value.strip(" ._") or "document"


async def download_document(
    client: PaperlessClient,
    document: dict[str, Any],
    target_dir: Path,
) -> dict[str, str]:
    assert client.session is not None
    document_id = int(document["id"])
    target_dir.mkdir(parents=True, exist_ok=True)
    async with client.session.get(
        client.url(f"/api/documents/{document_id}/download/"),
        headers=client.headers,
    ) as response:
        response.raise_for_status()
        data = await response.read()
        content_type = response.headers.get(
            "content-type", "application/octet-stream"
        ).split(";", 1)[0]
        filename = filename_from_content_disposition(
            response.headers.get("content-disposition")
        )

    if not filename:
        original = clean_text(document.get("original_file_name"))
        title = clean_text(document.get("title")) or f"paperless-{document_id}"
        suffix = (
            Path(original).suffix or mimetypes.guess_extension(content_type) or ".bin"
        )
        filename = f"{safe_filename(title)}{suffix}"

    path = target_dir / safe_filename(filename)
    path.write_bytes(data)
    return {
        "path": str(path.resolve()),
        "filename": path.name,
        "mimetype": content_type or "application/octet-stream",
    }


def with_caption(document_payload: dict[str, str], caption: str) -> dict[str, str]:
    caption = caption.strip()
    if len(caption) > MAX_WHATSAPP_CAPTION_CHARS:
        caption = caption[: MAX_WHATSAPP_CAPTION_CHARS - 3].rstrip() + "..."
    return {**document_payload, "caption": caption}


async def download_documents_for_complaint(
    client: PaperlessClient,
    main_document: dict[str, Any],
    related_documents: list[dict[str, Any]],
    downloads_root: Path,
    include_complaint_file: bool,
    include_attachments: bool,
    main_caption: str,
    complaint_number: str,
) -> list[dict[str, str]]:
    complaint_dir = downloads_root / str(main_document["id"])
    documents = []
    if include_complaint_file:
        documents.append(
            with_caption(
                await download_document(client, main_document, complaint_dir),
                main_caption,
            )
        )
    if include_attachments:
        for index, document in enumerate(related_documents, start=1):
            title = clean_text(document.get("title")) or clean_text(document.get("original_file_name"))
            caption = f"📎 Attachment {index} of {complaint_number}"
            if title:
                caption += f"\n{title}"
            documents.append(
                with_caption(
                    await download_document(client, document, complaint_dir),
                    caption,
                )
            )
    return documents


def expected_document_count(
    include_complaint_file: bool,
    include_attachments: bool,
    related_documents: list[dict[str, Any]],
) -> int:
    count = 1 if include_complaint_file else 0
    if include_attachments:
        count += len(related_documents)
    return count


def document_fingerprint_item(document: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(document["id"]),
        "modified": clean_text(document.get("modified")),
        "title": clean_text(document.get("title")),
        "original_file_name": clean_text(document.get("original_file_name")),
    }


def complaint_package_fingerprint(
    main_document: dict[str, Any],
    related_documents: list[dict[str, Any]],
    include_complaint_file: bool,
    include_attachments: bool,
    detail_level: str,
) -> str:
    selected_documents: list[dict[str, Any]] = []
    if include_complaint_file:
        selected_documents.append(main_document)
    if include_attachments:
        selected_documents.extend(related_documents)
    payload = {
        "detail_level": detail_level,
        "include_complaint_file": include_complaint_file,
        "include_attachments": include_attachments,
        "documents": [
            document_fingerprint_item(document) for document in selected_documents
        ],
    }
    return hashlib.sha256(
        orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()


def files_profile_flags(profile: str) -> tuple[bool, bool]:
    if profile == "none":
        return False, False
    if profile == "pdf":
        return True, False
    if profile == "pdf-and-attachments":
        return True, True
    raise ValueError("--files must be one of: none, pdf, pdf-and-attachments")


def render_template(template: str, values: dict[str, Any]) -> str:
    safe_values = {key: "" if value is None else value for key, value in values.items()}
    lines = []
    for raw_line in template.splitlines():
        keys = re.findall(r"\{([A-Za-z0-9_]+)\}", raw_line)
        if keys and all(not clean_text(safe_values.get(key)) for key in keys):
            continue
        line = raw_line.format_map(safe_values).rstrip()
        if re.search(r":\s*$", line):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def full_message(
    document: dict[str, Any],
    fields: dict[str, Any],
    recipient_name: str,
    role_label: str,
    templates: dict[str, Any] | None = None,
) -> str:
    values = {
        "recipient_name": recipient_name,
        "role": role_label,
        "complaint_number": complaint_code(fields, document),
        "status": clean_text(fields.get("Status")),
        "tehsil": clean_text(fields.get("Tehsil")) or "Unassigned",
        "complainant_name": clean_text(fields.get("Complainant Name")),
        "complainant_mobile": clean_text(fields.get("Complainant Mobile Number")),
        "department": clean_text(fields.get("Department")),
        "category": clean_text(fields.get("Complaint Category")),
        "reported_entity_name": clean_text(fields.get("The Reported Entity Name")),
        "reported_entity_address": clean_text(
            fields.get("The Reported Entity Address")
        ),
    }
    template = (templates or {}).get("officer_full")
    if isinstance(template, str) and template.strip():
        return render_template(template, values).strip()

    lines = [
        "🔎 *Under Investigation Complaint*",
        f"👤 *To:* {values['recipient_name']} ({values['role'].upper()})",
        f"🆔 *Complaint:* `{values['complaint_number']}`",
        f"📌 *Status:* {values['status']}",
        f"📍 *Tehsil:* {values['tehsil']}",
    ]
    details = (
        ("🙍", "Complainant", values["complainant_name"]),
        ("📞", "Mobile", values["complainant_mobile"]),
        ("🏛️", "Department", values["department"]),
        ("🏷️", "Category", values["category"]),
        ("🏫", "Reported Entity", values["reported_entity_name"]),
        ("📫", "Address", values["reported_entity_address"]),
    )
    for icon, label, value in details:
        if value:
            lines.append(f"{icon} *{label}:* {value}")
    return "\n".join(lines)


def summary_message(
    complaints: list[tuple[dict[str, Any], dict[str, Any]]],
    recipient_name: str,
    role_label: str,
) -> str:
    counts: dict[str, int] = {}
    for _, fields in complaints:
        key = clean_text(fields.get("Tehsil")) or "Unassigned"
        counts[key] = counts.get(key, 0) + 1

    lines = [
        "Under Investigation Complaints Summary",
        f"To: {recipient_name} ({role_label})",
        f"Total: {len(complaints)}",
    ]
    for tehsil, count in sorted(counts.items()):
        lines.append(f"{tehsil}: {count}")
    lines.append("")
    for document, fields in complaints:
        lines.append(
            f"- {complaint_code(fields, document)} | {clean_text(fields.get('Tehsil')) or 'Unassigned'}"
        )
    return "\n".join(lines)


def tehsil_display_name(value: Any) -> str:
    text = clean_text(value)
    return text if text else "Unassigned"


def complaint_matches_tehsil(fields: dict[str, Any], tehsil_filter: str | None) -> bool:
    if not tehsil_filter:
        return True
    wanted = normalize_tehsil(tehsil_filter)
    actual = normalize_tehsil(fields.get("Tehsil"))
    if wanted in {"", "UNASSIGNED"}:
        return not actual
    return actual == wanted


def group_summary_message(
    complaints: list[tuple[dict[str, Any], dict[str, Any]]],
    title: str,
    templates: dict[str, Any] | None = None,
) -> str:
    grouped: dict[str, dict[str, list[str]]] = {}
    for document, fields in complaints:
        code = complaint_code(fields, document)
        source = complaint_source_label(code)
        tehsil = tehsil_display_name(fields.get("Tehsil"))
        grouped.setdefault(source, {}).setdefault(tehsil, []).append(code)

    lines = [
        f"📣 *{clean_text((templates or {}).get('group_summary_title')) or title}*",
        f"⚖️ Total under investigation: *{len(complaints)}*",
        "",
    ]

    source_order = ["CRM Portal", "PM Citizen Portal", "Other"]
    source_icons = {
        "CRM Portal": "📜",
        "PM Citizen Portal": "🏛️",
        "Other": "📂",
    }
    for source in source_order:
        tehsil_map = grouped.get(source)
        if not tehsil_map:
            continue
        source_total = sum(len(numbers) for numbers in tehsil_map.values())
        lines.append(f"{source_icons.get(source, '📂')} *{source}*")
        lines.append(f"Total cases: *{source_total}*")
        for tehsil in sorted(
            tehsil_map, key=lambda item: (item == "Unassigned", item.upper())
        ):
            complaint_numbers = sorted(tehsil_map[tehsil])
            lines.append(f"▫️ *{tehsil}* — {len(complaint_numbers)}")
            for number in complaint_numbers:
                lines.append(f"   • `{number}`")
        lines.append("")

    return "\n".join(lines).strip()


def recipient_target(number: str) -> str:
    return f"{number}@s.whatsapp.net"


def recipients_for_complaint(
    officers: list[OfficerRow],
    tehsil: str,
    hierarchy: dict[str, str],
    recipient_roles: frozenset[str] | None = None,
) -> list[dict[str, str]]:
    recipients: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add(role: str, name: str, number: str, detail_level: str) -> None:
        if recipient_roles is not None and role not in recipient_roles:
            return
        if detail_level == "off" or not number:
            return
        key = (role, number)
        if key in seen:
            return
        seen.add(key)
        recipients.append(
            {
                "role": role,
                "name": name,
                "target": recipient_target(number),
                "detail_level": detail_level,
            }
        )

    for row in officers:
        add("deo_mee", row.deo_name, row.deo_number, hierarchy.get("deo_mee", "off"))

    normalized_tehsil = normalize_tehsil(tehsil)
    if not normalized_tehsil:
        return recipients

    row = next((item for item in officers if item.tehsil == normalized_tehsil), None)
    if not row:
        return recipients

    add("ddeo", row.ddeo_name, row.ddeo_number, hierarchy.get("ddeo", "off"))
    add("aeo", row.aeo_name, row.aeo_number, hierarchy.get("aeo", "off"))
    return recipients


async def publish_jobs(nats_url: str, subject: str, jobs: list[dict[str, Any]]) -> None:
    nc = NATS()
    await nc.connect(servers=[nats_url])
    try:
        js = nc.jetstream()
        for job in jobs:
            await js.publish(subject, orjson.dumps(job))
    finally:
        await nc.drain()


def log_preview(jobs: list[dict[str, Any]], logger: logging.Logger) -> None:
    by_recipient: dict[tuple[str, str, str], dict[str, Any]] = {}
    for job in jobs:
        key = (
            str(job.get("type", "")),
            str(job.get("recipient_name") or job.get("target")),
            str(job.get("target")),
        )
        item = by_recipient.setdefault(
            key,
            {"count": 0, "docs": 0, "examples": []},
        )
        item["count"] += 1
        item["docs"] += int(
            job.get("expected_document_count", len(job.get("documents", []))) or 0
        )
        text_value = job.get("text")
        if text_value:
            text = str(text_value).splitlines()
            if text and len(item["examples"]) < 3:
                item["examples"].append(text[0])
        elif int(job.get("expected_document_count", 0) or 0) > 0 and len(item["examples"]) < 3:
            item["examples"].append("document caption")

    logger.info("Preview summary:")
    logger.info(
        "%-10s | %-24s | %-32s | %-4s | %-4s | %s",
        "type",
        "recipient",
        "target",
        "jobs",
        "docs",
        "examples",
    )
    for (target_type, name, target), item in sorted(by_recipient.items()):
        logger.info(
            "%-10s | %-24s | %-32s | %-4s | %-4s | %s",
            target_type,
            name[:24],
            target[:32],
            item["count"],
            item["docs"],
            "; ".join(item["examples"]),
        )


async def run_notify_async(settings: NotifySettings, project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    config = load_json(settings.config_path)
    delivery = config.get("delivery", {})
    message_config = config.get("message", {})
    group_summary_config = config.get("group_summary", {})
    templates = config.get("templates", {})
    hierarchy = config.get("hierarchy", {})
    officers_enabled = bool(settings.recipient_roles)
    delivery_enabled = settings.action == "send"
    subject = str(delivery.get("subject", "whatsapp.pending"))
    nats_url = str(delivery.get("nats_url", "nats://localhost:4222"))
    mode = settings.message_mode or str(message_config.get("mode", "per_complaint"))
    include_complaint_file, include_attachments = files_profile_flags(settings.files)
    scope = settings.scope
    min_minutes = int(
        config.get("resend_policy", {}).get("min_minutes_between_same_recipient", 0)
        or 0
    )
    officers_path = project_root / str(config.get("officers_csv", "officers-data.csv"))
    groups_path = project_root / str(
        group_summary_config.get("groups_csv", "whatsapp_groups.csv")
    )
    downloads_root = (
        project_root / str(config.get("downloads_dir", "notification_downloads"))
    ).resolve()

    officers = load_officers(officers_path) if officers_enabled else []
    if officers_enabled and not officers:
        raise RuntimeError(f"No officers loaded from {officers_path}")
    whatsapp_groups = (
        load_whatsapp_groups(groups_path) if settings.include_group_summary else []
    )

    conn = duckdb.connect(str(settings.duckdb_path))
    jobs: list[dict[str, Any]] = []
    notification_records: list[
        tuple[int, str, str, str, str, str, str, dict[str, Any], str]
    ] = []
    try:
        init_notification_db(conn)
        batch_id = record_batch_start(conn, settings, config)
        async with PaperlessClient(settings.paperless) as client:
            custom_fields = await client.paginated_results(
                "/api/custom_fields/?page_size=100"
            )
            field_names, option_labels = option_maps(custom_fields)
            all_documents = await client.paginated_results(
                "/api/documents/?page_size=100"
            )
            documents_by_id = {
                int(document["id"]): document for document in all_documents
            }
            complaints: list[tuple[dict[str, Any], dict[str, Any]]] = []
            observed_main_complaints: list[tuple[dict[str, Any], dict[str, Any]]] = []
            for document in all_documents:
                fields = decoded_custom_fields(document, field_names, option_labels)
                if normalize_label(fields.get("Document Role")) == normalize_label(
                    config.get("paperless", {}).get("document_role", "Main Complaint")
                ):
                    observed_main_complaints.append((document, fields))
                if complaint_matches(fields, config):
                    complaints.append((document, fields))
            if settings.tehsil:
                before_filter = len(complaints)
                complaints = [
                    (document, fields)
                    for document, fields in complaints
                    if complaint_matches_tehsil(fields, settings.tehsil)
                ]
                logger.info(
                    "Tehsil filter %r applied: %s/%s complaints retained.",
                    settings.tehsil,
                    len(complaints),
                    before_filter,
                )

            logger.info("Under-investigation complaints matched: %s", len(complaints))

            if not officers_enabled:
                logger.info("Officer notifications disabled for this run.")
            elif mode == "per_recipient":
                grouped: dict[
                    tuple[str, str, str, str],
                    list[tuple[dict[str, Any], dict[str, Any]]],
                ] = {}
                for document, fields in complaints:
                    for recipient in recipients_for_complaint(
                        officers,
                        clean_text(fields.get("Tehsil")),
                        hierarchy,
                        settings.recipient_roles,
                    ):
                        key = (
                            recipient["role"],
                            recipient["name"],
                            recipient["target"],
                            recipient["detail_level"],
                        )
                        grouped.setdefault(key, []).append((document, fields))
                for (role, name, target, detail_level), items in grouped.items():
                    text = summary_message(items, name, role)
                    payload = {
                        "job_id": str(uuid.uuid4()),
                        "target": target,
                        "type": "contact",
                        "recipient_name": name,
                        "attachment_text_mode": message_config.get(
                            "attachment_text_mode", "separate"
                        ),
                        "text": text,
                        "delay_ms": int(delivery.get("delay_ms", 1500) or 0),
                    }
                    jobs.append(payload)
                    for document, fields in items:
                        notification_records.append(
                            (
                                int(document["id"]),
                                complaint_code(fields, document),
                                role,
                                name,
                                target,
                                mode,
                                detail_level,
                                payload,
                                "",
                            )
                        )
            elif mode == "per_complaint":
                for document, fields in complaints:
                    related_ids = related_document_ids(
                        document, all_documents, field_names
                    )
                    related = [
                        documents_by_id[item]
                        for item in related_ids
                        if item in documents_by_id
                    ]
                    downloaded_documents: list[dict[str, str]] | None = None
                    expected_docs = expected_document_count(
                        include_complaint_file,
                        include_attachments,
                        related,
                    )
                    for recipient in recipients_for_complaint(
                        officers,
                        clean_text(fields.get("Tehsil")),
                        hierarchy,
                        settings.recipient_roles,
                    ):
                        if not should_send(
                            conn,
                            int(document["id"]),
                            recipient["role"],
                            recipient["target"],
                            min_minutes,
                        ):
                            logger.info(
                                "Skipping recently sent complaint=%s recipient=%s",
                                complaint_code(fields, document),
                                recipient["target"],
                            )
                            continue
                        detail_level = recipient["detail_level"]
                        if detail_level == "summary":
                            text = summary_message(
                                [(document, fields)],
                                recipient["name"],
                                recipient["role"],
                            )
                        else:
                            text = full_message(
                                document,
                                fields,
                                recipient["name"],
                                recipient["role"],
                                templates,
                            )
                        fingerprint = complaint_package_fingerprint(
                            document,
                            related,
                            include_complaint_file,
                            include_attachments,
                            detail_level,
                        )
                        if not should_send_for_scope(
                            conn,
                            int(document["id"]),
                            clean_text(fields.get("Status")),
                            recipient["role"],
                            recipient["target"],
                            min_minutes,
                            scope,
                            fingerprint,
                        ):
                            logger.info(
                                "Skipping by scope=%s complaint=%s recipient=%s",
                                scope,
                                complaint_code(fields, document),
                                recipient["target"],
                            )
                            continue
                        payload = {
                            "job_id": str(uuid.uuid4()),
                            "target": recipient["target"],
                            "type": "contact",
                            "recipient_name": recipient["name"],
                            "attachment_text_mode": message_config.get(
                                "attachment_text_mode", "separate"
                            ),
                            "text": text if expected_docs == 0 else None,
                            "documents": [],
                            "expected_document_count": expected_docs,
                            "delay_ms": int(delivery.get("delay_ms", 1500) or 0),
                        }
                        if delivery_enabled and expected_docs:
                            if downloaded_documents is None:
                                downloaded_documents = (
                                    await download_documents_for_complaint(
                                        client,
                                        document,
                                        related,
                                        downloads_root,
                                        include_complaint_file,
                                        include_attachments,
                                        text,
                                        complaint_code(fields, document),
                                    )
                                )
                            payload["documents"] = downloaded_documents
                        jobs.append(payload)
                        notification_records.append(
                            (
                                int(document["id"]),
                                complaint_code(fields, document),
                                recipient["role"],
                                recipient["name"],
                                recipient["target"],
                                mode,
                                detail_level,
                                payload,
                                fingerprint,
                            )
                        )
            else:
                raise ValueError(
                    "message.mode must be either per_complaint or per_recipient"
                )

            if bool(group_summary_config.get("enabled", True)) and whatsapp_groups:
                group_title = str(
                    group_summary_config.get(
                        "title",
                        "Under Investigation Complaints Summary",
                    )
                )
                text = group_summary_message(complaints, group_title, templates)
                for group in whatsapp_groups:
                    recently_sent = [
                        not should_send(
                            conn,
                            int(document["id"]),
                            "whatsapp_group",
                            group.target,
                            min_minutes,
                        )
                        for document, _ in complaints
                    ]
                    if complaints and all(recently_sent):
                        logger.info(
                            "Skipping recently sent group summary: %s", group.target
                        )
                        continue
                    payload = {
                        "job_id": str(uuid.uuid4()),
                        "target": group.target,
                        "type": group.type,
                        "recipient_name": group.name,
                        "attachment_text_mode": "separate",
                        "text": text,
                        "delay_ms": int(delivery.get("delay_ms", 1500) or 0),
                    }
                    jobs.append(payload)
                    for document, fields in complaints:
                        notification_records.append(
                            (
                                int(document["id"]),
                                complaint_code(fields, document),
                                "whatsapp_group",
                                group.name,
                                group.target,
                                "group_summary",
                                "summary",
                                payload,
                                "",
                            )
                        )

        logger.info("WhatsApp jobs prepared: %s", len(jobs))
        if delivery_enabled and jobs:
            await publish_jobs(nats_url, subject, jobs)
            logger.info("Published WhatsApp jobs to NATS subject %s.", subject)
            for record in notification_records:
                record_notification(
                    conn, batch_id, *record[:-2], True, subject, record[-2], record[-1]
                )
            record_batch_complete(conn, batch_id, len(complaints), len(jobs), len(jobs))
        elif delivery_enabled:
            logger.info("Send mode selected, but no jobs matched the current filters.")
            record_batch_complete(conn, batch_id, len(complaints), 0, 0)
        else:
            logger.info(
                "Preview only; no jobs were published and no delivery rows were recorded."
            )
            log_preview(jobs, logger)
            record_batch_complete(conn, batch_id, len(complaints), len(jobs), 0)
        record_observed_statuses(conn, observed_main_complaints)
    finally:
        conn.close()


def run_notify(settings: NotifySettings, project_root: Path) -> None:
    asyncio.run(run_notify_async(settings, project_root))
