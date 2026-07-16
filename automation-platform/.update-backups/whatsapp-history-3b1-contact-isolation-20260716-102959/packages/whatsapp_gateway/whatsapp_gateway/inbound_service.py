from __future__ import annotations

import csv
import json
import shutil
import uuid
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import func, or_
from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.inbound_media import (
    SUPPORTED_CATEGORIES,
    classify_attachment_metadata,
    safe_filename,
    safe_slug,
)
from whatsapp_gateway.models import (
    WhatsAppDirectoryContact,
    WhatsAppInboundAttachment,
    WhatsAppInboundExportItem,
    WhatsAppInboundExportRun,
    WhatsAppInboundMessage,
    WhatsAppIdentityAlias,
)


def normalize_utc_naive(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def normalize_media_types(values: Iterable[str]) -> list[str]:
    normalized = sorted({str(value).strip().lower() for value in values if value})
    invalid = [value for value in normalized if value not in SUPPORTED_CATEGORIES]
    if invalid:
        raise ValueError(f"Unsupported inbound file type selection: {', '.join(invalid)}")
    if not normalized:
        raise ValueError("Select at least one file type")
    return normalized


def require_contact(session: Session, contact_id: uuid.UUID) -> WhatsAppDirectoryContact:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or not contact.active:
        raise ValueError("WhatsApp directory contact was not found or is inactive")
    return contact




def contact_identity_jids(
    session: Session, contact: WhatsAppDirectoryContact
) -> set[str]:
    values = {
        value
        for value in [contact.phone_jid, contact.primary_lid_jid]
        if value
    }
    values.update(
        session.exec(
            select(WhatsAppIdentityAlias.lid_jid).where(
                WhatsAppIdentityAlias.contact_id == contact.id,
                WhatsAppIdentityAlias.account_id == contact.account_id,
            )
        ).all()
    )
    return {str(value) for value in values if value}


def _contact_message_filter(
    session: Session, contact: WhatsAppDirectoryContact
):
    jids = contact_identity_jids(session, contact)
    clauses = [
        WhatsAppInboundMessage.account_id == contact.account_id,
        WhatsAppInboundMessage.directory_contact_id == contact.id,
    ]
    if jids:
        clauses.extend(
            [
                WhatsAppInboundMessage.sender_jid.in_(jids),
                WhatsAppInboundMessage.participant_jid.in_(jids),
                WhatsAppInboundMessage.remote_jid.in_(jids),
            ]
        )
    return or_(*clauses)


def attachment_matches_category(
    attachment: WhatsAppInboundAttachment,
    categories: set[str],
) -> tuple[bool, str | None]:
    category = attachment.media_category or classify_attachment_metadata(
        media_kind=attachment.media_kind,
        mime_type=attachment.detected_mime_type or attachment.mime_type,
        original_filename=attachment.original_filename,
    )
    return category in categories, category


def find_matching_attachments(
    session: Session,
    *,
    contact_id: uuid.UUID,
    date_from: datetime | None,
    date_to: datetime | None,
    chat_scope: str,
    media_types: Iterable[str],
) -> list[tuple[WhatsAppInboundAttachment, WhatsAppInboundMessage, str]]:
    if chat_scope not in {"direct", "direct_and_groups"}:
        raise ValueError("Invalid chat scope")
    categories = set(normalize_media_types(media_types))
    contact = require_contact(session, contact_id)
    filters: list[Any] = [
        _contact_message_filter(session, contact),
        WhatsAppInboundMessage.from_me.is_(False),
    ]
    if chat_scope == "direct":
        filters.append(WhatsAppInboundMessage.chat_scope == "direct")
    normalized_from = normalize_utc_naive(date_from)
    normalized_to = normalize_utc_naive(date_to)
    if normalized_from and normalized_to and normalized_from > normalized_to:
        raise ValueError("The start date must be before or equal to the end date")
    if normalized_from:
        filters.append(WhatsAppInboundMessage.message_timestamp >= normalized_from)
    if normalized_to:
        filters.append(WhatsAppInboundMessage.message_timestamp <= normalized_to)

    rows = session.exec(
        select(WhatsAppInboundAttachment, WhatsAppInboundMessage)
        .join(
            WhatsAppInboundMessage,
            WhatsAppInboundMessage.id == WhatsAppInboundAttachment.message_id,
        )
        .where(*filters)
        .order_by(
            WhatsAppInboundMessage.message_timestamp,
            WhatsAppInboundMessage.message_id,
        )
    ).all()
    matches: list[tuple[WhatsAppInboundAttachment, WhatsAppInboundMessage, str]] = []
    reconciled = False
    for attachment, message in rows:
        if message.directory_contact_id is None:
            message.directory_contact_id = contact.id
            message.last_ingested_at = utcnow()
            session.add(message)
            reconciled = True
        is_match, category = attachment_matches_category(attachment, categories)
        if is_match and category:
            matches.append((attachment, message, category))
    if reconciled:
        session.commit()
    return matches


def contact_coverage(
    session: Session,
    contact_id: uuid.UUID,
) -> tuple[datetime | None, datetime | None]:
    contact = require_contact(session, contact_id)
    return session.exec(
        select(
            func.min(WhatsAppInboundMessage.message_timestamp),
            func.max(WhatsAppInboundMessage.message_timestamp),
        ).where(
            _contact_message_filter(session, contact),
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()


def build_preview(
    session: Session,
    *,
    contact_id: uuid.UUID,
    date_from: datetime | None,
    date_to: datetime | None,
    chat_scope: str,
    media_types: Iterable[str],
    item_limit: int = 100,
) -> dict[str, Any]:
    contact = require_contact(session, contact_id)
    normalized_types = normalize_media_types(media_types)
    matches = find_matching_attachments(
        session,
        contact_id=contact.id,
        date_from=date_from,
        date_to=date_to,
        chat_scope=chat_scope,
        media_types=normalized_types,
    )
    earliest, latest = contact_coverage(session, contact.id)
    category_counts = Counter(category for _, _, category in matches)
    status_counts = Counter(attachment.download_status for attachment, _, _ in matches)
    declared_bytes = sum(int(attachment.declared_size or 0) for attachment, _, _ in matches)
    items = []
    for attachment, message, category in matches[:item_limit]:
        items.append(
            {
                "attachment_id": str(attachment.id),
                "message_id": message.message_id,
                "timestamp": message.message_timestamp,
                "category": category,
                "filename": attachment.original_filename
                or f"{category}-{message.message_id}",
                "mime_type": attachment.detected_mime_type or attachment.mime_type,
                "size_bytes": attachment.actual_size or attachment.declared_size,
                "chat_scope": message.chat_scope,
                "caption": attachment.caption or message.text_content,
                "download_status": attachment.download_status,
            }
        )
    return {
        "contact": {
            "id": str(contact.id),
            "display_name": contact.display_name,
            "phone_jid": contact.phone_jid,
            "primary_lid_jid": contact.primary_lid_jid,
        },
        "filters": {
            "date_from": normalize_utc_naive(date_from),
            "date_to": normalize_utc_naive(date_to),
            "chat_scope": chat_scope,
            "media_types": normalized_types,
        },
        "files_found": len(matches),
        "declared_bytes": declared_bytes,
        "category_counts": dict(category_counts),
        "download_status_counts": dict(status_counts),
        "coverage": {
            "earliest_message_at": earliest,
            "latest_message_at": latest,
            "historical_complete": False,
            "note": "Coverage is complete only for messages captured by this worker. Older WhatsApp history may be partial.",
        },
        "items": items,
        "items_truncated": len(matches) > item_limit,
    }


def create_export_run(
    session: Session,
    *,
    job_id: uuid.UUID,
    contact_id: uuid.UUID,
    date_from: datetime | None,
    date_to: datetime | None,
    chat_scope: str,
    media_types: Iterable[str],
) -> WhatsAppInboundExportRun:
    contact = require_contact(session, contact_id)
    normalized_types = normalize_media_types(media_types)
    matches = find_matching_attachments(
        session,
        contact_id=contact.id,
        date_from=date_from,
        date_to=date_to,
        chat_scope=chat_scope,
        media_types=normalized_types,
    )
    if not matches:
        raise ValueError("No matching inbound files were found for this contact and filter")
    earliest, latest = contact_coverage(session, contact.id)
    run = WhatsAppInboundExportRun(
        job_id=job_id,
        account_id=contact.account_id,
        contact_id=contact.id,
        contact_name=contact.display_name or contact.phone_jid or contact.canonical_key,
        date_from=normalize_utc_naive(date_from),
        date_to=normalize_utc_naive(date_to),
        chat_scope=chat_scope,
        media_types=normalized_types,
        files_matched=len(matches),
        coverage_earliest_at=earliest,
        coverage_latest_at=latest,
    )
    session.add(run)
    session.flush()
    for attachment, _message, category in matches:
        session.add(
            WhatsAppInboundExportItem(
                export_run_id=run.id,
                attachment_id=attachment.id,
                media_category=category,
            )
        )
    session.commit()
    session.refresh(run)
    return run


def serialize_run(session: Session, run: WhatsAppInboundExportRun) -> dict[str, Any]:
    counts = dict(
        session.exec(
            select(
                WhatsAppInboundExportItem.status,
                func.count(WhatsAppInboundExportItem.id),
            )
            .where(WhatsAppInboundExportItem.export_run_id == run.id)
            .group_by(WhatsAppInboundExportItem.status)
        ).all()
    )
    return {
        "id": str(run.id),
        "job_id": str(run.job_id),
        "account_id": str(run.account_id),
        "contact_id": str(run.contact_id),
        "contact_name": run.contact_name,
        "date_from": run.date_from,
        "date_to": run.date_to,
        "chat_scope": run.chat_scope,
        "media_types": run.media_types,
        "status": run.status,
        "files_matched": run.files_matched,
        "files_downloaded": run.files_downloaded,
        "files_reused": run.files_reused,
        "files_unavailable": run.files_unavailable,
        "total_bytes": run.total_bytes,
        "item_status_counts": counts,
        "coverage_earliest_at": run.coverage_earliest_at,
        "coverage_latest_at": run.coverage_latest_at,
        "error": run.error,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "updated_at": run.updated_at,
    }


def _export_name(
    message: WhatsAppInboundMessage,
    attachment: WhatsAppInboundAttachment,
    category: str,
) -> str:
    extension = attachment.safe_extension or Path(
        attachment.original_filename or ""
    ).suffix.lower()
    base = safe_filename(
        attachment.original_filename,
        fallback=f"{category}-{message.message_id}{extension}",
    )
    if extension and not base.lower().endswith(extension.lower()):
        base = f"{Path(base).stem}{extension}"
    timestamp = message.message_timestamp.strftime("%Y%m%d-%H%M%S")
    return safe_filename(f"{timestamp}_{message.message_id[:12]}_{base}")


def package_export(
    session: Session,
    *,
    run: WhatsAppInboundExportRun,
    output_root: Path,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    contact_folder = safe_slug(run.contact_name)
    package_root = output_root / contact_folder
    if package_root.exists():
        shutil.rmtree(package_root)
    package_root.mkdir(parents=True, exist_ok=True)
    for folder in ("images", "pdfs", "spreadsheets"):
        (package_root / folder).mkdir(parents=True, exist_ok=True)

    rows = session.exec(
        select(
            WhatsAppInboundExportItem,
            WhatsAppInboundAttachment,
            WhatsAppInboundMessage,
        )
        .join(
            WhatsAppInboundAttachment,
            WhatsAppInboundAttachment.id == WhatsAppInboundExportItem.attachment_id,
        )
        .join(
            WhatsAppInboundMessage,
            WhatsAppInboundMessage.id == WhatsAppInboundAttachment.message_id,
        )
        .where(WhatsAppInboundExportItem.export_run_id == run.id)
        .order_by(WhatsAppInboundMessage.message_timestamp)
    ).all()

    manifest: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    checksum_paths: dict[str, tuple[str, uuid.UUID]] = {}
    folder_for = {"image": "images", "pdf": "pdfs", "spreadsheet": "spreadsheets"}

    for item, attachment, message in rows:
        row = {
            "export_item_id": str(item.id),
            "attachment_id": str(attachment.id),
            "message_id": message.message_id,
            "message_timestamp": message.message_timestamp.isoformat(),
            "remote_jid": message.remote_jid,
            "sender_jid": message.sender_jid,
            "chat_scope": message.chat_scope,
            "category": item.media_category,
            "original_filename": attachment.original_filename or "",
            "declared_mime_type": attachment.mime_type or "",
            "detected_mime_type": attachment.detected_mime_type or "",
            "declared_size": attachment.declared_size or "",
            "actual_size": attachment.actual_size or "",
            "sha256": attachment.actual_sha256 or attachment.media_sha256 or "",
            "caption": attachment.caption or message.text_content or "",
            "download_status": attachment.download_status,
            "export_status": item.status,
            "export_path": "",
            "error": item.error or attachment.last_error or "",
        }
        stored = Path(attachment.stored_path) if attachment.stored_path else None
        if item.status in {"ready", "reused"} and stored and stored.is_file():
            checksum = attachment.actual_sha256 or ""
            if checksum and checksum in checksum_paths:
                relative, original_item_id = checksum_paths[checksum]
                item.status = "duplicate_reused"
                item.duplicate_of_item_id = original_item_id
                item.output_path = relative
                item.output_name = Path(relative).name
                row["export_status"] = item.status
                row["export_path"] = relative
            else:
                name = _export_name(message, attachment, item.media_category)
                relative_path = Path(folder_for[item.media_category]) / name
                destination = package_root / relative_path
                suffix = 1
                while destination.exists():
                    destination = destination.with_name(
                        f"{destination.stem}-{suffix}{destination.suffix}"
                    )
                    relative_path = Path(folder_for[item.media_category]) / destination.name
                    suffix += 1
                shutil.copy2(stored, destination)
                relative = relative_path.as_posix()
                if checksum:
                    checksum_paths[checksum] = (relative, item.id)
                item.output_path = relative
                item.output_name = destination.name
                row["export_path"] = relative
            item.updated_at = utcnow()
            session.add(item)
        else:
            unavailable.append(row)
        manifest.append(row)

    session.commit()
    manifest_csv = package_root / "manifest.csv"
    manifest_json = package_root / "manifest.json"
    unavailable_csv = package_root / "unavailable-files.csv"
    summary_json = package_root / "export-summary.json"
    fieldnames = list(manifest[0].keys()) if manifest else ["export_item_id"]
    with manifest_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(manifest)
    manifest_json.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    with unavailable_csv.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unavailable)
    summary = {
        "export_run_id": str(run.id),
        "contact_id": str(run.contact_id),
        "contact_name": run.contact_name,
        "filters": {
            "date_from": run.date_from.isoformat() if run.date_from else None,
            "date_to": run.date_to.isoformat() if run.date_to else None,
            "chat_scope": run.chat_scope,
            "media_types": run.media_types,
        },
        "coverage": {
            "earliest_message_at": run.coverage_earliest_at.isoformat()
            if run.coverage_earliest_at
            else None,
            "latest_message_at": run.coverage_latest_at.isoformat()
            if run.coverage_latest_at
            else None,
            "historical_complete": False,
        },
        "counts": {
            "matched": run.files_matched,
            "downloaded": run.files_downloaded,
            "reused": run.files_reused,
            "unavailable": run.files_unavailable,
            "unique_files_in_package": len(checksum_paths),
        },
        "total_bytes": run.total_bytes,
        "generated_at": utcnow().isoformat(),
    }
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    zip_path = output_root / f"whatsapp-files-{contact_folder}-{run.id}.zip"
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(package_root.rglob("*")):
            if file_path.is_file():
                archive.write(file_path, arcname=f"{contact_folder}/{file_path.relative_to(package_root)}")
    return {
        "zip_path": zip_path,
        "manifest_csv": manifest_csv,
        "manifest_json": manifest_json,
        "unavailable_csv": unavailable_csv,
        "summary_json": summary_json,
        "package_root": package_root,
    }
