
from __future__ import annotations

import csv
import json
import shutil
import uuid
import zipfile
from pathlib import Path
from typing import Any

from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.inbound.media_types import safe_filename, safe_slug
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundExportItem,
    WhatsAppInboundExportRun,
    WhatsAppInboundMessage,
)

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
