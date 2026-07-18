from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.object_storage import ObjectStorageError, S3ObjectStorage, StoredObjectResult
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment, WhatsAppInboundBatchItem, WhatsAppInboundMessage,
    WhatsAppInboundStoredObject,
)


def _record_event(session: Session, **values: Any) -> None:
    # Local import keeps storage independent from the batch orchestration module.
    from whatsapp_gateway.inbound.batches import record_batch_event

    record_batch_event(session, **values)


def _stored_object_row(
    session: Session, *, result: StoredObjectResult, metadata: dict[str, Any],
) -> tuple[WhatsAppInboundStoredObject, bool]:
    existing = session.exec(
        select(WhatsAppInboundStoredObject).where(
            WhatsAppInboundStoredObject.backend == "s3",
            WhatsAppInboundStoredObject.bucket == result.bucket,
            WhatsAppInboundStoredObject.object_key == result.object_key,
        )
    ).first()
    now = utcnow()
    if existing is not None:
        existing.sha256 = result.sha256
        existing.size_bytes = result.size_bytes
        existing.content_type = result.content_type
        existing.etag = result.etag
        existing.version_id = result.version_id
        existing.metadata_json = metadata
        existing.status = "available"
        existing.verified_at = now
        session.add(existing)
        session.flush()
        return existing, True
    stored = WhatsAppInboundStoredObject(
        backend="s3", bucket=result.bucket, object_key=result.object_key,
        sha256=result.sha256, size_bytes=result.size_bytes,
        content_type=result.content_type, etag=result.etag,
        version_id=result.version_id, metadata_json=metadata,
        status="available", created_at=now, verified_at=now,
    )
    session.add(stored)
    session.flush()
    return stored, result.reused


def store_attachment_object(
    session: Session, *, attachment: WhatsAppInboundAttachment,
    message: WhatsAppInboundMessage, source_path: Path, settings: Settings,
    storage: S3ObjectStorage | None = None,
) -> dict[str, Any]:
    now = utcnow()
    batch_items = session.exec(
        select(WhatsAppInboundBatchItem).where(
            WhatsAppInboundBatchItem.attachment_id == attachment.id
        )
    ).all()
    if not settings.object_storage_enabled:
        attachment.storage_status = "local_only"
        attachment.storage_error = None
        for item in batch_items:
            item.status = "storage_pending"
            item.sha256 = attachment.actual_sha256
            item.size_bytes = attachment.actual_size
            item.mime_type = attachment.detected_mime_type or attachment.mime_type
            item.updated_at = now
            session.add(item)
        session.add(attachment)
        for item in batch_items:
            _record_event(
                session, batch_id=item.batch_id, batch_item_id=item.id, level="warning",
                event_type="storage_pending",
                message=f"Archived locally; object storage is disabled: {item.original_filename or attachment.message_key}",
            )
        return {"enabled": False, "stored": False, "reused": False}

    if not attachment.actual_sha256 or attachment.actual_size is None:
        raise ObjectStorageError("Attachment checksum and size are required before object upload")
    adapter = storage or S3ObjectStorage(settings)
    from whatsapp_gateway.inbound.batches import content_object_key

    object_key = content_object_key(attachment.actual_sha256)
    for item in batch_items:
        item.status = "uploading"
        item.sha256 = attachment.actual_sha256
        item.size_bytes = attachment.actual_size
        item.mime_type = attachment.detected_mime_type or attachment.mime_type
        item.object_key = object_key
        item.error = None
        item.updated_at = now
        session.add(item)
    attachment.storage_status = "uploading"
    attachment.storage_error = None
    session.add(attachment)
    session.flush()
    for item in batch_items:
        _record_event(
            session, batch_id=item.batch_id, batch_item_id=item.id,
            event_type="object_upload_started",
            message=f"Uploading to object storage: {item.original_filename or attachment.message_key}",
            details={"object_key": object_key, "size_bytes": attachment.actual_size},
        )

    metadata = {
        "attachment_id": str(attachment.id), "message_id": str(message.id),
        "whatsapp_message_id": message.message_id, "remote_jid": message.remote_jid,
        "original_filename": attachment.original_filename or "",
    }
    try:
        result = adapter.put_file_if_absent(
            bucket=settings.object_storage_raw_bucket, object_key=object_key,
            source_path=source_path, sha256=attachment.actual_sha256,
            size_bytes=attachment.actual_size,
            content_type=attachment.detected_mime_type or attachment.mime_type,
            metadata=metadata,
        )
        stored, reused = _stored_object_row(session, result=result, metadata=metadata)
        attachment.stored_object_id = stored.id
        attachment.storage_status = "stored"
        attachment.storage_error = None
        attachment.storage_uploaded_at = now
        session.add(attachment)
        for item in batch_items:
            item.stored_object_id = stored.id
            item.status = "already_stored" if reused else "stored"
            item.object_key = object_key
            item.error = None
            item.stored_at = now
            item.updated_at = now
            session.add(item)
        session.flush()
        for item in batch_items:
            _record_event(
                session, batch_id=item.batch_id, batch_item_id=item.id,
                event_type="object_reused" if reused else "object_stored",
                message=(
                    f"Reused existing object: {item.original_filename or attachment.message_key}"
                    if reused else f"Stored object: {item.original_filename or attachment.message_key}"
                ),
                details={"bucket": result.bucket, "object_key": result.object_key, "sha256": result.sha256},
            )
        return {
            "enabled": True, "stored": True, "reused": reused,
            "stored_object_id": str(stored.id), "bucket": result.bucket,
            "object_key": result.object_key,
        }
    except Exception as exc:
        attachment.storage_status = "failed"
        attachment.storage_error = str(exc)
        session.add(attachment)
        for item in batch_items:
            item.status = "failed"
            item.error = str(exc)
            item.updated_at = now
            session.add(item)
            _record_event(
                session, batch_id=item.batch_id, batch_item_id=item.id, level="error",
                event_type="object_storage_failed",
                message=f"Object storage failed for {item.original_filename or attachment.message_key}: {exc}",
            )
        raise


__all__ = ["store_attachment_object"]
