from __future__ import annotations

import json
import secrets
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import func
from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.object_storage import ObjectStorageError, S3ObjectStorage, StoredObjectResult
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchItem,
    WhatsAppInboundHistoryRequest,
    WhatsAppInboundMessage,
    WhatsAppInboundStoredObject,
)

BATCH_TERMINAL_STATUSES = {"completed", "completed_with_errors", "failed", "cancelled"}
ITEM_STORED_STATUSES = {"stored", "already_stored"}
ITEM_FAILED_STATUSES = {"failed", "unsupported"}


def new_batch_code(now: datetime | None = None, *, prefix: str = "WAB") -> str:
    stamp = now or utcnow()
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    stamp = stamp.astimezone(timezone.utc)
    return f"{prefix}-{stamp:%Y%m%d-%H%M%S}-{secrets.token_hex(3).upper()}"


def content_object_key(sha256: str) -> str:
    normalized = sha256.lower().strip()
    if len(normalized) != 64:
        raise ValueError("A 64-character SHA-256 value is required")
    return f"objects/sha256/{normalized[:2]}/{normalized[2:4]}/{normalized}"


def create_history_batch(
    session: Session,
    *,
    account_id: uuid.UUID,
    contact_id: uuid.UUID,
    worker_key: str,
    provider: str,
    requested_count: int,
    remote_jid: str | None,
    anchor_message_id: str | None,
    anchor_timestamp: datetime | None,
    settings: Settings,
) -> WhatsAppInboundBatch:
    now = utcnow()
    batch = WhatsAppInboundBatch(
        batch_code=new_batch_code(now),
        account_id=account_id,
        contact_id=contact_id,
        worker_key=worker_key,
        provider=provider,
        requested_count=requested_count,
        remote_jid=remote_jid,
        anchor_message_id=anchor_message_id,
        anchor_timestamp=anchor_timestamp,
        status="created",
        storage_backend=(settings.object_storage_provider if settings.object_storage_enabled else "local"),
        raw_bucket=(settings.object_storage_raw_bucket if settings.object_storage_enabled else None),
        manifest_bucket=(settings.object_storage_manifest_bucket if settings.object_storage_enabled else None),
        created_at=now,
        updated_at=now,
    )
    session.add(batch)
    session.flush()
    return batch


def serialize_batch(batch: WhatsAppInboundBatch) -> dict[str, Any]:
    return {
        "id": str(batch.id),
        "batch_code": batch.batch_code,
        "account_id": str(batch.account_id),
        "contact_id": str(batch.contact_id),
        "worker_key": batch.worker_key,
        "provider": batch.provider,
        "requested_count": batch.requested_count,
        "remote_jid": batch.remote_jid,
        "anchor_message_id": batch.anchor_message_id,
        "anchor_timestamp": batch.anchor_timestamp,
        "status": batch.status,
        "messages_discovered": batch.messages_discovered,
        "files_discovered": batch.files_discovered,
        "files_stored": batch.files_stored,
        "files_reused": batch.files_reused,
        "files_failed": batch.files_failed,
        "total_bytes": batch.total_bytes,
        "storage_backend": batch.storage_backend,
        "raw_bucket": batch.raw_bucket,
        "manifest_bucket": batch.manifest_bucket,
        "manifest_object_key": batch.manifest_object_key,
        "error": batch.error,
        "created_at": batch.created_at,
        "started_at": batch.started_at,
        "finished_at": batch.finished_at,
        "updated_at": batch.updated_at,
    }


def serialize_batch_item(item: WhatsAppInboundBatchItem) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "batch_id": str(item.batch_id),
        "attachment_id": str(item.attachment_id),
        "message_id": str(item.message_id),
        "stored_object_id": str(item.stored_object_id) if item.stored_object_id else None,
        "status": item.status,
        "original_filename": item.original_filename,
        "message_timestamp": item.message_timestamp,
        "mime_type": item.mime_type,
        "sha256": item.sha256,
        "size_bytes": item.size_bytes,
        "object_key": item.object_key,
        "error": item.error,
        "created_at": item.created_at,
        "stored_at": item.stored_at,
        "updated_at": item.updated_at,
    }


def register_batch_item(
    session: Session,
    *,
    batch_id: uuid.UUID | None,
    attachment_id: uuid.UUID,
    message_id: uuid.UUID,
) -> WhatsAppInboundBatchItem | None:
    if batch_id is None:
        return None
    batch = session.get(WhatsAppInboundBatch, batch_id)
    if batch is None:
        return None
    existing = session.exec(
        select(WhatsAppInboundBatchItem).where(
            WhatsAppInboundBatchItem.batch_id == batch_id,
            WhatsAppInboundBatchItem.attachment_id == attachment_id,
        )
    ).first()
    if existing is not None:
        return existing
    attachment = session.get(WhatsAppInboundAttachment, attachment_id)
    message = session.get(WhatsAppInboundMessage, message_id)
    if attachment is None or message is None:
        return None
    now = utcnow()
    item = WhatsAppInboundBatchItem(
        batch_id=batch_id,
        attachment_id=attachment_id,
        message_id=message_id,
        stored_object_id=attachment.stored_object_id,
        status=("already_stored" if attachment.stored_object_id else "discovered"),
        original_filename=attachment.original_filename,
        message_timestamp=message.message_timestamp,
        mime_type=attachment.mime_type,
        sha256=attachment.actual_sha256,
        size_bytes=attachment.actual_size,
        created_at=now,
        stored_at=(attachment.storage_uploaded_at if attachment.stored_object_id else None),
        updated_at=now,
    )
    session.add(item)
    batch.files_discovered += 1
    if batch.started_at is None:
        batch.started_at = now
    if batch.status == "created":
        batch.status = "fetching"
    batch.updated_at = now
    session.add(batch)
    session.flush()
    return item


def _stored_object_row(
    session: Session,
    *,
    result: StoredObjectResult,
    metadata: dict[str, Any],
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
        backend="s3",
        bucket=result.bucket,
        object_key=result.object_key,
        sha256=result.sha256,
        size_bytes=result.size_bytes,
        content_type=result.content_type,
        etag=result.etag,
        version_id=result.version_id,
        metadata_json=metadata,
        status="available",
        created_at=now,
        verified_at=now,
    )
    session.add(stored)
    session.flush()
    return stored, result.reused


def store_attachment_object(
    session: Session,
    *,
    attachment: WhatsAppInboundAttachment,
    message: WhatsAppInboundMessage,
    source_path: Path,
    settings: Settings,
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
        return {"enabled": False, "stored": False, "reused": False}

    if not attachment.actual_sha256 or attachment.actual_size is None:
        raise ObjectStorageError("Attachment checksum and size are required before object upload")
    adapter = storage or S3ObjectStorage(settings)
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

    metadata = {
        "attachment_id": str(attachment.id),
        "message_id": str(message.id),
        "whatsapp_message_id": message.message_id,
        "remote_jid": message.remote_jid,
        "original_filename": attachment.original_filename or "",
    }
    try:
        result = adapter.put_file_if_absent(
            bucket=settings.object_storage_raw_bucket,
            object_key=object_key,
            source_path=source_path,
            sha256=attachment.actual_sha256,
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
        return {
            "enabled": True,
            "stored": True,
            "reused": reused,
            "stored_object_id": str(stored.id),
            "bucket": result.bucket,
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
        raise


def _batch_items(session: Session, batch_id: uuid.UUID) -> list[WhatsAppInboundBatchItem]:
    return list(
        session.exec(
            select(WhatsAppInboundBatchItem)
            .where(WhatsAppInboundBatchItem.batch_id == batch_id)
            .order_by(WhatsAppInboundBatchItem.created_at)
        ).all()
    )


def reconcile_batch(
    session: Session,
    *,
    batch_id: uuid.UUID | None,
    settings: Settings | None = None,
) -> WhatsAppInboundBatch | None:
    if batch_id is None:
        return None
    batch = session.get(WhatsAppInboundBatch, batch_id)
    if batch is None:
        return None
    request = session.exec(
        select(WhatsAppInboundHistoryRequest).where(
            WhatsAppInboundHistoryRequest.batch_id == batch.id
        )
    ).first()
    items = _batch_items(session, batch.id)
    now = utcnow()
    batch.files_discovered = len(items)
    batch.files_stored = sum(item.status == "stored" for item in items)
    batch.files_reused = sum(item.status == "already_stored" for item in items)
    batch.files_failed = sum(item.status in ITEM_FAILED_STATUSES for item in items)
    batch.total_bytes = sum(int(item.size_bytes or 0) for item in items if item.status in ITEM_STORED_STATUSES)
    if request is not None:
        batch.messages_discovered = max(batch.messages_discovered, request.messages_received)
        if request.status in {"requested", "accepted", "syncing"}:
            batch.status = "storing" if items else "fetching"
            if batch.started_at is None:
                batch.started_at = request.accepted_at or request.requested_at
        elif request.status in {"succeeded", "no_results"}:
            if any(item.status in {"discovered", "downloading", "hashing", "uploading"} for item in items):
                batch.status = "storing"
            elif batch.files_failed:
                batch.status = "completed_with_errors"
                batch.finished_at = batch.finished_at or now
            else:
                batch.status = "completed"
                batch.finished_at = batch.finished_at or now
        elif request.status in {"failed", "timed_out"}:
            batch.status = "completed_with_errors" if (batch.files_stored or batch.files_reused) else "failed"
            batch.error = request.error
            batch.finished_at = batch.finished_at or now
    batch.updated_at = now
    session.add(batch)
    session.flush()
    if settings and batch.status in BATCH_TERMINAL_STATUSES and not batch.manifest_object_key:
        write_batch_manifest(session, batch=batch, items=items, settings=settings)
    return batch


def write_batch_manifest(
    session: Session,
    *,
    batch: WhatsAppInboundBatch,
    items: list[WhatsAppInboundBatchItem] | None,
    settings: Settings,
    storage: S3ObjectStorage | None = None,
) -> None:
    items = items if items is not None else _batch_items(session, batch.id)
    manifest = {
        "schema_version": 1,
        "batch": serialize_batch(batch),
        "items": [serialize_batch_item(item) for item in items],
    }
    body = json.dumps(manifest, indent=2, default=str, sort_keys=True).encode("utf-8")
    local_root = settings.whatsapp_inbound_media_root / "batch-manifests"
    local_root.mkdir(parents=True, exist_ok=True)
    local_path = local_root / f"{batch.batch_code}.json"
    local_path.write_bytes(body)
    if not settings.object_storage_enabled:
        return
    object_key = f"batches/{batch.created_at:%Y/%m/%d}/{batch.batch_code}/manifest.json"
    try:
        adapter = storage or S3ObjectStorage(settings)
        adapter.put_bytes(
            bucket=settings.object_storage_manifest_bucket,
            object_key=object_key,
            body=body,
            content_type="application/json",
            metadata={"batch_id": str(batch.id), "batch_code": batch.batch_code},
        )
        batch.manifest_bucket = settings.object_storage_manifest_bucket
        batch.manifest_object_key = object_key
        batch.error = None if batch.status != "failed" else batch.error
        batch.updated_at = utcnow()
        session.add(batch)
        session.flush()
    except ObjectStorageError as exc:
        batch.status = "completed_with_errors" if batch.status == "completed" else batch.status
        batch.error = str(exc)
        batch.updated_at = utcnow()
        session.add(batch)
        session.flush()


def batch_counts(session: Session) -> dict[str, int]:
    total = session.exec(select(func.count()).select_from(WhatsAppInboundBatch)).one()
    active = session.exec(
        select(func.count()).select_from(WhatsAppInboundBatch).where(
            WhatsAppInboundBatch.status.notin_(BATCH_TERMINAL_STATUSES)
        )
    ).one()
    return {"batches": int(total), "active_batches": int(active)}
