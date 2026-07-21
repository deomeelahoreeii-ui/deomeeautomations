from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import func
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.object_storage import ObjectStorageError, S3ObjectStorage
from whatsapp_gateway.directory.master_contacts import resolved_contact_name
from whatsapp_gateway.inbound.batches import (
    BATCH_TERMINAL_STATUSES,
    reconcile_batch,
    record_batch_event,
    serialize_batch,
    serialize_batch_event,
    serialize_batch_item,
    store_attachment_object,
)
from whatsapp_gateway.inbound.history_worker_status import refresh_history_request_from_worker
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchEvent,
    WhatsAppInboundBatchItem,
    WhatsAppInboundHistoryRequest,
    WhatsAppInboundMessage,
    WhatsAppInboundProcessingRun,
)


def _sort_value(value: object) -> tuple[int, object]:
    """Keep nulls together while preserving numeric and chronological ordering."""
    if value is None:
        return (0, "")
    if isinstance(value, datetime):
        return (1, value.timestamp())
    if isinstance(value, (int, float)):
        return (1, value)
    return (1, str(value).casefold())


def object_storage_status(
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return S3ObjectStorage(settings).health()


def _batch_view(session: Session, batch: WhatsAppInboundBatch) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, batch.contact_id)
    account = session.get(WhatsAppAccount, batch.account_id)
    request = session.exec(
        select(WhatsAppInboundHistoryRequest).where(
            WhatsAppInboundHistoryRequest.batch_id == batch.id
        )
    ).first()
    push_name = session.exec(
        select(WhatsAppInboundMessage.push_name)
        .where(WhatsAppInboundMessage.directory_contact_id == batch.contact_id)
        .where(WhatsAppInboundMessage.from_me.is_(False))
        .where(WhatsAppInboundMessage.push_name.is_not(None))
        .order_by(WhatsAppInboundMessage.message_timestamp.desc())
        .limit(1)
    ).first()
    processing_run = session.exec(
        select(WhatsAppInboundProcessingRun)
        .where(WhatsAppInboundProcessingRun.batch_id == batch.id)
        .where(WhatsAppInboundProcessingRun.status.notin_(["failed", "cancelled"]))
        .order_by(WhatsAppInboundProcessingRun.created_at.desc())
        .limit(1)
    ).first()
    if processing_run is None:
        processing_run = session.exec(
            select(WhatsAppInboundProcessingRun)
            .where(WhatsAppInboundProcessingRun.batch_id == batch.id)
            .order_by(WhatsAppInboundProcessingRun.created_at.desc())
            .limit(1)
        ).first()
    return {
        **serialize_batch(batch),
        "contact_name": (resolved_contact_name(session, contact) if contact else "")
        or str(push_name or "Unnamed contact"),
        "contact_identity": (
            (contact.phone_jid or contact.primary_lid_jid or contact.canonical_key)
            if contact
            else batch.remote_jid
        ),
        "account_name": account.name if account else batch.worker_key,
        "history_request_id": str(request.id) if request else None,
        "history_status": request.status if request else None,
        "history_error": request.error if request else None,
        "processing_run_id": str(processing_run.id) if processing_run else None,
        "processing_run_code": processing_run.run_code if processing_run else None,
        "processing_status": processing_run.status if processing_run else "not_started",
        "processing_total_items": processing_run.total_items if processing_run else 0,
        "processing_processed_items": processing_run.processed_items if processing_run else 0,
        "processing_review_items": processing_run.review_items if processing_run else 0,
        "processing_failed_items": processing_run.failed_items if processing_run else 0,
    }


def list_inbound_batches(
    contact_id: uuid.UUID | None = Query(default=None),
    status: str | None = Query(default=None),
    search: str | None = Query(default=None, max_length=120),
    limit: int = Query(default=25, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="created_at", max_length=40),
    order: Literal["asc", "desc"] = Query(default="desc"),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    query = select(WhatsAppInboundBatch)
    if contact_id is not None:
        query = query.where(WhatsAppInboundBatch.contact_id == contact_id)
    if status:
        query = query.where(WhatsAppInboundBatch.status == status)
    rows = list(session.exec(query).all())
    items: list[dict[str, Any]] = []
    term = (search or "").strip().lower()
    for row in rows:
        reconcile_batch(session, batch_id=row.id, settings=settings)
        session.refresh(row)
        view = _batch_view(session, row)
        if term and term not in " ".join(
            str(view.get(key) or "").lower()
            for key in ("batch_code", "contact_name", "contact_identity", "status")
        ):
            continue
        items.append(view)
    sort_keys = {
        "batch_code": "batch_code",
        "contact_name": "contact_name",
        "status": "status",
        "messages_discovered": "messages_discovered",
        "files_discovered": "files_discovered",
        "created_at": "created_at",
    }
    sort_key = sort_keys.get(sort, "created_at")
    items.sort(
        key=lambda item: _sort_value(item.get(sort_key)),
        reverse=order == "desc",
    )
    filtered_total = len(items)
    session.commit()
    counts = {
        "total": int(session.exec(select(func.count()).select_from(WhatsAppInboundBatch)).one()),
        "active": int(
            session.exec(
                select(func.count())
                .select_from(WhatsAppInboundBatch)
                .where(WhatsAppInboundBatch.status.notin_(BATCH_TERMINAL_STATUSES))
            ).one()
        ),
        "completed": int(
            session.exec(
                select(func.count())
                .select_from(WhatsAppInboundBatch)
                .where(WhatsAppInboundBatch.status == "completed")
            ).one()
        ),
        "with_errors": int(
            session.exec(
                select(func.count())
                .select_from(WhatsAppInboundBatch)
                .where(WhatsAppInboundBatch.status.in_(["completed_with_errors", "failed"]))
            ).one()
        ),
    }
    return {
        "items": items[offset : offset + limit],
        "total": filtered_total,
        "limit": limit,
        "offset": offset,
        "counts": counts,
    }


async def read_inbound_batch(
    batch_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    batch = session.get(WhatsAppInboundBatch, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Inbound batch not found")
    request = session.exec(
        select(WhatsAppInboundHistoryRequest).where(
            WhatsAppInboundHistoryRequest.batch_id == batch.id
        )
    ).first()
    if request is not None:
        await refresh_history_request_from_worker(session, item=request, settings=settings)
    batch = reconcile_batch(session, batch_id=batch_id, settings=settings)
    assert batch is not None
    items = session.exec(
        select(WhatsAppInboundBatchItem)
        .where(WhatsAppInboundBatchItem.batch_id == batch.id)
        .order_by(WhatsAppInboundBatchItem.message_timestamp, WhatsAppInboundBatchItem.created_at)
    ).all()
    session.commit()
    return {
        **_batch_view(session, batch),
        "items": [serialize_batch_item(item) for item in items],
    }


def list_inbound_batch_events(
    batch_id: uuid.UUID,
    after: datetime | None = Query(default=None),
    limit: int = Query(default=250, ge=1, le=1000),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if session.get(WhatsAppInboundBatch, batch_id) is None:
        raise HTTPException(status_code=404, detail="Inbound batch not found")
    query = select(WhatsAppInboundBatchEvent).where(WhatsAppInboundBatchEvent.batch_id == batch_id)
    if after is not None:
        query = query.where(WhatsAppInboundBatchEvent.created_at > after)
    rows = session.exec(
        query.order_by(WhatsAppInboundBatchEvent.created_at.asc()).limit(limit)
    ).all()
    return {"items": [serialize_batch_event(row) for row in rows]}


def download_inbound_batch_item(
    batch_id: uuid.UUID,
    item_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> FileResponse:
    item = session.get(WhatsAppInboundBatchItem, item_id)
    if item is None or item.batch_id != batch_id:
        raise HTTPException(status_code=404, detail="Inbound batch item not found")
    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
    if attachment is None or not attachment.stored_path:
        raise HTTPException(
            status_code=409, detail="This file is not available in the local compatibility archive"
        )
    source = Path(attachment.stored_path)
    if not source.is_file():
        raise HTTPException(status_code=410, detail="The local compatibility file is missing")
    return FileResponse(
        source,
        media_type=attachment.detected_mime_type
        or attachment.mime_type
        or "application/octet-stream",
        filename=attachment.original_filename or source.name,
    )


def retry_inbound_batch_storage(
    batch_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    batch = session.get(WhatsAppInboundBatch, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Inbound batch not found")
    if not settings.object_storage_enabled:
        raise HTTPException(status_code=409, detail="Object storage is disabled")
    rows = session.exec(
        select(WhatsAppInboundBatchItem).where(
            WhatsAppInboundBatchItem.batch_id == batch_id,
            WhatsAppInboundBatchItem.status.in_(["failed", "storage_pending"]),
        )
    ).all()
    attempted = 0
    succeeded = 0
    errors: list[str] = []
    for item in rows:
        attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
        message = session.get(WhatsAppInboundMessage, item.message_id)
        if attachment is None or message is None or not attachment.stored_path:
            errors.append(f"{item.original_filename or item.id}: local archive is unavailable")
            continue
        source = Path(attachment.stored_path)
        if not source.is_file():
            errors.append(f"{item.original_filename or item.id}: local file is missing")
            continue
        attempted += 1
        try:
            store_attachment_object(
                session,
                attachment=attachment,
                message=message,
                source_path=source,
                settings=settings,
            )
            succeeded += 1
        except (ObjectStorageError, OSError, ValueError) as exc:
            errors.append(f"{item.original_filename or item.id}: {exc}")
    record_batch_event(
        session,
        batch_id=batch.id,
        level="warning" if errors else "info",
        event_type="storage_retry_finished",
        message=f"Storage retry finished: {succeeded}/{attempted} item(s) stored.",
        details={"attempted": attempted, "succeeded": succeeded, "errors": errors[:20]},
    )
    reconcile_batch(session, batch_id=batch.id, settings=settings)
    session.commit()
    return {"attempted": attempted, "succeeded": succeeded, "errors": errors}
