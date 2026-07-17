from __future__ import annotations

import uuid
from typing import Any

from fastapi import Depends, HTTPException, Query
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.object_storage import S3ObjectStorage
from whatsapp_gateway.inbound.batches import (
    reconcile_batch,
    serialize_batch,
    serialize_batch_item,
)
from whatsapp_gateway.models import WhatsAppInboundBatch, WhatsAppInboundBatchItem


def object_storage_status(
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return S3ObjectStorage(settings).health()


def list_inbound_batches(
    contact_id: uuid.UUID | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    query = select(WhatsAppInboundBatch)
    if contact_id is not None:
        query = query.where(WhatsAppInboundBatch.contact_id == contact_id)
    if status:
        query = query.where(WhatsAppInboundBatch.status == status)
    rows = session.exec(
        query.order_by(WhatsAppInboundBatch.created_at.desc()).limit(limit)
    ).all()
    items = []
    for row in rows:
        reconcile_batch(session, batch_id=row.id, settings=settings)
        session.refresh(row)
        items.append(serialize_batch(row))
    session.commit()
    return {"items": items}


def read_inbound_batch(
    batch_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    batch = reconcile_batch(session, batch_id=batch_id, settings=settings)
    if batch is None:
        raise HTTPException(status_code=404, detail="Inbound batch not found")
    items = session.exec(
        select(WhatsAppInboundBatchItem)
        .where(WhatsAppInboundBatchItem.batch_id == batch.id)
        .order_by(WhatsAppInboundBatchItem.created_at)
    ).all()
    session.commit()
    return {
        **serialize_batch(batch),
        "items": [serialize_batch_item(item) for item in items],
    }
