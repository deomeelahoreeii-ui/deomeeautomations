from __future__ import annotations

import uuid
from datetime import timedelta

from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.inbound.batches import record_batch_event
from whatsapp_gateway.models import WhatsAppInboundHistoryRequest


def _utc_naive(value):
    from whatsapp_gateway.inbound.history_tracking import utc_naive

    return utc_naive(value)


def record_history_progress(
    session: Session, *, account_id: uuid.UUID, contact_id: uuid.UUID | None,
    created_message: bool, has_attachment: bool, ingestion_source: str,
) -> None:
    if not created_message or contact_id is None:
        return
    if ingestion_source not in {"history_sync", "offline_sync", "web_history"}:
        return

    cutoff = _utc_naive(utcnow()) - timedelta(minutes=20)
    request = session.exec(
        select(WhatsAppInboundHistoryRequest)
        .where(
            WhatsAppInboundHistoryRequest.account_id == account_id,
            WhatsAppInboundHistoryRequest.contact_id == contact_id,
            WhatsAppInboundHistoryRequest.requested_at >= cutoff,
            WhatsAppInboundHistoryRequest.status.in_([
                "requested", "accepted", "syncing", "succeeded", "no_results", "timed_out",
            ]),
        )
        .order_by(WhatsAppInboundHistoryRequest.requested_at.desc())
    ).first()
    if request is None:
        return

    now = _utc_naive(utcnow())
    request.status = "syncing"
    request.messages_received += 1
    if has_attachment:
        request.attachments_discovered += 1
    request.last_activity_at = now
    request.finished_at = None
    request.error = None
    request.updated_at = now
    session.add(request)
    record_batch_event(
        session, batch_id=request.batch_id,
        event_type="history_file_received" if has_attachment else "history_message_received",
        message=(
            f"Received historical file metadata ({request.attachments_discovered} file(s), {request.messages_received} message(s))."
            if has_attachment else f"Received historical message {request.messages_received}."
        ),
        details={
            "messages_received": request.messages_received,
            "attachments_discovered": request.attachments_discovered,
        },
    )


__all__ = ["record_history_progress"]
