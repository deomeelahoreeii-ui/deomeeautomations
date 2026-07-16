from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func
from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.inbound_service import contact_message_filter
from whatsapp_gateway.models import (
    WhatsAppDirectoryContact,
    WhatsAppInboundAttachment,
    WhatsAppInboundHistoryRequest,
    WhatsAppInboundMessage,
)

HISTORY_ACTIVE_STATUSES = {"requested", "accepted", "syncing"}
HISTORY_QUIET_SECONDS = 8
HISTORY_NO_RESULT_SECONDS = 45
HISTORY_HARD_TIMEOUT_SECONDS = 180


def history_contact_counts(
    session: Session,
    account_id: uuid.UUID,
    contact_id: uuid.UUID,
) -> tuple[int, int]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or contact.account_id != account_id:
        return 0, 0
    identity_filter = contact_message_filter(session, contact)
    message_count = session.exec(
        select(func.count(WhatsAppInboundMessage.id)).where(
            identity_filter,
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
    attachment_count = session.exec(
        select(func.count(WhatsAppInboundAttachment.id))
        .join(
            WhatsAppInboundMessage,
            WhatsAppInboundMessage.id == WhatsAppInboundAttachment.message_id,
        )
        .where(
            identity_filter,
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
    return int(message_count or 0), int(attachment_count or 0)


def utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def serialize_history_request(
    item: WhatsAppInboundHistoryRequest,
) -> dict[str, Any]:
    return {
        "accepted": item.status not in {"failed", "timed_out"},
        "id": str(item.id),
        "request_id": item.request_id,
        "account_id": str(item.account_id),
        "contact_id": str(item.contact_id),
        "worker_key": item.worker_key,
        "requested_count": item.requested_count,
        "remote_jid": item.remote_jid,
        "anchor_message_id": item.anchor_message_id,
        "anchor_timestamp": item.anchor_timestamp,
        "operation_id": item.operation_id,
        "status": item.status,
        "baseline_messages": item.baseline_messages,
        "baseline_attachments": item.baseline_attachments,
        "messages_received": item.messages_received,
        "attachments_discovered": item.attachments_discovered,
        "error": item.error,
        "requested_at": item.requested_at,
        "accepted_at": item.accepted_at,
        "last_activity_at": item.last_activity_at,
        "finished_at": item.finished_at,
        "updated_at": item.updated_at,
        "active": item.status in HISTORY_ACTIVE_STATUSES,
    }


def reconcile_history_requests(
    session: Session,
    *,
    contact_id: uuid.UUID | None = None,
) -> bool:
    now = utc_naive(utcnow())
    query = select(WhatsAppInboundHistoryRequest).where(
        WhatsAppInboundHistoryRequest.status.in_(HISTORY_ACTIVE_STATUSES)
    )
    if contact_id is not None:
        query = query.where(WhatsAppInboundHistoryRequest.contact_id == contact_id)

    changed = False
    for item in session.exec(query).all():
        requested_at = utc_naive(item.requested_at)
        age = (now - requested_at).total_seconds()
        quiet_since = utc_naive(
            item.last_activity_at or item.accepted_at or item.requested_at
        )
        quiet = (now - quiet_since).total_seconds()
        if item.messages_received > 0 and quiet >= HISTORY_QUIET_SECONDS:
            item.status = "succeeded"
            item.finished_at = now
        elif item.messages_received == 0 and age >= HISTORY_NO_RESULT_SECONDS:
            item.status = "no_results"
            item.finished_at = now
        elif age >= HISTORY_HARD_TIMEOUT_SECONDS:
            item.status = "timed_out"
            item.finished_at = now
            item.error = (
                item.error or "WhatsApp did not finish the history request in time"
            )
        else:
            continue
        item.updated_at = now
        session.add(item)
        changed = True

    if changed:
        session.commit()
    return changed


def record_history_progress(
    session: Session,
    *,
    account_id: uuid.UUID,
    contact_id: uuid.UUID | None,
    created_message: bool,
    has_attachment: bool,
    ingestion_source: str,
) -> None:
    if not created_message or contact_id is None:
        return
    if ingestion_source not in {"history_sync", "offline_sync"}:
        return

    cutoff = utc_naive(utcnow()) - timedelta(minutes=10)
    request = session.exec(
        select(WhatsAppInboundHistoryRequest)
        .where(
            WhatsAppInboundHistoryRequest.account_id == account_id,
            WhatsAppInboundHistoryRequest.contact_id == contact_id,
            WhatsAppInboundHistoryRequest.requested_at >= cutoff,
            WhatsAppInboundHistoryRequest.status.in_(
                [
                    "requested",
                    "accepted",
                    "syncing",
                    "succeeded",
                    "no_results",
                    "timed_out",
                ]
            ),
        )
        .order_by(WhatsAppInboundHistoryRequest.requested_at.desc())
    ).first()
    if request is None:
        return

    now = utc_naive(utcnow())
    request.status = "syncing"
    request.messages_received += 1
    if has_attachment:
        request.attachments_discovered += 1
    request.last_activity_at = now
    request.finished_at = None
    request.error = None
    request.updated_at = now
    session.add(request)
