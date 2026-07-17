from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import nats
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from sqlmodel import Session

from automation_core.config import Settings
from automation_core.time import utcnow
from whatsapp_gateway.inbound.batches import record_batch_event
from whatsapp_gateway.models import WhatsAppInboundHistoryRequest

WORKER_HISTORY_STATUSES = {
    "requested",
    "accepted",
    "syncing",
    "succeeded",
    "no_results",
    "failed",
    "timed_out",
}
WORKER_TERMINAL_STATUSES = {"succeeded", "no_results", "failed", "timed_out"}


def _worker_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace(" ", "T")
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    elif "+" not in normalized[10:] and "-" not in normalized[10:]:
        normalized += "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


async def refresh_history_request_from_worker(
    session: Session,
    *,
    item: WhatsAppInboundHistoryRequest,
    settings: Settings,
) -> bool:
    """Refresh one active audit row from the worker's durable lifecycle state."""
    if item.status not in {"requested", "accepted", "syncing"}:
        return False

    subject_base = (
        settings.whatsapp_web_history_subject
        if item.provider == "wwebjs"
        else settings.whatsapp_inbound_history_subject
    )
    subject = f"{subject_base}.{item.worker_key}"
    payload = {
        "action": "history_status",
        "requestId": item.request_id,
        "workerId": item.worker_key,
    }
    client = None
    try:
        client = await nats.connect(settings.whatsapp_nats_url, connect_timeout=2)
        response = await client.request(
            subject,
            json.dumps(payload).encode("utf-8"),
            timeout=min(float(settings.whatsapp_inbound_history_timeout_seconds), 3.0),
        )
        result: dict[str, Any] = json.loads(response.data.decode("utf-8"))
    except (NoRespondersError, NatsTimeoutError, OSError, ValueError, json.JSONDecodeError):
        return False
    finally:
        if client is not None:
            await client.close()

    if str(result.get("requestId") or "") != item.request_id:
        return False
    worker_status = str(result.get("status") or "").strip()
    if worker_status not in WORKER_HISTORY_STATUSES:
        return False

    now = utcnow().replace(tzinfo=None)
    changed = worker_status != item.status
    item.status = worker_status
    if worker_status in {"accepted", "syncing"} and item.accepted_at is None:
        item.accepted_at = now
        changed = True
    worker_updated_at = _worker_datetime(result.get("updatedAt"))
    if worker_status == "syncing" and worker_updated_at is not None:
        if item.last_activity_at != worker_updated_at:
            item.last_activity_at = worker_updated_at
            changed = True
    worker_messages = int(result.get("messagesReceived") or 0)
    worker_attachments = int(result.get("attachmentsDiscovered") or 0)
    if worker_messages > item.messages_received:
        item.messages_received = worker_messages
        changed = True
    if worker_attachments > item.attachments_discovered:
        item.attachments_discovered = worker_attachments
        changed = True
    error = str(result.get("error") or "").strip() or None
    if item.error != error:
        item.error = error
        changed = True
    if worker_status in WORKER_TERMINAL_STATUSES and item.finished_at is None:
        item.finished_at = now
        changed = True
    if changed:
        item.updated_at = now
        session.add(item)
        record_batch_event(
            session,
            batch_id=item.batch_id,
            level=("error" if worker_status in {"failed", "timed_out"} else "info"),
            event_type="history_worker_status",
            message=f"WhatsApp Web history status: {worker_status}.",
            details={
                "messages_received": item.messages_received,
                "attachments_discovered": item.attachments_discovered,
                "error": item.error,
            },
        )
        session.commit()
        session.refresh(item)
    return changed
