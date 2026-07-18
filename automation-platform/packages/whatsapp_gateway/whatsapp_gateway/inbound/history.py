from __future__ import annotations

import json
import uuid
from typing import Any

import nats  # Compatibility hook used by existing integrations and tests.
from fastapi import APIRouter, Depends, HTTPException, Query
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from whatsapp_gateway.inbound.batches import reconcile_batch
from whatsapp_gateway.inbound.history_request import (
    request_inbound_history, router as request_router,
)
from whatsapp_gateway.inbound.history_tracking import (
    reconcile_history_requests, serialize_history_request,
)
from whatsapp_gateway.inbound.history_transport import (
    bridge_operator_message, history_subject, provider, request_nats,
)
from whatsapp_gateway.inbound.history_worker_status import refresh_history_request_from_worker
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppDirectoryContact, WhatsAppInboundHistoryRequest,
)

router = APIRouter()
router.include_router(request_router)


@router.get("/history/bridge/status")
async def history_bridge_status(
    contact_id: uuid.UUID = Query(...),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or not contact.active:
        raise HTTPException(status_code=404, detail="WhatsApp contact was not found")
    account = session.get(WhatsAppAccount, contact.account_id)
    if account is None or not account.enabled:
        raise HTTPException(status_code=409, detail="WhatsApp account is unavailable")
    provider_name = provider(settings)
    if provider_name == "baileys":
        return {
            "provider": "baileys", "reachable": True, "ready": True,
            "status": "legacy", "worker_id": account.worker_key,
            "message": "Legacy Baileys history provider is selected.",
        }
    try:
        result = await request_nats(
            settings=settings,
            subject=history_subject(settings, account.worker_key, provider_name),
            payload={"action": "bridge_health", "workerId": account.worker_key},
            timeout=min(float(settings.whatsapp_inbound_history_timeout_seconds), 3.0),
        )
        operator_message = bridge_operator_message(result)
        return {
            **result, "provider": "wwebjs", "reachable": True,
            "error": None if result.get("historyReady") else operator_message,
            "message": operator_message,
        }
    except (NoRespondersError, NatsTimeoutError, OSError, ValueError, json.JSONDecodeError):
        return {
            "provider": "wwebjs", "reachable": False, "ready": False,
            "status": "offline", "worker_id": account.worker_key,
            "error": "WhatsApp Web history bridge is not reachable.",
            "message": "WhatsApp Web history bridge is not reachable.",
        }


@router.get("/history/requests")
def list_inbound_history_requests(
    contact_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    reconcile_history_requests(session, contact_id=contact_id)
    query = select(WhatsAppInboundHistoryRequest)
    if contact_id is not None:
        query = query.where(WhatsAppInboundHistoryRequest.contact_id == contact_id)
    items = session.exec(
        query.order_by(WhatsAppInboundHistoryRequest.requested_at.desc()).limit(limit)
    ).all()
    for item in items:
        reconcile_batch(session, batch_id=item.batch_id, settings=settings)
    session.commit()
    return {"items": [serialize_history_request(item) for item in items]}


@router.get("/history/requests/{request_id}")
async def get_inbound_history_request(
    request_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    item = session.get(WhatsAppInboundHistoryRequest, request_id)
    if item is None:
        raise HTTPException(status_code=404, detail="History request not found")
    await refresh_history_request_from_worker(session, item=item, settings=settings)
    reconcile_history_requests(session, contact_id=item.contact_id)
    reconcile_batch(session, batch_id=item.batch_id, settings=settings)
    session.commit()
    session.refresh(item)
    return serialize_history_request(item)


__all__ = [
    "get_inbound_history_request", "history_bridge_status",
    "list_inbound_history_requests", "request_inbound_history", "router",
]
