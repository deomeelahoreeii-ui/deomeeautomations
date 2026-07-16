
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

import nats
from fastapi import APIRouter, Depends, HTTPException, Query, status
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from whatsapp_gateway.inbound.history_tracking import (
    HISTORY_ACTIVE_STATUSES,
    history_contact_counts as _history_contact_counts,
    reconcile_history_requests as _reconcile_history_requests,
    serialize_history_request as _serialize_history_request,
)
from whatsapp_gateway.inbound.history_worker_status import refresh_history_request_from_worker
from whatsapp_gateway.inbound.schemas import RequestInboundHistory
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppIdentityAlias,
    WhatsAppInboundHistoryRequest,
)

router = APIRouter()

@router.post("/history/request", status_code=status.HTTP_202_ACCEPTED)
async def request_inbound_history(
    data: RequestInboundHistory,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    _reconcile_history_requests(session, contact_id=data.contact_id)
    contact = session.get(WhatsAppDirectoryContact, data.contact_id)
    if contact is None or not contact.active:
        raise HTTPException(status_code=404, detail="WhatsApp contact was not found")
    account = session.get(WhatsAppAccount, contact.account_id)
    if account is None or not account.enabled:
        raise HTTPException(status_code=409, detail="WhatsApp account is unavailable")
    active = session.exec(
        select(WhatsAppInboundHistoryRequest)
        .where(
            WhatsAppInboundHistoryRequest.contact_id == contact.id,
            WhatsAppInboundHistoryRequest.status.in_(HISTORY_ACTIVE_STATUSES),
        )
        .order_by(WhatsAppInboundHistoryRequest.requested_at.desc())
    ).first()
    if active is not None:
        raise HTTPException(
            status_code=409,
            detail=f"A history request is already active ({active.request_id})",
        )
    aliases = session.exec(
        select(WhatsAppIdentityAlias.lid_jid).where(
            WhatsAppIdentityAlias.account_id == account.id,
            WhatsAppIdentityAlias.contact_id == contact.id,
        )
    ).all()
    remote_jids = list(dict.fromkeys(
        value for value in [contact.phone_jid, contact.primary_lid_jid, *aliases] if value
    ))
    if not remote_jids:
        raise HTTPException(status_code=409, detail="This contact has no WhatsApp JID available for history lookup")

    request_id = str(uuid.uuid4())
    baseline_messages, baseline_attachments = _history_contact_counts(
        session, account.id, contact.id
    )
    audit = WhatsAppInboundHistoryRequest(
        request_id=request_id,
        account_id=account.id,
        contact_id=contact.id,
        worker_key=account.worker_key,
        requested_count=data.count,
        baseline_messages=baseline_messages,
        baseline_attachments=baseline_attachments,
    )
    session.add(audit)
    session.commit()
    session.refresh(audit)

    payload = {
        "action": "request_history",
        "requestId": request_id,
        "workerId": account.worker_key,
        "remoteJids": remote_jids,
        "count": data.count,
    }
    subject = f"{settings.whatsapp_inbound_history_subject}.{account.worker_key}"
    client = None
    try:
        client = await nats.connect(settings.whatsapp_nats_url, connect_timeout=2)
        message = await client.request(
            subject,
            json.dumps(payload).encode("utf-8"),
            timeout=settings.whatsapp_inbound_history_timeout_seconds,
        )
        result = json.loads(message.data.decode("utf-8"))
        if not result.get("accepted"):
            raise HTTPException(status_code=409, detail=result.get("error") or "WhatsApp history request was rejected")
        now = utcnow()
        audit.status = "accepted"
        audit.remote_jid = result.get("remoteJid")
        audit.anchor_message_id = result.get("anchorMessageId")
        anchor = result.get("anchorTimestamp")
        audit.anchor_timestamp = datetime.fromisoformat(anchor.replace("Z", "+00:00")).replace(tzinfo=None) if anchor else None
        audit.operation_id = result.get("operationId")
        audit.accepted_at = now
        audit.updated_at = now
        session.add(audit)
        session.commit()
        session.refresh(audit)
        return _serialize_history_request(audit)
    except HTTPException as exc:
        audit.status = "failed"
        audit.error = str(exc.detail)
        audit.finished_at = utcnow()
        audit.updated_at = utcnow()
        session.add(audit)
        session.commit()
        raise
    except NoRespondersError as exc:
        detail = "WhatsApp worker is not listening for history requests"
        audit.status = "failed"; audit.error = detail; audit.finished_at = utcnow(); audit.updated_at = utcnow(); session.add(audit); session.commit()
        raise HTTPException(status_code=503, detail=detail) from exc
    except NatsTimeoutError as exc:
        detail = "WhatsApp history request timed out"
        audit.status = "failed"; audit.error = detail; audit.finished_at = utcnow(); audit.updated_at = utcnow(); session.add(audit); session.commit()
        raise HTTPException(status_code=504, detail=detail) from exc
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        detail = f"WhatsApp history request failed: {exc}"
        audit.status = "failed"; audit.error = detail; audit.finished_at = utcnow(); audit.updated_at = utcnow(); session.add(audit); session.commit()
        raise HTTPException(status_code=502, detail=detail) from exc
    finally:
        if client is not None:
            await client.close()


@router.get("/history/requests")
def list_inbound_history_requests(
    contact_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    _reconcile_history_requests(session, contact_id=contact_id)
    query = select(WhatsAppInboundHistoryRequest)
    if contact_id is not None:
        query = query.where(WhatsAppInboundHistoryRequest.contact_id == contact_id)
    items = session.exec(
        query.order_by(WhatsAppInboundHistoryRequest.requested_at.desc()).limit(limit)
    ).all()
    return {"items": [_serialize_history_request(item) for item in items]}


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
    _reconcile_history_requests(session, contact_id=item.contact_id)
    session.refresh(item)
    return _serialize_history_request(item)
