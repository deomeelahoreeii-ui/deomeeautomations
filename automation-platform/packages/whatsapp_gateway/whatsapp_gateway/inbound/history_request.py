from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from whatsapp_gateway.inbound.batches import create_history_batch, reconcile_batch, record_batch_event
from whatsapp_gateway.inbound.history_tracking import (
    HISTORY_ACTIVE_STATUSES, history_contact_anchor, history_contact_counts,
    reconcile_history_requests, serialize_history_request,
)
from whatsapp_gateway.inbound.history_transport import history_subject, iso_utc, provider, request_nats
from whatsapp_gateway.inbound.schemas import MAX_INBOUND_HISTORY_MESSAGES, RequestInboundHistory
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppDirectoryContact, WhatsAppIdentityAlias,
    WhatsAppInboundHistoryRequest,
)

router = APIRouter()


def _fail_request(
    session: Session, audit: WhatsAppInboundHistoryRequest, batch,
    detail: str,
) -> None:
    now = utcnow()
    audit.status = "failed"
    audit.error = detail
    audit.finished_at = now
    audit.updated_at = now
    batch.status = "failed"
    batch.error = detail
    batch.finished_at = now
    batch.updated_at = now
    session.add(batch)
    session.add(audit)
    record_batch_event(
        session, batch_id=batch.id, level="error",
        event_type="history_request_failed", message=detail,
    )
    session.commit()


@router.post("/history/request", status_code=status.HTTP_202_ACCEPTED)
async def request_inbound_history(
    data: RequestInboundHistory,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    reconcile_history_requests(session, contact_id=data.contact_id)
    contact = session.get(WhatsAppDirectoryContact, data.contact_id)
    if contact is None or not contact.active:
        raise HTTPException(status_code=404, detail="WhatsApp contact was not found")
    account = session.get(WhatsAppAccount, contact.account_id)
    if account is None or not account.enabled:
        raise HTTPException(status_code=409, detail="WhatsApp account is unavailable")
    provider_name = provider(settings)

    active_query = select(WhatsAppInboundHistoryRequest).where(
        WhatsAppInboundHistoryRequest.status.in_(HISTORY_ACTIVE_STATUSES)
    )
    if provider_name == "wwebjs":
        active_query = active_query.where(
            WhatsAppInboundHistoryRequest.account_id == account.id,
            WhatsAppInboundHistoryRequest.provider == "wwebjs",
        )
    else:
        active_query = active_query.where(
            WhatsAppInboundHistoryRequest.contact_id == contact.id,
            WhatsAppInboundHistoryRequest.provider == "baileys",
        )
    active = session.exec(
        active_query.order_by(WhatsAppInboundHistoryRequest.requested_at.desc())
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
        raise HTTPException(
            status_code=409,
            detail="This contact has no WhatsApp JID available for history lookup",
        )

    anchor = history_contact_anchor(session, contact_id=contact.id)
    request_id = str(uuid.uuid4())
    baseline_messages, baseline_attachments = history_contact_counts(
        session, account.id, contact.id
    )
    requested_count = MAX_INBOUND_HISTORY_MESSAGES if data.all_history else data.count
    platform_remote_jid = contact.phone_jid or contact.primary_lid_jid or remote_jids[0]
    batch = create_history_batch(
        session, account_id=account.id, contact_id=contact.id,
        worker_key=account.worker_key, provider=provider_name,
        requested_count=requested_count, all_history=data.all_history,
        remote_jid=platform_remote_jid,
        anchor_message_id=anchor.message_id if anchor else None,
        anchor_timestamp=anchor.message_timestamp if anchor else None,
        settings=settings,
    )
    audit = WhatsAppInboundHistoryRequest(
        request_id=request_id, account_id=account.id, contact_id=contact.id,
        worker_key=account.worker_key, provider=provider_name, batch_id=batch.id,
        requested_count=requested_count, all_history=data.all_history,
        baseline_messages=baseline_messages, baseline_attachments=baseline_attachments,
        remote_jid=platform_remote_jid,
        anchor_message_id=anchor.message_id if anchor else None,
        anchor_timestamp=anchor.message_timestamp if anchor else None,
    )
    session.add(audit)
    session.commit()
    session.refresh(audit)

    payload = {
        "action": "request_history", "requestId": request_id,
        "batchId": str(batch.id), "batchCode": batch.batch_code,
        "workerId": account.worker_key, "provider": provider_name,
        "remoteJids": remote_jids, "platformRemoteJid": platform_remote_jid,
        "count": requested_count, "allHistory": data.all_history,
        "anchorMessageId": anchor.message_id if anchor else None,
        "beforeTimestamp": iso_utc(anchor.message_timestamp) if anchor else None,
    }
    try:
        result = await request_nats(
            settings=settings,
            subject=history_subject(settings, account.worker_key, provider_name),
            payload=payload,
        )
        if not result.get("accepted"):
            raise HTTPException(
                status_code=409,
                detail=result.get("error") or "WhatsApp history request was rejected",
            )
        now = utcnow()
        audit.status = str(result.get("status") or "accepted")
        audit.remote_jid = result.get("remoteJid") or audit.remote_jid
        audit.anchor_message_id = result.get("anchorMessageId") or audit.anchor_message_id
        returned_anchor = result.get("anchorTimestamp")
        if returned_anchor:
            audit.anchor_timestamp = datetime.fromisoformat(
                str(returned_anchor).replace("Z", "+00:00")
            ).replace(tzinfo=None)
        audit.operation_id = result.get("operationId")
        audit.accepted_at = now
        audit.updated_at = now
        batch.status = "fetching"
        batch.started_at = batch.started_at or now
        batch.updated_at = now
        session.add(batch)
        session.add(audit)
        record_batch_event(
            session, batch_id=batch.id, event_type="history_request_accepted",
            message=(
                f"WhatsApp Web accepted the request for all available history (up to {requested_count} messages)."
                if data.all_history
                else f"WhatsApp Web accepted the request for up to {requested_count} older messages."
            ),
            details={
                "request_id": request_id, "operation_id": audit.operation_id,
                "all_history": data.all_history,
            },
        )
        session.commit()
        session.refresh(audit)
        reconcile_batch(session, batch_id=audit.batch_id, settings=settings)
        session.commit()
        return serialize_history_request(audit)
    except HTTPException as exc:
        _fail_request(session, audit, batch, str(exc.detail))
        raise
    except NoRespondersError as exc:
        label = "WhatsApp Web history bridge" if provider_name == "wwebjs" else "WhatsApp worker"
        detail = f"{label} is not listening for history requests"
        _fail_request(session, audit, batch, detail)
        raise HTTPException(status_code=503, detail=detail) from exc
    except NatsTimeoutError as exc:
        detail = "WhatsApp history request timed out"
        _fail_request(session, audit, batch, detail)
        raise HTTPException(status_code=504, detail=detail) from exc
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        detail = f"WhatsApp history request failed: {exc}"
        _fail_request(session, audit, batch, detail)
        raise HTTPException(status_code=502, detail=detail) from exc


__all__ = ["request_inbound_history", "router"]
