from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import nats
from fastapi import APIRouter, Depends, HTTPException, Query, status
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from whatsapp_gateway.inbound.batches import create_history_batch, reconcile_batch, record_batch_event
from whatsapp_gateway.inbound.history_tracking import (
    HISTORY_ACTIVE_STATUSES,
    history_contact_anchor as _history_contact_anchor,
    history_contact_counts as _history_contact_counts,
    reconcile_history_requests as _reconcile_history_requests,
    serialize_history_request as _serialize_history_request,
)
from whatsapp_gateway.inbound.history_worker_status import refresh_history_request_from_worker
from whatsapp_gateway.inbound.schemas import MAX_INBOUND_HISTORY_MESSAGES, RequestInboundHistory
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppIdentityAlias,
    WhatsAppInboundHistoryRequest,
)

router = APIRouter()


def _provider(settings: Settings) -> str:
    value = str(settings.whatsapp_inbound_history_provider or "wwebjs").strip().lower()
    if value not in {"wwebjs", "baileys"}:
        raise HTTPException(
            status_code=500,
            detail="WHATSAPP_INBOUND_HISTORY_PROVIDER must be wwebjs or baileys",
        )
    return value


def _history_subject(settings: Settings, worker_key: str, provider: str) -> str:
    base = (
        settings.whatsapp_web_history_subject
        if provider == "wwebjs"
        else settings.whatsapp_inbound_history_subject
    )
    return f"{base}.{worker_key}"


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _bridge_operator_message(result: dict[str, Any]) -> str:
    if result.get("historyReady") or result.get("ready"):
        return "Managed WhatsApp Web is ready."
    bridge_status = str(result.get("status") or "unavailable").strip().lower()
    messages = {
        "starting": "Managed WhatsApp Web is starting. Retry shortly.",
        "initializing": "Managed WhatsApp Web is starting. Retry shortly.",
        "recovering": "Managed WhatsApp Web is recovering its browser page. Retry shortly.",
        "qr": "Managed WhatsApp Web needs pairing. Scan the QR shown by the bridge process.",
        "authenticated": "Managed WhatsApp Web is authenticated and finishing startup.",
        "failed": "Managed WhatsApp Web could not start. Restart the managed bridge and check its server log.",
        "disconnected": "Managed WhatsApp Web disconnected. Restart the managed bridge.",
        "auth_failure": "Managed WhatsApp Web authentication failed. Refresh the browser snapshot and pair it again.",
    }
    return messages.get(bridge_status, "Managed WhatsApp Web is unavailable.")


async def _request_nats(
    *,
    settings: Settings,
    subject: str,
    payload: dict[str, Any],
    timeout: float | None = None,
) -> dict[str, Any]:
    client = await nats.connect(settings.whatsapp_nats_url, connect_timeout=2)
    try:
        message = await client.request(
            subject,
            json.dumps(payload).encode("utf-8"),
            timeout=timeout or settings.whatsapp_inbound_history_timeout_seconds,
        )
        return json.loads(message.data.decode("utf-8"))
    finally:
        await client.close()


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
    provider = _provider(settings)
    if provider == "baileys":
        return {
            "provider": "baileys",
            "reachable": True,
            "ready": True,
            "status": "legacy",
            "worker_id": account.worker_key,
            "message": "Legacy Baileys history provider is selected.",
        }
    try:
        result = await _request_nats(
            settings=settings,
            subject=_history_subject(settings, account.worker_key, provider),
            payload={
                "action": "bridge_health",
                "workerId": account.worker_key,
            },
            timeout=min(float(settings.whatsapp_inbound_history_timeout_seconds), 3.0),
        )
        operator_message = _bridge_operator_message(result)
        return {
            **result,
            "provider": "wwebjs",
            "reachable": True,
            # Never expose Node/Puppeteer internals through the operator API.
            # Full diagnostics remain in the structured bridge server log.
            "error": None if result.get("historyReady") else operator_message,
            "message": operator_message,
        }
    except (NoRespondersError, NatsTimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "provider": "wwebjs",
            "reachable": False,
            "ready": False,
            "status": "offline",
            "worker_id": account.worker_key,
            "error": "WhatsApp Web history bridge is not reachable.",
            "message": "WhatsApp Web history bridge is not reachable.",
        }


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
    provider = _provider(settings)

    active_query = select(WhatsAppInboundHistoryRequest).where(
        WhatsAppInboundHistoryRequest.status.in_(HISTORY_ACTIVE_STATUSES)
    )
    if provider == "wwebjs":
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
    remote_jids = list(
        dict.fromkeys(
            value
            for value in [contact.phone_jid, contact.primary_lid_jid, *aliases]
            if value
        )
    )
    if not remote_jids:
        raise HTTPException(
            status_code=409,
            detail="This contact has no WhatsApp JID available for history lookup",
        )

    anchor = _history_contact_anchor(session, contact_id=contact.id)
    request_id = str(uuid.uuid4())
    baseline_messages, baseline_attachments = _history_contact_counts(
        session, account.id, contact.id
    )
    requested_count = MAX_INBOUND_HISTORY_MESSAGES if data.all_history else data.count
    platform_remote_jid = contact.phone_jid or contact.primary_lid_jid or remote_jids[0]
    batch = create_history_batch(
        session,
        account_id=account.id,
        contact_id=contact.id,
        worker_key=account.worker_key,
        provider=provider,
        requested_count=requested_count,
        all_history=data.all_history,
        remote_jid=platform_remote_jid,
        anchor_message_id=anchor.message_id if anchor else None,
        anchor_timestamp=anchor.message_timestamp if anchor else None,
        settings=settings,
    )
    audit = WhatsAppInboundHistoryRequest(
        request_id=request_id,
        account_id=account.id,
        contact_id=contact.id,
        worker_key=account.worker_key,
        provider=provider,
        batch_id=batch.id,
        requested_count=requested_count,
        all_history=data.all_history,
        baseline_messages=baseline_messages,
        baseline_attachments=baseline_attachments,
        remote_jid=platform_remote_jid,
        anchor_message_id=anchor.message_id if anchor else None,
        anchor_timestamp=anchor.message_timestamp if anchor else None,
    )
    session.add(audit)
    session.commit()
    session.refresh(audit)

    payload = {
        "action": "request_history",
        "requestId": request_id,
        "batchId": str(batch.id),
        "batchCode": batch.batch_code,
        "workerId": account.worker_key,
        "provider": provider,
        "remoteJids": remote_jids,
        "platformRemoteJid": contact.phone_jid or contact.primary_lid_jid or remote_jids[0],
        "count": requested_count,
        "allHistory": data.all_history,
        "anchorMessageId": anchor.message_id if anchor else None,
        "beforeTimestamp": _iso_utc(anchor.message_timestamp) if anchor else None,
    }
    try:
        result = await _request_nats(
            settings=settings,
            subject=_history_subject(settings, account.worker_key, provider),
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
            session,
            batch_id=batch.id,
            event_type="history_request_accepted",
            message=(
                f"WhatsApp Web accepted the request for all available history (up to {requested_count} messages)."
                if data.all_history
                else f"WhatsApp Web accepted the request for up to {requested_count} older messages."
            ),
            details={"request_id": request_id, "operation_id": audit.operation_id, "all_history": data.all_history},
        )
        session.commit()
        session.refresh(audit)
        reconcile_batch(session, batch_id=audit.batch_id, settings=settings)
        session.commit()
        return _serialize_history_request(audit)
    except HTTPException as exc:
        audit.status = "failed"
        audit.error = str(exc.detail)
        audit.finished_at = utcnow()
        audit.updated_at = utcnow()
        batch.status = "failed"
        batch.error = audit.error
        batch.finished_at = audit.finished_at
        batch.updated_at = audit.updated_at
        session.add(batch)
        session.add(audit)
        record_batch_event(session, batch_id=batch.id, level="error", event_type="history_request_failed", message=audit.error or "History request failed.")
        session.commit()
        raise
    except NoRespondersError as exc:
        label = "WhatsApp Web history bridge" if provider == "wwebjs" else "WhatsApp worker"
        detail = f"{label} is not listening for history requests"
        audit.status = "failed"
        audit.error = detail
        audit.finished_at = utcnow()
        audit.updated_at = utcnow()
        batch.status = "failed"
        batch.error = detail
        batch.finished_at = audit.finished_at
        batch.updated_at = audit.updated_at
        session.add(batch)
        session.add(audit)
        record_batch_event(session, batch_id=batch.id, level="error", event_type="history_request_failed", message=detail)
        session.commit()
        raise HTTPException(status_code=503, detail=detail) from exc
    except NatsTimeoutError as exc:
        detail = "WhatsApp history request timed out"
        audit.status = "failed"
        audit.error = detail
        audit.finished_at = utcnow()
        audit.updated_at = utcnow()
        batch.status = "failed"
        batch.error = detail
        batch.finished_at = audit.finished_at
        batch.updated_at = audit.updated_at
        session.add(batch)
        session.add(audit)
        record_batch_event(session, batch_id=batch.id, level="error", event_type="history_request_failed", message=detail)
        session.commit()
        raise HTTPException(status_code=504, detail=detail) from exc
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        detail = f"WhatsApp history request failed: {exc}"
        audit.status = "failed"
        audit.error = detail
        audit.finished_at = utcnow()
        audit.updated_at = utcnow()
        batch.status = "failed"
        batch.error = detail
        batch.finished_at = audit.finished_at
        batch.updated_at = audit.updated_at
        session.add(batch)
        session.add(audit)
        record_batch_event(session, batch_id=batch.id, level="error", event_type="history_request_failed", message=detail)
        session.commit()
        raise HTTPException(status_code=502, detail=detail) from exc


@router.get("/history/requests")
def list_inbound_history_requests(
    contact_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    _reconcile_history_requests(session, contact_id=contact_id)
    query = select(WhatsAppInboundHistoryRequest)
    if contact_id is not None:
        query = query.where(WhatsAppInboundHistoryRequest.contact_id == contact_id)
    items = session.exec(
        query.order_by(WhatsAppInboundHistoryRequest.requested_at.desc()).limit(limit)
    ).all()
    for item in items:
        reconcile_batch(session, batch_id=item.batch_id, settings=settings)
    session.commit()
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
    reconcile_batch(session, batch_id=item.batch_id, settings=settings)
    session.commit()
    session.refresh(item)
    return _serialize_history_request(item)
