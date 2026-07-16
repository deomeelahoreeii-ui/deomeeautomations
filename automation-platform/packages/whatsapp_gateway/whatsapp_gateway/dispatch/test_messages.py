from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import nats
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from pydantic import BaseModel, Field as PydanticField
from sqlalchemy import func, or_, select
from sqlmodel import Session

from automation_core.config import get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from master_data.models import District, Markaz, Officer, School, SchoolHead, Tehsil, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppContactLink,
    WhatsAppDelivery,
    WhatsAppDirectoryContact,
    WhatsAppDirectoryGroup,
    WhatsAppDispatchPreview,
    WhatsAppDispatchApproval,
    WhatsAppGroup,
    WhatsAppGroupMember,
    WhatsAppIdentityAlias,
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
    WhatsAppSettings,
    WhatsAppTemplate,
)
from whatsapp_gateway.preview_service import (
    cleanup_unreferenced_preview_files,
    delete_preview_records,
)
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.dispatch.deliveries import delivery_dict
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.gateway.client import refresh_health
from whatsapp_gateway.schemas import TestMessageInput

router = APIRouter()


def normalize_target(value: str) -> str:
    target = value.strip()
    if target.endswith("@g.us") or target.endswith("@s.whatsapp.net"):
        return target
    digits = re.sub(r"\D", "", target)
    if digits.startswith("03"):
        digits = "92" + digits[1:]
    elif digits.startswith("3") and len(digits) == 10:
        digits = "92" + digits
    if not re.fullmatch(r"923\d{9}", digits):
        raise HTTPException(status_code=422, detail="Enter a valid Pakistani WhatsApp number")
    return f"{digits}@s.whatsapp.net"


@router.post("/test-message", status_code=status.HTTP_202_ACCEPTED)
async def send_test_message(
    data: TestMessageInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    if not gateway_settings.live_delivery_enabled:
        raise HTTPException(
            status_code=409,
            detail="Live delivery is disabled in WhatsApp Settings",
        )
    health = await refresh_health(session, account)
    if not health.get("ready"):
        raise HTTPException(status_code=503, detail="WhatsApp gateway is not ready")

    target = normalize_target(data.target)
    delivery = WhatsAppDelivery(
        account_id=account.id,
        recipient_type="test",
        recipient_name=data.recipient_name,
        target=target,
        message=data.message,
        status_subject=f"whatsapp.status.platform.{uuid.uuid4().hex}",
    )
    session.add(delivery)
    session.flush()
    activity(
        session,
        account,
        "test_message_queued",
        f"Queued a test message for {data.recipient_name}",
        details={"delivery_id": str(delivery.id), "target": target},
    )
    session.commit()

    client = None
    try:
        config = get_settings()
        client = await nats.connect(config.whatsapp_nats_url, connect_timeout=2)
        subscription = await client.subscribe(delivery.status_subject)
        await client.flush()
        payload = {
            "job_id": str(delivery.id),
            "target": target,
            "type": "group" if target.endswith("@g.us") else "contact",
            "recipient_name": data.recipient_name,
            "text": data.message,
            "delay_ms": gateway_settings.send_delay_ms,
            "status_subject": delivery.status_subject,
        }
        acknowledgement = await client.jetstream().publish(
            account.command_subject,
            json.dumps(payload).encode("utf-8"),
            headers={"Nats-Msg-Id": str(delivery.id)},
        )
        delivery.queue_stream = acknowledgement.stream
        delivery.queue_sequence = acknowledgement.seq
        session.add(delivery)
        session.commit()

        try:
            message = await subscription.next_msg(
                timeout=gateway_settings.acknowledgement_timeout_seconds
            )
            result = json.loads(message.data.decode("utf-8"))
            delivery.status = str(result.get("status") or "failed")
            delivery.error = result.get("error")
            delivery.provider_result = result
            delivery.completed_at = utcnow()
            activity(
                session,
                account,
                "test_message_completed",
                f"Test message {delivery.status} for {data.recipient_name}",
                level="error" if delivery.status == "failed" else "info",
                details={"delivery_id": str(delivery.id), "status": delivery.status},
            )
        except NatsTimeoutError:
            delivery.status = "timed_out"
            delivery.error = "Gateway acknowledgement timed out"
            delivery.completed_at = utcnow()
            activity(
                session,
                account,
                "test_message_timeout",
                f"Test message acknowledgement timed out for {data.recipient_name}",
                level="warning",
                details={"delivery_id": str(delivery.id)},
            )
        session.add(delivery)
        session.commit()
    except Exception as exc:
        delivery.status = "failed"
        delivery.error = str(exc)
        delivery.completed_at = utcnow()
        activity(
            session,
            account,
            "test_message_failed",
            f"Could not queue test message: {exc}",
            level="error",
            details={"delivery_id": str(delivery.id)},
        )
        session.add(delivery)
        session.commit()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    finally:
        if client is not None:
            await client.close()
    return delivery_dict(delivery)
