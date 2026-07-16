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
from whatsapp_gateway.gateway.client import refresh_health
from whatsapp_gateway.gateway.serialization import account_dict, settings_dict

router = APIRouter()


@router.get("/overview")
async def overview(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    health = await refresh_health(session, account)
    since = utcnow() - timedelta(hours=24)
    count = lambda model, *filters: session.scalar(
        select(func.count()).select_from(model).where(*filters)
    ) or 0
    return {
        "account": account_dict(account),
        "health": health,
        "settings": settings_dict(gateway_settings),
        "counts": {
            "groups": count(WhatsAppGroup, WhatsAppGroup.enabled.is_(True)),
            "queued": count(WhatsAppDelivery, WhatsAppDelivery.status == "queued"),
            "delivered_24h": count(
                WhatsAppDelivery,
                WhatsAppDelivery.status.in_(["delivered", "sent_pending_confirmation"]),
                WhatsAppDelivery.queued_at >= since,
            ),
            "failed_24h": count(
                WhatsAppDelivery,
                WhatsAppDelivery.status.in_(["failed", "timed_out"]),
                WhatsAppDelivery.queued_at >= since,
            ),
            "recipients": count(Officer, Officer.active.is_(True))
            + count(SchoolHead, SchoolHead.active.is_(True)),
        },
    }


@router.get("/connection")
async def connection(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    health = await refresh_health(session, account)
    return {
        "account": account_dict(account),
        "health": health,
        "settings": settings_dict(gateway_settings),
    }


@router.get("/connection/qr")
def connection_qr() -> FileResponse:
    path = get_settings().whatsapp_qr_image_path
    if not path.exists():
        raise HTTPException(status_code=404, detail="No WhatsApp login QR is pending")
    return FileResponse(path, media_type="image/png", filename="whatsapp-login-qr.png")
