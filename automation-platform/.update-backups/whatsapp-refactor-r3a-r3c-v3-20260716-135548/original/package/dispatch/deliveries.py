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

router = APIRouter()


def delivery_dict(item: WhatsAppDelivery) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "recipient_type": item.recipient_type,
        "recipient_name": item.recipient_name,
        "target": item.target,
        "message": item.message,
        "status": item.status,
        "error": item.error,
        "queue_stream": item.queue_stream,
        "queue_sequence": item.queue_sequence,
        "queued_at": item.queued_at,
        "completed_at": item.completed_at,
    }


@router.get("/deliveries")
def deliveries(
    status_filter: str = Query(default="", alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters = [WhatsAppDelivery.status == status_filter] if status_filter else []
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDelivery).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppDelivery)
        .where(*filters)
        .order_by(WhatsAppDelivery.queued_at.desc())
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [delivery_dict(item) for item in records],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
