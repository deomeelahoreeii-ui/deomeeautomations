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
from whatsapp_gateway.configuration.audience_serialization import audience_dict
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.configuration.deletion import _delete_audience_records
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.schemas import AudienceInput

router = APIRouter()


@router.get("/audiences")
def audiences(
    application_id: uuid.UUID | None = None,
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters: list[Any] = []
    if application_id:
        filters.append(WhatsAppAudience.application_id == application_id)
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(WhatsAppAudience.name.ilike(pattern), WhatsAppAudience.key.ilike(pattern))
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppAudience).where(*filters)
    ) or 0
    records = session.execute(
        select(WhatsAppAudience, WhatsAppApplication)
        .join(WhatsAppApplication, WhatsAppApplication.id == WhatsAppAudience.application_id)
        .where(*filters)
        .order_by(WhatsAppApplication.name, WhatsAppAudience.name)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            audience_dict(session, item, application)
            for item, application in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/audiences/{audience_id}")
def audience(audience_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    ensure_defaults(session)
    item = session.get(WhatsAppAudience, audience_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    application = session.get(WhatsAppApplication, item.application_id)
    return audience_dict(session, item, application)


@router.post("/audiences", status_code=status.HTTP_201_CREATED)
def save_audience(
    data: AudienceInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    application = session.get(WhatsAppApplication, data.application_id)
    if application is None or not application.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    item = session.get(WhatsAppAudience, data.id) if data.id else None
    creating = item is None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    generated_key = data.key
    if creating:
        base_key = generated_key or re.sub(r"[^a-z0-9]+", "_", data.name.lower()).strip("_")
        base_key = (base_key or "audience")[:100]
        generated_key = base_key
        suffix = 2
        while session.scalar(
            select(WhatsAppAudience.id).where(
                WhatsAppAudience.application_id == data.application_id,
                WhatsAppAudience.key == generated_key,
            )
        ):
            ending = f"_{suffix}"
            generated_key = f"{base_key[:100 - len(ending)]}{ending}"
            suffix += 1
    if item is None:
        item = WhatsAppAudience(
            application_id=data.application_id,
            key=generated_key,
            name=data.name,
        )
    elif item.application_id != data.application_id:
        raise HTTPException(status_code=409, detail="An audience cannot move between modules")
    # Automation code may refer to this identifier, so it is immutable after create.
    if creating:
        item.key = generated_key
    item.name = data.name
    item.description = data.description
    item.enabled = data.enabled
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "audience_saved", f"Saved audience {application.name} / {item.name}")
    session.commit()
    return audience_dict(session, item, application)


@router.delete("/audiences/{audience_id}/hard")
def hard_delete_audience(
    audience_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppAudience, audience_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    name = item.name
    paths = _delete_audience_records(session, item)
    activity(session, account, "audience_deleted", f"Permanently deleted audience {name}")
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"id": str(audience_id), "deleted": True}
