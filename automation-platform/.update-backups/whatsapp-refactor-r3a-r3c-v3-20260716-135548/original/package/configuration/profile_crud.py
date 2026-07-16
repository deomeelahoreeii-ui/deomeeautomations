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
from whatsapp_gateway.configuration.deletion import _delete_guided_setup_records, _delete_profile_records
from whatsapp_gateway.configuration.profile_serialization import dispatch_profile_dict
from whatsapp_gateway.configuration.profile_validation import validate_dispatch_profile
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.schemas import DispatchProfileInput

router = APIRouter()


@router.get("/dispatch-profiles")
def dispatch_profiles(
    application_id: uuid.UUID | None = None,
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters: list[Any] = []
    if application_id:
        filters.append(WhatsAppDispatchProfile.application_id == application_id)
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(or_(WhatsAppDispatchProfile.name.ilike(pattern), WhatsAppDispatchProfile.key.ilike(pattern)))
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDispatchProfile).where(*filters)
    ) or 0
    profiles = session.scalars(
        select(WhatsAppDispatchProfile)
        .where(*filters)
        .order_by(WhatsAppDispatchProfile.name)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    items = []
    for item in profiles:
        application = session.get(WhatsAppApplication, item.application_id)
        report_type = session.get(WhatsAppReportType, item.report_type_id)
        audience_item = session.get(WhatsAppAudience, item.audience_id)
        account = session.get(WhatsAppAccount, item.account_id)
        wing = session.get(Wing, item.wing_id) if item.wing_id else None
        recipient_scope = (
            session.get(WhatsAppRecipientScope, item.recipient_scope_id)
            if item.recipient_scope_id
            else None
        )
        items.append(dispatch_profile_dict(item, application, report_type, audience_item, account, wing, recipient_scope))
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("/dispatch-profiles", status_code=status.HTTP_201_CREATED)
def save_dispatch_profile(
    data: DispatchProfileInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    activity_account, _ = ensure_defaults(session)
    data.delivery_mode = "groups" if data.recipient_channel == "group" else "individuals"
    application, report_type, audience_item, account, wing, recipient_scope = validate_dispatch_profile(session, data)
    item = session.get(WhatsAppDispatchProfile, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="Dispatch profile not found")
    duplicate = session.scalar(
        select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.application_id == data.application_id,
            WhatsAppDispatchProfile.key == data.key,
            WhatsAppDispatchProfile.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This profile key already exists in the module")
    if item is None:
        item = WhatsAppDispatchProfile(
            application_id=data.application_id,
            key=data.key,
            name=data.name,
            report_type_id=data.report_type_id,
            audience_id=data.audience_id,
            account_id=data.account_id,
        )
    elif item.application_id != data.application_id:
        raise HTTPException(status_code=409, detail="A profile cannot move between modules")
    else:
        item.version += 1
    values = data.model_dump(exclude={"id", "message_style", "attachment_mode", "image_content"})
    values["presentation_policy"] = {
        "message_style": data.message_style,
        "attachment_mode": data.attachment_mode,
        "image_content": data.image_content,
    }
    for key, value in values.items():
        setattr(item, key, value)
    item.updated_at = utcnow()
    session.add(item)
    activity(
        session,
        activity_account,
        "dispatch_profile_saved",
        f"Saved dispatch profile {application.name} / {item.name} version {item.version}",
        details={"profile_id": str(item.id), "version": item.version},
    )
    session.commit()
    return dispatch_profile_dict(item, application, report_type, audience_item, account, wing, recipient_scope)


@router.delete("/dispatch-profiles/{profile_id}")
def archive_dispatch_profile(
    profile_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppDispatchProfile, profile_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Dispatch profile not found")
    item.enabled = False
    item.version += 1
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "dispatch_profile_archived", f"Archived dispatch profile {item.name}")
    session.commit()
    return {"id": str(item.id), "enabled": False, "version": item.version}


@router.delete("/dispatch-profiles/{profile_id}/hard")
def hard_delete_dispatch_profile(
    profile_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppDispatchProfile, profile_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Dispatch profile not found")
    name = item.name
    guided_setup = item.guided_setup
    if guided_setup:
        paths, audience_deleted, template_deleted = _delete_guided_setup_records(session, item)
        activity(session, account, "reporting_setup_deleted", f"Permanently deleted reporting setup {name}")
    else:
        paths = _delete_profile_records(session, item)
        audience_deleted = False
        template_deleted = False
        activity(session, account, "dispatch_profile_deleted", f"Permanently deleted dispatch profile {name}")
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {
        "id": str(profile_id),
        "deleted": True,
        "guided_setup": guided_setup,
        "audience_deleted": audience_deleted,
        "template_deleted": template_deleted,
    }
