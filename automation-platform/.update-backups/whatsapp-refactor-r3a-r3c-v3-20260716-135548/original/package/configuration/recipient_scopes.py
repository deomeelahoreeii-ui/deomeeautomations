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
from whatsapp_gateway.configuration.catalog_serialization import recipient_scope_dict
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.configuration.deletion import _delete_profile_records
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.schemas import RecipientScopeInput

router = APIRouter()


@router.get("/recipient-scopes")
def recipient_scopes(
    application_id: uuid.UUID | None = None,
    channel: Literal["individual", "group"] | None = None,
    session: Session = Depends(get_session),
) -> list[dict[str, Any]]:
    ensure_defaults(session)
    filters: list[Any] = []
    if application_id:
        filters.append(WhatsAppRecipientScope.application_id == application_id)
    if channel:
        filters.append(WhatsAppRecipientScope.channel == channel)
    records = session.execute(
        select(WhatsAppRecipientScope, WhatsAppApplication)
        .join(
            WhatsAppApplication,
            WhatsAppApplication.id == WhatsAppRecipientScope.application_id,
        )
        .where(*filters)
        .order_by(
            WhatsAppApplication.name,
            WhatsAppRecipientScope.channel,
            WhatsAppRecipientScope.hierarchy_level,
            WhatsAppRecipientScope.name,
        )
    ).all()
    return [
        recipient_scope_dict(
            item,
            application,
            session.get(WhatsAppRecipientScope, item.parent_id)
            if item.parent_id
            else None,
        )
        for item, application in records
    ]


@router.post("/recipient-scopes", status_code=status.HTTP_201_CREATED)
def save_recipient_scope(
    data: RecipientScopeInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    application = session.get(WhatsAppApplication, data.application_id)
    if application is None or not application.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    item = session.get(WhatsAppRecipientScope, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="Recipient scope not found")
    duplicate = session.scalar(
        select(WhatsAppRecipientScope).where(
            WhatsAppRecipientScope.application_id == data.application_id,
            WhatsAppRecipientScope.channel == data.channel,
            WhatsAppRecipientScope.key == data.key,
            WhatsAppRecipientScope.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This recipient scope key already exists")
    parent = session.get(WhatsAppRecipientScope, data.parent_id) if data.parent_id else None
    if parent and (
        parent.application_id != data.application_id
        or parent.channel != data.channel
        or parent.id == data.id
        or parent.hierarchy_level >= data.hierarchy_level
    ):
        raise HTTPException(
            status_code=422,
            detail="Parent scope must be a higher-level scope in the same module and channel",
        )
    if item is None:
        item = WhatsAppRecipientScope(
            application_id=data.application_id,
            channel=data.channel,
            key=data.key,
            name=data.name,
        )
    elif item.application_id != data.application_id or item.channel != data.channel:
        raise HTTPException(status_code=409, detail="A recipient scope cannot move between modules or channels")
    item.parent_id = data.parent_id
    item.name = data.name
    item.hierarchy_level = data.hierarchy_level
    item.description = data.description
    item.enabled = data.enabled
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "recipient_scope_saved", f"Saved {application.name} / {item.name}")
    session.commit()
    return recipient_scope_dict(item, application, parent)


@router.delete("/recipient-scopes/{scope_id}/hard")
def hard_delete_recipient_scope(
    scope_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    root = session.get(WhatsAppRecipientScope, scope_id)
    if root is None:
        raise HTTPException(status_code=404, detail="Recipient scope not found")
    paths: set[Path] = set()

    def remove_scope(item: WhatsAppRecipientScope) -> None:
        children = session.scalars(
            select(WhatsAppRecipientScope).where(
                WhatsAppRecipientScope.parent_id == item.id
            )
        ).all()
        for child in children:
            remove_scope(child)
        profiles = session.scalars(
            select(WhatsAppDispatchProfile).where(
                WhatsAppDispatchProfile.recipient_scope_id == item.id
            )
        ).all()
        for profile in profiles:
            paths.update(_delete_profile_records(session, profile))
        templates = session.scalars(
            select(WhatsAppTemplate).where(
                WhatsAppTemplate.recipient_scope_id == item.id
            )
        ).all()
        for template in templates:
            session.delete(template)
        session.delete(item)
        session.flush()

    name = root.name
    remove_scope(root)
    activity(session, account, "recipient_scope_deleted", f"Permanently deleted recipient scope {name}")
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"id": str(scope_id), "deleted": True}
