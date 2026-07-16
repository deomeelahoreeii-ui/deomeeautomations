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
from whatsapp_gateway.directory.serialization import directory_group_dict

router = APIRouter()


@router.get("/directory/summary")
def directory_summary(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    count = lambda model, *filters: session.scalar(
        select(func.count()).select_from(model).where(*filters)
    ) or 0
    last_group_sync = session.scalar(
        select(func.max(WhatsAppDirectoryGroup.last_synced_at)).where(
            WhatsAppDirectoryGroup.account_id == account.id
        )
    )
    last_contact_sync = session.scalar(
        select(func.max(WhatsAppDirectoryContact.last_synced_at)).where(
            WhatsAppDirectoryContact.account_id == account.id
        )
    )
    return {
        "groups": count(
            WhatsAppDirectoryGroup,
            WhatsAppDirectoryGroup.account_id == account.id,
            WhatsAppDirectoryGroup.available.is_(True),
        ),
        "configured_groups": count(
            WhatsAppGroup,
            WhatsAppGroup.account_id == account.id,
            WhatsAppGroup.enabled.is_(True),
        ),
        "contacts": count(
            WhatsAppDirectoryContact,
            WhatsAppDirectoryContact.account_id == account.id,
            WhatsAppDirectoryContact.active.is_(True),
        ),
        "lid_mappings": count(
            WhatsAppIdentityAlias,
            WhatsAppIdentityAlias.account_id == account.id,
        ),
        "last_synced_at": max(
            (value for value in [last_group_sync, last_contact_sync] if value),
            default=None,
        ),
    }


@router.get("/directory/groups")
def directory_groups(
    search: str = "",
    availability: Literal["all", "available", "unavailable"] = "all",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    filters: list[Any] = [WhatsAppDirectoryGroup.account_id == account.id]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppDirectoryGroup.name.ilike(pattern),
                WhatsAppDirectoryGroup.jid.ilike(pattern),
            )
        )
    if availability != "all":
        filters.append(
            WhatsAppDirectoryGroup.available.is_(availability == "available")
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDirectoryGroup).where(*filters)
    ) or 0
    records = session.execute(
        select(WhatsAppDirectoryGroup, WhatsAppGroup, Wing)
        .outerjoin(
            WhatsAppGroup,
            WhatsAppGroup.directory_group_id == WhatsAppDirectoryGroup.id,
        )
        .outerjoin(Wing, Wing.id == WhatsAppGroup.wing_id)
        .where(*filters)
        .order_by(WhatsAppDirectoryGroup.name, WhatsAppDirectoryGroup.jid)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            directory_group_dict(item, configured, wing)
            for item, configured, wing in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/directory/groups/{group_id}")
def directory_group(group_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppDirectoryGroup, group_id)
    if item is None or item.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    configured = session.scalar(
        select(WhatsAppGroup).where(WhatsAppGroup.directory_group_id == item.id)
    )
    wing = session.get(Wing, configured.wing_id) if configured else None
    return directory_group_dict(item, configured, wing)


@router.get("/directory/groups/{group_id}/members")
def directory_group_members(
    group_id: uuid.UUID,
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    group = session.get(WhatsAppDirectoryGroup, group_id)
    if group is None or group.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    filters: list[Any] = [
        WhatsAppGroupMember.directory_group_id == group.id,
        WhatsAppGroupMember.active.is_(True),
    ]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppGroupMember.display_name.ilike(pattern),
                WhatsAppGroupMember.phone_jid.ilike(pattern),
                WhatsAppGroupMember.lid_jid.ilike(pattern),
                WhatsAppGroupMember.member_jid.ilike(pattern),
            )
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppGroupMember).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppGroupMember)
        .where(*filters)
        .order_by(WhatsAppGroupMember.display_name, WhatsAppGroupMember.member_key)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            {
                "id": str(item.id),
                "contact_id": str(item.contact_id) if item.contact_id else None,
                "display_name": item.display_name,
                "member_jid": item.member_jid,
                "phone_jid": item.phone_jid,
                "lid_jid": item.lid_jid,
                "admin_role": item.admin_role,
                "last_seen_at": item.last_seen_at,
            }
            for item in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/directory/contacts")
def directory_contacts(
    search: str = "",
    token_status: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    filters: list[Any] = [
        WhatsAppDirectoryContact.account_id == account.id,
        WhatsAppDirectoryContact.active.is_(True),
    ]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppDirectoryContact.display_name.ilike(pattern),
                WhatsAppDirectoryContact.phone_jid.ilike(pattern),
                WhatsAppDirectoryContact.primary_lid_jid.ilike(pattern),
            )
        )
    if token_status:
        filters.append(WhatsAppDirectoryContact.token_status == token_status)
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDirectoryContact).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppDirectoryContact)
        .where(*filters)
        .order_by(
            WhatsAppDirectoryContact.display_name,
            WhatsAppDirectoryContact.phone_jid,
        )
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    items = []
    for item in records:
        aliases = session.scalars(
            select(WhatsAppIdentityAlias)
            .where(WhatsAppIdentityAlias.contact_id == item.id)
            .order_by(WhatsAppIdentityAlias.confidence.desc())
        ).all()
        group_count = session.scalar(
            select(func.count())
            .select_from(WhatsAppGroupMember)
            .where(
                WhatsAppGroupMember.contact_id == item.id,
                WhatsAppGroupMember.active.is_(True),
            )
        ) or 0
        link_count = session.scalar(
            select(func.count())
            .select_from(WhatsAppContactLink)
            .where(
                WhatsAppContactLink.directory_contact_id == item.id,
                WhatsAppContactLink.active.is_(True),
                WhatsAppContactLink.status == "verified",
            )
        ) or 0
        items.append(
            {
                "id": str(item.id),
                "display_name": item.display_name,
                "phone_jid": item.phone_jid,
                "primary_lid_jid": item.primary_lid_jid,
                "aliases": [alias.lid_jid for alias in aliases],
                "alias_count": len(aliases),
                "group_count": group_count,
                "link_count": link_count,
                "source": item.source,
                "confidence": item.confidence,
                "token_status": item.token_status,
                "token_checked_at": item.token_checked_at,
                "token_issued_at": item.token_issued_at,
                "last_seen_at": item.last_seen_at,
            }
        )
    return {"items": items, "total": total, "page": page, "page_size": page_size}
