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
from whatsapp_gateway.directory.master_contacts import resolved_contact_name
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
from whatsapp_gateway.configuration.audience_routing import _authorize_directory_group, _validate_group_route
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.schemas import AudienceMemberInput

router = APIRouter()


class AudienceMemberEnabledInput(BaseModel):
    enabled: bool


@router.get("/audiences/{audience_id}/members")
def audience_members(
    audience_id: uuid.UUID,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    audience_item = session.get(WhatsAppAudience, audience_id)
    if audience_item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    filters = [WhatsAppAudienceMember.audience_id == audience_id]
    total = session.scalar(
        select(func.count()).select_from(WhatsAppAudienceMember).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppAudienceMember)
        .where(*filters)
        .order_by(WhatsAppAudienceMember.target_type, WhatsAppAudienceMember.created_at)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    items: list[dict[str, Any]] = []
    for member in records:
        if member.target_type == "group":
            target = session.get(WhatsAppDirectoryGroup, member.directory_group_id)
            configured = (
                session.scalar(
                    select(WhatsAppGroup).where(
                        WhatsAppGroup.account_id == target.account_id,
                        WhatsAppGroup.jid == target.jid,
                    )
                )
                if target
                else None
            )
            target_wing = session.get(Wing, configured.wing_id) if configured else None
            items.append({
                "id": str(member.id),
                "enabled": member.enabled,
                "target_type": "group",
                "target_id": str(member.directory_group_id),
                "name": target.name if target else "Deleted group",
                "identifier": target.jid if target else member.target_key,
                "available": target.available if target else False,
                "detail": f"{target.participant_count} participants" if target else "Unavailable",
                "route_scope_key": member.route_scope_key,
                "route_scope_value": member.route_scope_value,
                "route_scope_label": member.route_scope_label,
                "wing_id": str(configured.wing_id) if configured else None,
                "wing_name": target_wing.name if target_wing else "Not authorized",
            })
        else:
            target = session.get(WhatsAppDirectoryContact, member.directory_contact_id)
            items.append({
                "id": str(member.id),
                "enabled": member.enabled,
                "target_type": "contact",
                "target_id": str(member.directory_contact_id),
                "name": resolved_contact_name(session, target) or "Unnamed contact" if target else "Deleted contact",
                "identifier": (target.phone_jid or target.primary_lid_jid) if target else member.target_key,
                "available": target.active if target else False,
                "detail": target.primary_lid_jid or "No LID mapping" if target else "Unavailable",
                "route_scope_key": member.route_scope_key,
                "route_scope_value": member.route_scope_value,
                "route_scope_label": member.route_scope_label,
            })
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("/audiences/{audience_id}/members", status_code=status.HTTP_201_CREATED)
def add_audience_member(
    audience_id: uuid.UUID,
    data: AudienceMemberInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    audience_item = session.get(WhatsAppAudience, audience_id)
    if audience_item is None or not audience_item.enabled:
        raise HTTPException(status_code=404, detail="Enabled audience not found")
    group = session.get(WhatsAppDirectoryGroup, data.target_id) if data.target_type == "group" else None
    contact = session.get(WhatsAppDirectoryContact, data.target_id) if data.target_type == "contact" else None
    target = group or contact
    if target is None or target.account_id != account.id:
        raise HTTPException(status_code=422, detail="Select a target from the synchronized directory")
    if data.target_type == "group" and not group.available:
        raise HTTPException(status_code=422, detail="The detected group is not currently available")
    if data.target_type == "contact" and not contact.active:
        raise HTTPException(status_code=422, detail="The directory contact is inactive")
    if data.target_type == "group" and (
        data.route_scope_key not in {"district", "wing", "tehsil", "markaz", "other"}
        or not data.route_scope_value.strip()
        or not data.route_scope_label.strip()
    ):
        raise HTTPException(
            status_code=422,
            detail="Bind every group target to a district, wing, tehsil, markaz or custom route.",
        )
    wing = _validate_group_route(session, data) if group else None
    if group and wing:
        _authorize_directory_group(session, account=account, group=group, wing=wing)
    target_key = f"{data.target_type}:{data.target_id}"
    existing = session.scalar(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience_id,
            WhatsAppAudienceMember.target_key == target_key,
        )
    )
    if existing and existing.enabled:
        raise HTTPException(status_code=409, detail="This target is already in the audience")
    item = existing or WhatsAppAudienceMember(
        audience_id=audience_id,
        target_type=data.target_type,
        target_key=target_key,
    )
    item.target_type = data.target_type
    item.directory_group_id = group.id if group else None
    item.directory_contact_id = contact.id if contact else None
    item.route_scope_key = data.route_scope_key if group else ""
    item.route_scope_value = data.route_scope_value.strip() if group else ""
    item.route_scope_label = data.route_scope_label.strip() if group else ""
    item.enabled = True
    session.add(item)
    activity(session, account, "audience_member_added", f"Added {data.target_type} to {audience_item.name}")
    session.commit()
    return {
        "id": str(item.id),
        "target_type": item.target_type,
        "target_id": str(data.target_id),
        "route_scope_key": item.route_scope_key,
        "route_scope_value": item.route_scope_value,
        "route_scope_label": item.route_scope_label,
        "wing_id": str(wing.id) if wing else None,
    }


@router.put("/audiences/{audience_id}/members/{member_id}")
def update_audience_member(
    audience_id: uuid.UUID,
    member_id: uuid.UUID,
    data: AudienceMemberInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppAudienceMember, member_id)
    if item is None or item.audience_id != audience_id:
        raise HTTPException(status_code=404, detail="Audience target not found")
    if data.target_id != (item.directory_group_id or item.directory_contact_id):
        raise HTTPException(status_code=409, detail="Remove and re-add a target to change its identity")
    if item.target_type == "group" and (
        data.route_scope_key not in {"district", "wing", "tehsil", "markaz", "other"}
        or not data.route_scope_value.strip()
        or not data.route_scope_label.strip()
    ):
        raise HTTPException(status_code=422, detail="Select the hierarchy route represented by this group")
    group = session.get(WhatsAppDirectoryGroup, item.directory_group_id) if item.target_type == "group" else None
    wing = _validate_group_route(session, data) if group else None
    if group and wing:
        _authorize_directory_group(session, account=account, group=group, wing=wing)
    item.route_scope_key = data.route_scope_key if item.target_type == "group" else ""
    item.route_scope_value = data.route_scope_value.strip() if item.target_type == "group" else ""
    item.route_scope_label = data.route_scope_label.strip() if item.target_type == "group" else ""
    session.add(item)
    activity(
        session,
        account,
        "audience_member_route_updated",
        f"Updated audience route binding to {item.route_scope_key} / {item.route_scope_label}",
    )
    session.commit()
    return {
        "id": str(item.id),
        "route_scope_key": item.route_scope_key,
        "route_scope_value": item.route_scope_value,
        "route_scope_label": item.route_scope_label,
        "wing_id": str(wing.id) if wing else None,
    }


@router.patch("/audiences/{audience_id}/members/{member_id}/enabled")
def set_audience_member_enabled(
    audience_id: uuid.UUID,
    member_id: uuid.UUID,
    data: AudienceMemberEnabledInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    audience_item = session.get(WhatsAppAudience, audience_id)
    item = session.get(WhatsAppAudienceMember, member_id)
    if audience_item is None or item is None or item.audience_id != audience_id:
        raise HTTPException(status_code=404, detail="Audience target not found")
    if data.enabled and not audience_item.enabled:
        raise HTTPException(status_code=409, detail="Enable the audience before enabling its targets")
    changed = item.enabled != data.enabled
    item.enabled = data.enabled
    session.add(item)
    if changed:
        activity(
            session,
            account,
            "audience_member_enabled" if data.enabled else "audience_member_disabled",
            f"{'Enabled' if data.enabled else 'Disabled'} {item.target_type} target in {audience_item.name}",
        )
    session.commit()
    return {"id": str(item.id), "enabled": item.enabled, "changed": changed}


@router.delete("/audiences/{audience_id}/members/{member_id}")
def remove_audience_member(
    audience_id: uuid.UUID,
    member_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppAudienceMember, member_id)
    if item is None or item.audience_id != audience_id:
        raise HTTPException(status_code=404, detail="Audience member not found")
    session.delete(item)
    activity(session, account, "audience_member_deleted", "Permanently deleted a target from an audience")
    session.commit()
    return {"id": str(member_id), "deleted": True}
