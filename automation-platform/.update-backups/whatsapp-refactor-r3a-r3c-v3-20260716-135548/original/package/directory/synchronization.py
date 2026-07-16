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
from whatsapp_gateway.directory.contacts import upsert_directory_contact
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.gateway.client import gateway_request
from whatsapp_gateway.gateway.serialization import gateway_datetime

router = APIRouter()


@router.post("/directory/sync")
async def sync_directory(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    config = get_settings()
    groups_result = await gateway_request(
        config.whatsapp_group_directory_subject,
        {"refresh": True},
        timeout=30,
    )
    identities_result = await gateway_request(
        config.whatsapp_identity_directory_subject,
        {"action": "list"},
        timeout=30,
    )
    synced_at = gateway_datetime(
        identities_result.get("checkedAt") or groups_result.get("checkedAt")
    )
    for item in session.scalars(
        select(WhatsAppDirectoryGroup).where(
            WhatsAppDirectoryGroup.account_id == account.id
        )
    ):
        item.available = False
        session.add(item)
    for source in groups_result.get("groups") or []:
        jid = str(source.get("jid") or "").strip()
        if not jid.endswith("@g.us"):
            continue
        item = session.scalar(
            select(WhatsAppDirectoryGroup).where(
                WhatsAppDirectoryGroup.account_id == account.id,
                WhatsAppDirectoryGroup.jid == jid,
            )
        )
        if item is None:
            item = WhatsAppDirectoryGroup(account_id=account.id, jid=jid)
        item.name = str(source.get("name") or item.name or jid)
        item.description = str(source.get("description") or "")
        item.owner_jid = str(source.get("ownerJid") or "") or None
        item.participant_count = int(source.get("participantCount") or 0)
        item.available = True
        item.last_seen_at = synced_at
        item.last_synced_at = synced_at
        session.add(item)
    for item in session.scalars(
        select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account.id
        )
    ):
        item.active = False
        session.add(item)
    contact_count = 0
    for source in identities_result.get("contacts") or []:
        if upsert_directory_contact(session, account, source, synced_at):
            contact_count += 1
    activity(
        session,
        account,
        "directory_synchronized",
        "Synchronized WhatsApp groups and identities",
        details={
            "groups": len(groups_result.get("groups") or []),
            "contacts": contact_count,
        },
    )
    session.commit()
    return {
        "groups": len(groups_result.get("groups") or []),
        "contacts": contact_count,
        "synced_at": synced_at,
    }


@router.post("/directory/groups/{group_id}/members/sync")
async def sync_group_members(
    group_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    group = session.get(WhatsAppDirectoryGroup, group_id)
    if group is None or group.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    result = await gateway_request(
        get_settings().whatsapp_group_members_subject,
        {"groupJid": group.jid},
        timeout=30,
    )
    synced_at = gateway_datetime(result.get("checkedAt"))
    for item in session.scalars(
        select(WhatsAppGroupMember).where(
            WhatsAppGroupMember.directory_group_id == group.id
        )
    ):
        item.active = False
        session.add(item)
    for source in result.get("members") or []:
        member_jid = str(source.get("id") or "").strip() or None
        phone_jid = str(source.get("phoneNumber") or "").strip() or None
        lid_jid = str(source.get("lid") or "").strip() or None
        if member_jid and member_jid.endswith("@s.whatsapp.net"):
            phone_jid = phone_jid or member_jid
        if member_jid and member_jid.endswith("@lid"):
            lid_jid = lid_jid or member_jid
        member_key = phone_jid or lid_jid or member_jid
        if not member_key:
            continue
        contact = upsert_directory_contact(
            session,
            account,
            {
                "phoneJid": phone_jid,
                "primaryLidJid": lid_jid,
                "displayName": source.get("name") or "",
                "source": "group_metadata",
                "confidence": 0.9 if phone_jid and lid_jid else 0.6,
                "lastSeenAt": synced_at.isoformat(),
                "aliases": [{"lidJid": lid_jid, "source": "group_metadata"}]
                if lid_jid
                else [],
            },
            synced_at,
        )
        member = session.scalar(
            select(WhatsAppGroupMember).where(
                WhatsAppGroupMember.directory_group_id == group.id,
                WhatsAppGroupMember.member_key == member_key,
            )
        )
        if member is None:
            member = WhatsAppGroupMember(
                directory_group_id=group.id,
                member_key=member_key,
            )
        member.contact_id = contact.id if contact else None
        member.member_jid = member_jid
        member.phone_jid = phone_jid
        member.lid_jid = lid_jid
        member.display_name = str(source.get("name") or member.display_name)
        member.admin_role = str(source.get("admin") or "") or None
        member.active = True
        member.last_seen_at = synced_at
        session.add(member)
    group.name = str(result.get("groupName") or group.name)
    group.participant_count = int(result.get("size") or 0)
    group.last_synced_at = synced_at
    group.last_seen_at = synced_at
    session.add(group)
    activity(
        session,
        account,
        "group_members_synchronized",
        f"Synchronized members for {group.name}",
        details={"group_id": str(group.id), "members": len(result.get("members") or [])},
    )
    session.commit()
    return {"members": len(result.get("members") or []), "synced_at": synced_at}


@router.post("/directory/contacts/{contact_id}/token/refresh")
async def refresh_contact_token(
    contact_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or contact.account_id != account.id:
        raise HTTPException(status_code=404, detail="WhatsApp contact not found")
    if not contact.primary_lid_jid:
        raise HTTPException(status_code=422, detail="This contact has no mapped LID")
    result = await gateway_request(
        get_settings().whatsapp_identity_directory_subject,
        {"action": "refresh_token", "lidJid": contact.primary_lid_jid},
        timeout=30,
    )
    token = result.get("token") or {}
    contact.token_status = str(token.get("status") or "unknown")
    contact.token_checked_at = gateway_datetime(token.get("checkedAt"))
    contact.token_issued_at = (
        gateway_datetime(token.get("issuedAt")) if token.get("issuedAt") else None
    )
    contact.last_synced_at = utcnow()
    session.add(contact)
    activity(
        session,
        account,
        "identity_token_refreshed",
        f"Refreshed privacy token status for {contact.display_name or contact.phone_jid}",
        details={"contact_id": str(contact.id), "status": contact.token_status},
    )
    session.commit()
    return {
        "id": str(contact.id),
        "token_status": contact.token_status,
        "token_checked_at": contact.token_checked_at,
        "token_issued_at": contact.token_issued_at,
    }
