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
from whatsapp_gateway.schemas import DispatchProfileInput

def validate_dispatch_profile(
    session: Session,
    data: DispatchProfileInput,
) -> tuple[WhatsAppApplication, WhatsAppReportType, WhatsAppAudience, WhatsAppAccount, Wing | None, WhatsAppRecipientScope | None]:
    application = session.get(WhatsAppApplication, data.application_id)
    report_type = session.get(WhatsAppReportType, data.report_type_id)
    audience_item = session.get(WhatsAppAudience, data.audience_id)
    account = session.get(WhatsAppAccount, data.account_id)
    wing = session.get(Wing, data.wing_id) if data.wing_id else None
    recipient_scope = (
        session.get(WhatsAppRecipientScope, data.recipient_scope_id)
        if data.recipient_scope_id
        else None
    )
    if application is None or not application.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    if report_type is None or report_type.application_id != application.id or not report_type.enabled:
        raise HTTPException(status_code=422, detail="Report type does not belong to the selected module")
    if audience_item is None or audience_item.application_id != application.id or not audience_item.enabled:
        raise HTTPException(status_code=422, detail="Audience does not belong to the selected module")
    if account is None or not account.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled WhatsApp connection")
    if data.enabled and recipient_scope is None:
        raise HTTPException(status_code=422, detail="Select an individual role or group hierarchy scope")
    if recipient_scope and (
        recipient_scope.application_id != application.id
        or recipient_scope.channel != data.recipient_channel
        or not recipient_scope.enabled
    ):
        raise HTTPException(status_code=422, detail="Recipient scope does not match the selected module and channel")
    if data.enabled and not data.template_id:
        raise HTTPException(status_code=422, detail="Enabled dispatch profiles require a report template")
    if data.template_id:
        template = session.get(WhatsAppTemplate, data.template_id)
        if template is None or not template.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled message template")
        if template.application_id != application.id:
            raise HTTPException(status_code=422, detail="Template belongs to another module")
        if template.report_type_id != report_type.id:
            raise HTTPException(status_code=422, detail="Template belongs to another report type")
        if template.category != "report":
            raise HTTPException(status_code=422, detail="Select a report template, not a system template")
        if template.recipient_channel not in {"any", data.recipient_channel}:
            raise HTTPException(status_code=422, detail="Template belongs to another recipient channel")
        if template.recipient_scope_id and template.recipient_scope_id != data.recipient_scope_id:
            raise HTTPException(status_code=422, detail="Template belongs to another recipient role or scope")
    members = session.scalars(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience_item.id,
            WhatsAppAudienceMember.enabled.is_(True),
        )
    ).all()
    group_members = [member for member in members if member.target_type == "group"]
    contact_members = [member for member in members if member.target_type == "contact"]
    if data.enabled and not members:
        raise HTTPException(status_code=422, detail="Add at least one target before enabling this profile")
    if data.recipient_channel == "group" and (not group_members or contact_members):
        raise HTTPException(status_code=422, detail="Group profiles require a group-only audience")
    if data.recipient_channel == "individual" and (not contact_members or group_members):
        raise HTTPException(status_code=422, detail="Individual profiles require a contact-only audience")
    if data.recipient_channel == "group" and recipient_scope:
        unbound = [
            member
            for member in group_members
            if member.route_scope_key != recipient_scope.key
            or not member.route_scope_value
            or not member.route_scope_label
        ]
        if unbound:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Every audience group must be bound to a {recipient_scope.name} route "
                    "before this profile can be enabled."
                ),
            )
    for member in group_members:
        target = session.get(WhatsAppDirectoryGroup, member.directory_group_id)
        if target is None or target.account_id != account.id:
            raise HTTPException(status_code=422, detail="Audience contains a group from another WhatsApp connection")
    for member in contact_members:
        target = session.get(WhatsAppDirectoryContact, member.directory_contact_id)
        if target is None or target.account_id != account.id:
            raise HTTPException(status_code=422, detail="Audience contains a contact from another WhatsApp connection")
    if application.key == "antidengue" and (wing is None or not wing.active):
        raise HTTPException(status_code=422, detail="AntiDengue profiles require an active wing scope")
    if data.fallback_policy == "same_scope" and application.key == "antidengue" and not wing:
        raise HTTPException(status_code=422, detail="Same-scope fallback requires a wing scope")
    if application.key == "antidengue" and wing and group_members:
        group_ids = [member.directory_group_id for member in group_members]
        conflicts = session.execute(
            select(WhatsAppDispatchProfile, WhatsAppAudienceMember)
            .join(
                WhatsAppAudienceMember,
                WhatsAppAudienceMember.audience_id == WhatsAppDispatchProfile.audience_id,
            )
            .where(
                WhatsAppDispatchProfile.application_id == application.id,
                WhatsAppDispatchProfile.enabled.is_(True),
                WhatsAppDispatchProfile.wing_id.is_not(None),
                WhatsAppDispatchProfile.wing_id != wing.id,
                WhatsAppDispatchProfile.id != data.id if data.id else True,
                WhatsAppAudienceMember.directory_group_id.in_(group_ids),
                WhatsAppAudienceMember.enabled.is_(True),
            )
        ).first()
        if conflicts:
            raise HTTPException(status_code=409, detail="A group in this audience is already authorized for another AntiDengue wing")
    return application, report_type, audience_item, account, wing, recipient_scope
