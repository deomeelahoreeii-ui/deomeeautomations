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
from whatsapp_gateway.configuration.audience_routing import _authorize_directory_group, _validate_group_route
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.configuration.profile_serialization import dispatch_profile_dict
from whatsapp_gateway.configuration.profile_validation import validate_dispatch_profile
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.schemas import AudienceMemberInput, DispatchProfileInput, ReportingRouteSetupInput

router = APIRouter()


@router.post("/reporting-routes/setup", status_code=status.HTTP_201_CREATED)
def setup_reporting_route(
    data: ReportingRouteSetupInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    """Create a complete route, or add a group to one, without partial state."""
    activity_account, _ = ensure_defaults(session)
    existing_profile = session.get(WhatsAppDispatchProfile, data.profile_id) if data.profile_id else None
    if data.profile_id and (existing_profile is None or not existing_profile.enabled):
        raise HTTPException(status_code=404, detail="Select an enabled reporting setup")

    if existing_profile:
        application = session.get(WhatsAppApplication, existing_profile.application_id)
        report_type = session.get(WhatsAppReportType, existing_profile.report_type_id)
        audience_item = session.get(WhatsAppAudience, existing_profile.audience_id)
        account = session.get(WhatsAppAccount, existing_profile.account_id)
        wing = session.get(Wing, existing_profile.wing_id) if existing_profile.wing_id else None
        recipient_scope = session.get(WhatsAppRecipientScope, existing_profile.recipient_scope_id) if existing_profile.recipient_scope_id else None
        template = session.get(WhatsAppTemplate, existing_profile.template_id) if existing_profile.template_id else None
        if not all((application, report_type, audience_item, account, wing, recipient_scope, template)):
            raise HTTPException(status_code=409, detail="The selected reporting setup has incomplete dependencies")
        profile = existing_profile
        created = {"audience": False, "template": False, "profile": False}
    else:
        application = session.get(WhatsAppApplication, data.application_id) if data.application_id else None
        report_type = session.get(WhatsAppReportType, data.report_type_id) if data.report_type_id else None
        account = session.get(WhatsAppAccount, data.account_id) if data.account_id else None
        wing = session.get(Wing, data.wing_id) if data.wing_id else None
        recipient_scope = session.get(WhatsAppRecipientScope, data.recipient_scope_id) if data.recipient_scope_id else None
        if application is None or not application.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled platform module")
        if report_type is None or report_type.application_id != application.id or not report_type.enabled:
            raise HTTPException(status_code=422, detail="Select a report type from this module")
        if account is None or not account.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled WhatsApp connection")
        if wing is None or not wing.active:
            raise HTTPException(status_code=422, detail="Select an active AntiDengue wing")
        if recipient_scope is None or recipient_scope.application_id != application.id or recipient_scope.channel != "group" or not recipient_scope.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled group hierarchy scope")

        def available_key(model: Any, application_id: uuid.UUID | None, name: str) -> str:
            base = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:90] or "reporting_route"
            candidate = base
            suffix = 2
            filters = [model.key == candidate]
            if hasattr(model, "application_id") and application_id:
                filters.append(model.application_id == application_id)
            while session.scalar(select(model.id).where(*filters)):
                candidate = f"{base[:90]}_{suffix}"
                filters = [model.key == candidate]
                if hasattr(model, "application_id") and application_id:
                    filters.append(model.application_id == application_id)
                suffix += 1
            return candidate

        wing_label = re.sub(r"^DEO[\s_-]+", "", wing.code or wing.name, flags=re.IGNORECASE)
        audience_name = data.audience_name.strip() or f"{application.name} – {wing_label} – {recipient_scope.name}s"
        audience_item = WhatsAppAudience(
            application_id=application.id,
            key=available_key(WhatsAppAudience, application.id, audience_name),
            name=audience_name,
            description="Created by the guided reporting-route setup.",
        )
        session.add(audience_item)
        session.flush()

        template = session.get(WhatsAppTemplate, data.template_id) if data.template_id else None
        if template and (
            not template.enabled
            or template.application_id != application.id
            or template.report_type_id != report_type.id
            or template.recipient_channel not in {"any", "group"}
            or (template.recipient_scope_id and template.recipient_scope_id != recipient_scope.id)
        ):
            raise HTTPException(status_code=422, detail="The selected template is incompatible with this reporting route")
        template_created = False
        if template is None:
            if not data.create_recommended_template:
                raise HTTPException(status_code=422, detail="Select a compatible template or create the recommended template")
            template_name = data.template_name.strip() or f"{report_type.name} – {recipient_scope.name}"
            template_body = data.template_body
            if report_type.key != "tehsil_dormant_summary" and template_body.strip() == "{{report_body}}":
                template_body = "{{message}}"
            template = WhatsAppTemplate(
                application_id=application.id,
                report_type_id=report_type.id,
                recipient_scope_id=recipient_scope.id,
                recipient_channel="group",
                key=available_key(WhatsAppTemplate, None, template_name),
                name=template_name,
                category="report",
                body=template_body,
            )
            session.add(template)
            session.flush()
            template_created = True

        profile_name = data.profile_name.strip() or f"{report_type.name} – {wing_label} – {recipient_scope.name}"
        profile = WhatsAppDispatchProfile(
            application_id=application.id,
            key=available_key(WhatsAppDispatchProfile, application.id, profile_name),
            name=profile_name,
            report_type_id=report_type.id,
            audience_id=audience_item.id,
            account_id=account.id,
            template_id=template.id,
            recipient_scope_id=recipient_scope.id,
            recipient_channel="group",
            wing_id=wing.id,
            delivery_mode="groups",
            require_approval=data.require_approval,
            max_retries=data.max_retries,
            messages_per_minute=data.messages_per_minute,
            presentation_policy={
                "message_style": data.message_style,
                "attachment_mode": data.attachment_mode,
                "image_content": data.image_content,
            },
            guided_setup=True,
            owns_audience=True,
            owns_template=template_created,
            notes="Created by the guided reporting-route setup.",
        )
        session.add(profile)
        session.flush()
        created = {"audience": True, "template": template_created, "profile": True}

    if application.key != "antidengue":
        raise HTTPException(status_code=422, detail="The first guided workflow currently supports AntiDengue group reporting")
    if recipient_scope.channel != "group":
        raise HTTPException(status_code=422, detail="This guided workflow requires a group recipient scope")

    group = session.get(WhatsAppDirectoryGroup, data.directory_group_id)
    if group is None or group.account_id != account.id or not group.available:
        raise HTTPException(status_code=422, detail="Select an available group from this WhatsApp connection")
    route_data = AudienceMemberInput(
        target_type="group",
        target_id=group.id,
        wing_id=wing.id,
        route_scope_key=recipient_scope.key,
        route_scope_value=data.route_scope_value,
        route_scope_label=data.route_scope_label,
    )
    _validate_group_route(session, route_data)
    configured = session.scalar(
        select(WhatsAppGroup).where(
            WhatsAppGroup.account_id == account.id,
            or_(WhatsAppGroup.directory_group_id == group.id, WhatsAppGroup.jid == group.jid),
        )
    )
    if configured and configured.wing_id != wing.id:
        configured_wing = session.get(Wing, configured.wing_id)
        raise HTTPException(
            status_code=409,
            detail=f"{group.name} is already authorized for {configured_wing.name if configured_wing else 'another wing'}",
        )
    _authorize_directory_group(session, account=account, group=group, wing=wing)
    target_key = f"group:{group.id}"
    member = session.scalar(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience_item.id,
            WhatsAppAudienceMember.target_key == target_key,
        )
    )
    if member and member.enabled:
        raise HTTPException(status_code=409, detail=f"{group.name} is already part of this reporting setup")
    member = member or WhatsAppAudienceMember(
        audience_id=audience_item.id,
        target_type="group",
        target_key=target_key,
        directory_group_id=group.id,
    )
    member.enabled = True
    member.route_scope_key = recipient_scope.key
    member.route_scope_value = data.route_scope_value
    member.route_scope_label = data.route_scope_label
    session.add(member)
    session.flush()

    validate_dispatch_profile(
        session,
        DispatchProfileInput(
            id=profile.id,
            application_id=application.id,
            key=profile.key,
            name=profile.name,
            report_type_id=report_type.id,
            audience_id=audience_item.id,
            account_id=account.id,
            template_id=template.id,
            recipient_scope_id=recipient_scope.id,
            recipient_channel="group",
            wing_id=wing.id,
            delivery_mode="groups",
            require_approval=profile.require_approval,
            fallback_policy=profile.fallback_policy,
            max_retries=profile.max_retries,
            messages_per_minute=profile.messages_per_minute,
            message_style=(profile.presentation_policy or {}).get("message_style", "summary"),
            attachment_mode=(profile.presentation_policy or {}).get("attachment_mode", "none"),
            image_content=(profile.presentation_policy or {}).get("image_content", "details"),
            enabled=True,
            notes=profile.notes,
        ),
    )
    activity(
        session,
        activity_account,
        "reporting_route_setup",
        f"Configured {report_type.name} for {group.name} / {data.route_scope_label}",
        details={"profile_id": str(profile.id), "audience_id": str(audience_item.id), "group_id": str(group.id)},
    )
    session.commit()
    return {
        "profile": dispatch_profile_dict(profile, application, report_type, audience_item, account, wing, recipient_scope),
        "audience_id": str(audience_item.id),
        "member_id": str(member.id),
        "group_name": group.name,
        "route": {"scope": recipient_scope.name, "area": data.route_scope_label, "wing": wing.name},
        "created": created,
    }
