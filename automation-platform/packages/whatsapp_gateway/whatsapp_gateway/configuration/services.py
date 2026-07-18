from __future__ import annotations

from whatsapp_gateway.api_imports import *

DEFAULT_APPLICATIONS = {
    "antidengue": (
        "AntiDengue",
        "Dengue activity reports, officer summaries and escalation workflows.",
    ),
    "crm": (
        "CRM",
        "Filtered workbooks, PDFs and CRM operational notifications.",
    ),
    "pmdu": (
        "PMDU",
        "Performance monitoring reports and PMDU notifications.",
    ),
}

DEFAULT_REPORT_TYPES = {
    "antidengue": [
        ("school_activity", "School activity report", "School-level activity evidence."),
        ("officer_summary", "Officer summary", "Officer coverage and follow-up summary."),
        ("wing_summary", "Wing summary", "Consolidated report for a wing audience."),
        ("consolidated_action_digest", "Consolidated Action Digest", "One concise digest and workbook covering dormant, distance and timing issues."),
    ],
    "crm": [
        ("filtered_workbook", "Filtered workbook", "CRM workbook filtered for an audience."),
        ("filtered_pdf", "Filtered PDF", "CRM PDF filtered for an audience."),
    ],
    "pmdu": [
        ("performance_report", "Performance report", "PMDU performance report for an audience."),
    ],
}

DEFAULT_RECIPIENT_SCOPES = {
    "antidengue": [
        ("individual", "deo", "DEO", 0, "District education officer."),
        ("individual", "ddeo", "DDEO", 1, "Tehsil-level deputy district education officer."),
        ("individual", "aeo", "AEO", 2, "Markaz-level assistant education officer."),
        ("individual", "school_head", "School head", 3, "Head teacher or school focal person."),
        ("individual", "other", "Other individual", 9, "Custom individual recipient role."),
        ("group", "district", "District group", 0, "District-wide WhatsApp group."),
        ("group", "wing", "Wing group", 1, "Wing-specific WhatsApp group."),
        ("group", "tehsil", "Tehsil group", 2, "Tehsil-level WhatsApp group."),
        ("group", "markaz", "Markaz group", 3, "Markaz-level WhatsApp group."),
        ("group", "other", "Other group", 9, "Custom group recipient scope."),
    ],
    "crm": [
        ("individual", "officer", "Officer", 0, "Individual CRM recipient."),
        ("individual", "other", "Other individual", 9, "Custom individual recipient role."),
        ("group", "district", "District group", 0, "District CRM audience."),
        ("group", "wing", "Wing group", 1, "Wing CRM audience."),
        ("group", "tehsil", "Tehsil group", 2, "Tehsil CRM audience."),
        ("group", "other", "Other group", 9, "Custom group recipient scope."),
    ],
    "pmdu": [
        ("individual", "officer", "Officer", 0, "Individual PMDU recipient."),
        ("individual", "other", "Other individual", 9, "Custom individual recipient role."),
        ("group", "district", "District group", 0, "District PMDU audience."),
        ("group", "wing", "Wing group", 1, "Wing PMDU audience."),
        ("group", "tehsil", "Tehsil group", 2, "Tehsil PMDU audience."),
        ("group", "other", "Other group", 9, "Custom group recipient scope."),
    ],
}

from whatsapp_gateway.api_schemas import *

def _delete_profile_records(
    session: Session,
    profile: WhatsAppDispatchProfile,
) -> set[Path]:
    paths: set[Path] = set()
    previews = session.scalars(
        select(WhatsAppDispatchPreview).where(
            WhatsAppDispatchPreview.dispatch_profile_id == profile.id
        )
    ).all()
    for preview in previews:
        if session.scalar(
            select(WhatsAppDispatchApproval.id).where(
                WhatsAppDispatchApproval.preview_id == preview.id
            )
        ):
            raise HTTPException(
                status_code=409,
                detail="This setup has an approved dispatch and must be retained for audit",
            )
        paths.update(delete_preview_records(session, preview))
    session.delete(profile)
    session.flush()
    return paths

def _delete_audience_records(
    session: Session,
    audience: WhatsAppAudience,
) -> set[Path]:
    paths: set[Path] = set()
    profiles = session.scalars(
        select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.audience_id == audience.id
        )
    ).all()
    for profile in profiles:
        paths.update(_delete_profile_records(session, profile))
    members = session.scalars(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience.id
        )
    ).all()
    for member in members:
        session.delete(member)
    session.flush()
    session.delete(audience)
    session.flush()
    return paths

def _delete_guided_setup_records(
    session: Session,
    profile: WhatsAppDispatchProfile,
) -> tuple[set[Path], bool, bool]:
    """Delete wizard-owned configuration while preserving shared resources."""
    audience = session.get(WhatsAppAudience, profile.audience_id)
    template = session.get(WhatsAppTemplate, profile.template_id) if profile.template_id else None
    audience_deleted = False
    template_deleted = False
    profile_count = session.scalar(
        select(func.count())
        .select_from(WhatsAppDispatchProfile)
        .where(WhatsAppDispatchProfile.audience_id == profile.audience_id)
    ) or 0
    if profile.owns_audience and audience is not None and profile_count == 1:
        paths = _delete_audience_records(session, audience)
        audience_deleted = True
    else:
        paths = _delete_profile_records(session, profile)

    if profile.owns_template and template is not None:
        remaining_template_profiles = session.scalar(
            select(func.count())
            .select_from(WhatsAppDispatchProfile)
            .where(WhatsAppDispatchProfile.template_id == template.id)
        ) or 0
        if remaining_template_profiles == 0:
            session.delete(template)
            session.flush()
            template_deleted = True
    return paths, audience_deleted, template_deleted

def report_type_dict(
    item: WhatsAppReportType, application: WhatsAppApplication
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "description": item.description,
        "artifact_kind": item.artifact_kind,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }

def recipient_scope_dict(
    item: WhatsAppRecipientScope,
    application: WhatsAppApplication,
    parent: WhatsAppRecipientScope | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "parent_id": str(item.parent_id) if item.parent_id else None,
        "parent_name": parent.name if parent else None,
        "channel": item.channel,
        "key": item.key,
        "name": item.name,
        "hierarchy_level": item.hierarchy_level,
        "description": item.description,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }

def audience_dict(
    session: Session,
    item: WhatsAppAudience,
    application: WhatsAppApplication,
) -> dict[str, Any]:
    group_count = session.scalar(
        select(func.count()).select_from(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == item.id,
            WhatsAppAudienceMember.target_type == "group",
            WhatsAppAudienceMember.enabled.is_(True),
        )
    ) or 0
    contact_count = session.scalar(
        select(func.count()).select_from(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == item.id,
            WhatsAppAudienceMember.target_type == "contact",
            WhatsAppAudienceMember.enabled.is_(True),
        )
    ) or 0
    disabled_member_count = session.scalar(
        select(func.count()).select_from(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == item.id,
            WhatsAppAudienceMember.enabled.is_(False),
        )
    ) or 0
    profile_count = session.scalar(
        select(func.count()).select_from(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.audience_id == item.id,
            WhatsAppDispatchProfile.enabled.is_(True),
        )
    ) or 0
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "description": item.description,
        "enabled": item.enabled,
        "group_count": group_count,
        "contact_count": contact_count,
        "member_count": group_count + contact_count,
        "disabled_member_count": disabled_member_count,
        "configured_member_count": group_count + contact_count + disabled_member_count,
        "profile_count": profile_count,
        "updated_at": item.updated_at,
    }

def _validate_group_route(
    session: Session,
    data: AudienceMemberInput,
) -> Wing:
    if data.wing_id is None:
        raise HTTPException(status_code=422, detail="Select the wing authorized to use this group")
    wing = session.get(Wing, data.wing_id)
    if wing is None or not wing.active:
        raise HTTPException(status_code=422, detail="Select an active wing")
    try:
        area_id = uuid.UUID(data.route_scope_value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Select a valid hierarchy area") from exc
    area_valid = False
    if data.route_scope_key == "district":
        area = session.get(District, area_id)
        area_valid = area is not None and area.active and area.id == wing.district_id
    elif data.route_scope_key == "wing":
        area_valid = area_id == wing.id
    elif data.route_scope_key == "tehsil":
        area = session.get(Tehsil, area_id)
        area_valid = area is not None and area.active and area.district_id == wing.district_id
    elif data.route_scope_key == "markaz":
        area = session.get(Markaz, area_id)
        area_valid = area is not None and area.active and area.wing_id == wing.id
    if not area_valid:
        raise HTTPException(
            status_code=422,
            detail="The selected hierarchy area does not belong to the authorized wing",
        )
    return wing

def _authorize_directory_group(
    session: Session,
    *,
    account: WhatsAppAccount,
    group: WhatsAppDirectoryGroup,
    wing: Wing,
) -> WhatsAppGroup:
    configured = session.scalar(
        select(WhatsAppGroup).where(
            WhatsAppGroup.account_id == account.id,
            WhatsAppGroup.jid == group.jid,
        )
    )
    if configured is None:
        configured = WhatsAppGroup(
            account_id=account.id,
            directory_group_id=group.id,
            wing_id=wing.id,
            name=group.name,
            jid=group.jid,
        )
    configured.directory_group_id = group.id
    configured.wing_id = wing.id
    configured.name = group.name
    configured.jid = group.jid
    configured.enabled = True
    configured.verified_at = utcnow()
    configured.updated_at = utcnow()
    session.add(configured)
    return configured

def dispatch_profile_dict(
    item: WhatsAppDispatchProfile,
    application: WhatsAppApplication,
    report_type: WhatsAppReportType,
    audience_item: WhatsAppAudience,
    account: WhatsAppAccount,
    wing: Wing | None,
    recipient_scope: WhatsAppRecipientScope | None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "report_type_id": str(item.report_type_id),
        "report_type_name": report_type.name,
        "audience_id": str(item.audience_id),
        "audience_name": audience_item.name,
        "account_id": str(item.account_id),
        "account_name": account.name,
        "template_id": str(item.template_id) if item.template_id else None,
        "recipient_channel": item.recipient_channel,
        "recipient_scope_id": str(item.recipient_scope_id) if item.recipient_scope_id else None,
        "recipient_scope_name": recipient_scope.name if recipient_scope else "Legacy / unscoped",
        "wing_id": str(item.wing_id) if item.wing_id else None,
        "wing_name": wing.name if wing else None,
        "delivery_mode": item.delivery_mode,
        "delivery_granularity": item.delivery_granularity,
        "require_approval": item.require_approval,
        "fallback_policy": item.fallback_policy,
        "max_retries": item.max_retries,
        "messages_per_minute": item.messages_per_minute,
        "presentation_policy": item.presentation_policy or {},
        "guided_setup": item.guided_setup,
        "enabled": item.enabled,
        "version": item.version,
        "notes": item.notes,
        "updated_at": item.updated_at,
    }

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
