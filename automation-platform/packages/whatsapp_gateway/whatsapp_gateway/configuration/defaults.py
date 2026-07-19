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
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource
from whatsapp_gateway.configuration.profile_granularity import (
    ensure_dynamic_markaz_profile_granularity,
)
from whatsapp_gateway.rendering.antidengue.digest_models import (
    CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE,
)
from whatsapp_gateway.preview_service import (
    cleanup_unreferenced_preview_files,
    delete_preview_records,
)
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
        (
            "markaz_dormant_summary",
            "Markaz Dormant Summary",
            "Dormant-school summary resolved from active AEO Markaz jurisdictions.",
        ),
        ("wing_summary", "Wing summary", "Consolidated report for a wing audience."),
        (
            "hotspot_distance_activity",
            "Hotspot distance review",
            "Activities selected by published distance and submission-time review rules.",
        ),
        (
            "simple_activity_timing",
            "Simple activity timing review",
            "Before/after activity pairs below the published minimum time interval.",
        ),
        (
            "consolidated_action_digest",
            "Consolidated Action Digest",
            "One concise school-level digest and workbook covering dormant, distance and timing issues.",
        ),
    ],
    "crm": [
        ("filtered_workbook", "Filtered workbook", "CRM workbook filtered for an audience."),
        ("filtered_pdf", "Filtered PDF", "CRM PDF filtered for an audience."),
    ],
    "pmdu": [
        ("performance_report", "Performance report", "PMDU performance report for an audience."),
    ],
}


def ensure_dynamic_aeo_markaz_profile(
    session: Session,
    *,
    application: WhatsAppApplication,
    account: WhatsAppAccount,
) -> None:
    """Install the canonical dynamic MEE AEO route when the MEE wing exists."""
    wing = session.scalar(
        select(Wing).where(
            Wing.active.is_(True),
            or_(Wing.code == "DEO MEE", Wing.name == "DEO MEE"),
        )
    )
    if wing is None:
        return
    report_type = session.scalar(select(WhatsAppReportType).where(
        WhatsAppReportType.application_id == application.id,
        WhatsAppReportType.key == "markaz_dormant_summary",
    ))
    recipient_scope = session.scalar(select(WhatsAppRecipientScope).where(
        WhatsAppRecipientScope.application_id == application.id,
        WhatsAppRecipientScope.channel == "individual",
        WhatsAppRecipientScope.key == "aeo",
    ))
    if report_type is None or recipient_scope is None:
        return
    audience = session.scalar(select(WhatsAppAudience).where(
        WhatsAppAudience.application_id == application.id,
        WhatsAppAudience.key == "mee_aeo_markaz_dynamic",
    ))
    if audience is None:
        audience = WhatsAppAudience(
            application_id=application.id,
            key="mee_aeo_markaz_dynamic",
            name="MEE AEOs by active Markaz jurisdiction",
            description="Resolved from active Master Data AEO jurisdictions at preview time.",
        )
        session.add(audience)
        session.flush()
    source = session.scalar(select(WhatsAppAudienceSource).where(
        WhatsAppAudienceSource.audience_id == audience.id,
        WhatsAppAudienceSource.source_type == "master_data_jurisdictions",
        WhatsAppAudienceSource.recipient_role == "aeo",
        WhatsAppAudienceSource.wing_id == wing.id,
        WhatsAppAudienceSource.route_scope_key == "markaz",
    ))
    if source is None:
        session.add(WhatsAppAudienceSource(
            audience_id=audience.id,
            recipient_role="aeo",
            wing_id=wing.id,
            route_scope_key="markaz",
            aggregate_by_recipient=True,
        ))
    template = session.scalar(select(WhatsAppTemplate).where(
        WhatsAppTemplate.key == "antidengue_markaz_dormant_aeo_v1"
    ))
    if template is None:
        template = WhatsAppTemplate(
            application_id=application.id,
            report_type_id=report_type.id,
            recipient_scope_id=recipient_scope.id,
            recipient_channel="individual",
            key="antidengue_markaz_dormant_aeo_v1",
            name="AEO Markaz dormant report",
            category="report",
            body="{{report_body}}",
        )
        session.add(template)
        session.flush()
    profile = session.scalar(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.key == "mee_aeo_markaz_dormant_personal",
    ))
    if profile is None:
        session.add(WhatsAppDispatchProfile(
            application_id=application.id,
            key="mee_aeo_markaz_dormant_personal",
            name="Markaz Dormant Summary — MEE — AEO Personal",
            report_type_id=report_type.id,
            audience_id=audience.id,
            account_id=account.id,
            template_id=template.id,
            recipient_scope_id=recipient_scope.id,
            recipient_channel="individual",
            wing_id=wing.id,
            delivery_mode="individuals",
            delivery_granularity="scope",
            require_approval=True,
            presentation_policy={
                "message_style": "detailed",
                "attachment_mode": "image_excel",
                "image_content": "details",
            },
            guided_setup=True,
            owns_audience=True,
            owns_template=True,
            notes="Dynamic Master Data jurisdiction audience; resolved and frozen per preview.",
        ))


def ensure_consolidated_digest_profiles(
    session: Session,
    *,
    application: WhatsAppApplication,
) -> None:
    """Create additive digest routes that reuse each live dormant audience."""
    report_types = {
        item.id: item for item in session.scalars(select(WhatsAppReportType).where(
            WhatsAppReportType.application_id == application.id
        )).all()
    }
    digest_report = next(
        (item for item in report_types.values() if item.key == "consolidated_action_digest"),
        None,
    )
    if digest_report is None:
        return
    source_keys = {"wing_summary", "tehsil_dormant_summary", "markaz_dormant_summary"}
    sources = [
        profile for profile in session.scalars(select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.application_id == application.id,
            WhatsAppDispatchProfile.enabled.is_(True),
        )).all()
        if (report_types.get(profile.report_type_id) and report_types[profile.report_type_id].key in source_keys)
    ]
    existing = list(session.scalars(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.report_type_id == digest_report.id,
    )).all())
    for profile in existing:
        if profile.recipient_channel == "individual" and profile.delivery_granularity != "scope":
            profile.delivery_granularity = "scope"
            profile.version += 1
            profile.updated_at = utcnow()
            session.add(profile)
        template = session.get(WhatsAppTemplate, profile.template_id) if profile.template_id else None
        if template is not None:
            original_body = template.body
            if original_body.strip() == "{{report_body}}":
                template.body = CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE
            elif "*DETAILS BY MARKAZ*" in original_body and "{{detail_heading}}" not in original_body:
                # Structural v1-to-v2 upgrade: preserve every operator edit and
                # replace only the scope-inaccurate fixed heading.
                template.body = original_body.replace(
                    "*DETAILS BY MARKAZ*", "*{{detail_heading}}*", 1
                )
            if template.body != original_body:
                session.add(template)
    linked_source_ids = {
        str((profile.presentation_policy or {}).get("digest_source_profile_id") or "")
        for profile in existing
    }
    for source in sources:
        if str(source.id) in linked_source_ids:
            continue
        fragment = source.id.hex[:12]
        template = WhatsAppTemplate(
            application_id=application.id,
            report_type_id=digest_report.id,
            recipient_scope_id=source.recipient_scope_id,
            recipient_channel=source.recipient_channel,
            key=f"antidengue_action_digest_{fragment}",
            name=f"{source.name} — Action digest message"[:200],
            category="report",
            body=CONSOLIDATED_DIGEST_DEFAULT_TEMPLATE,
        )
        session.add(template)
        session.flush()
        base_key = re.sub(r"[^a-z0-9_]+", "_", f"{source.key}_action_digest".lower()).strip("_")[:100]
        key, suffix = base_key, 2
        while session.scalar(select(WhatsAppDispatchProfile.id).where(
            WhatsAppDispatchProfile.application_id == application.id,
            WhatsAppDispatchProfile.key == key,
        )):
            ending = f"_{suffix}"
            key = f"{base_key[:100-len(ending)]}{ending}"
            suffix += 1
        session.add(WhatsAppDispatchProfile(
            application_id=application.id, key=key,
            name=f"{source.name} — Consolidated Action Digest"[:200],
            report_type_id=digest_report.id, audience_id=source.audience_id,
            account_id=source.account_id, template_id=template.id,
            recipient_scope_id=source.recipient_scope_id,
            recipient_channel=source.recipient_channel, wing_id=source.wing_id,
            delivery_mode=source.delivery_mode, require_approval=source.require_approval,
            delivery_granularity="scope" if source.recipient_channel == "individual" else "recipient",
            fallback_policy=source.fallback_policy, max_retries=source.max_retries,
            messages_per_minute=source.messages_per_minute,
            presentation_policy={
                "message_style": "summary", "attachment_mode": "excel",
                "image_content": "summary", "digest_source_profile_id": str(source.id),
            },
            guided_setup=True, owns_audience=False, owns_template=True,
            notes=f"Additive consolidated digest reusing the live audience from {source.name}.",
        ))
        linked_source_ids.add(str(source.id))




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


def ensure_defaults(session: Session) -> tuple[WhatsAppAccount, WhatsAppSettings]:
    config = get_settings()
    account = session.scalar(
        select(WhatsAppAccount).where(WhatsAppAccount.worker_key == "default")
    )
    if account is None:
        account = WhatsAppAccount(
            name="Primary WhatsApp",
            worker_key="default",
            command_subject=config.whatsapp_command_subject,
            health_subject=config.whatsapp_health_subject,
        )
        session.add(account)
        session.flush()
    applications: dict[str, WhatsAppApplication] = {}
    for key, (name, description) in DEFAULT_APPLICATIONS.items():
        application = session.scalar(
            select(WhatsAppApplication).where(WhatsAppApplication.key == key)
        )
        if application is None:
            application = WhatsAppApplication(
                key=key,
                name=name,
                description=description,
            )
            session.add(application)
            session.flush()
        applications[key] = application
    # Configuration endpoints are commonly loaded in parallel by the UI. Lock
    # the stable application rows before seeding child report types/scopes so
    # two requests cannot both observe a missing default and race the unique
    # (application_id, key) constraint.
    session.flush()
    for application in sorted(applications.values(), key=lambda item: str(item.id)):
        session.execute(
            select(WhatsAppApplication.id)
            .where(WhatsAppApplication.id == application.id)
            .with_for_update()
        )
    for application_key, definitions in DEFAULT_REPORT_TYPES.items():
        application = applications[application_key]
        for key, name, description in definitions:
            existing = session.scalar(
                select(WhatsAppReportType).where(
                    WhatsAppReportType.application_id == application.id,
                    WhatsAppReportType.key == key,
                )
            )
            if existing is None:
                session.add(
                    WhatsAppReportType(
                        application_id=application.id,
                        key=key,
                        name=name,
                        description=description,
                    )
                )
    session.flush()
    for application_key, definitions in DEFAULT_RECIPIENT_SCOPES.items():
        application = applications[application_key]
        for channel, key, name, hierarchy_level, description in definitions:
            existing = session.scalar(
                select(WhatsAppRecipientScope).where(
                    WhatsAppRecipientScope.application_id == application.id,
                    WhatsAppRecipientScope.channel == channel,
                    WhatsAppRecipientScope.key == key,
                )
            )
            if existing is None:
                session.add(
                    WhatsAppRecipientScope(
                        application_id=application.id,
                        channel=channel,
                        key=key,
                        name=name,
                        hierarchy_level=hierarchy_level,
                        description=description,
                    )
                )
    session.flush()
    ensure_dynamic_aeo_markaz_profile(
        session, application=applications["antidengue"], account=account
    )
    session.flush()
    ensure_consolidated_digest_profiles(
        session, application=applications["antidengue"]
    )
    session.flush()
    ensure_dynamic_markaz_profile_granularity(
        session, application=applications["antidengue"]
    )
    settings = session.scalar(
        select(WhatsAppSettings).where(
            WhatsAppSettings.default_account_id == account.id
        )
    )
    if settings is None:
        settings = WhatsAppSettings(default_account_id=account.id)
        session.add(settings)
    if session.scalar(select(func.count()).select_from(WhatsAppTemplate)) == 0:
        session.add(
            WhatsAppTemplate(
                key="test_message",
                name="Connection test",
                category="system",
                body="WhatsApp gateway test from Deomee Automation Platform.",
            )
        )
    session.commit()
    session.refresh(account)
    session.refresh(settings)
    return account, settings
