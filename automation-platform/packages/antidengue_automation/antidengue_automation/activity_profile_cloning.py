from __future__ import annotations

import re

from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.configuration.dynamic_audiences import active_audience_sources
from whatsapp_gateway.models import (
    WhatsAppApplication,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
    WhatsAppTemplate,
)

ACTIVITY_REPORT_KEYS = {
    "hotspot": "hotspot_distance_activity",
    "simple": "simple_activity_timing",
}
DORMANT_SOURCE_REPORT_KEYS = {
    "markaz_dormant_summary",
    "tehsil_dormant_summary",
    "wing_summary",
    "school_activity",
}


def _available_profile_key(session: Session, application_id, source_key: str, activity: str) -> str:
    base = re.sub(r"[^a-z0-9_]+", "_", f"{source_key}_{activity}".lower()).strip("_")[:90]
    candidate, suffix = base, 2
    while session.scalar(select(WhatsAppDispatchProfile.id).where(
        WhatsAppDispatchProfile.application_id == application_id,
        WhatsAppDispatchProfile.key == candidate,
    )):
        ending = f"_{suffix}"
        candidate = f"{base[:100 - len(ending)]}{ending}"
        suffix += 1
    return candidate


def _activity_template(
    session: Session,
    *,
    application: WhatsAppApplication,
    report_type: WhatsAppReportType,
    source: WhatsAppDispatchProfile,
) -> WhatsAppTemplate:
    scope_token = str(source.recipient_scope_id or "all").replace("-", "")[:12]
    key = f"antidengue_{report_type.key}_{source.recipient_channel}_{scope_token}"[:100]
    template = session.scalar(select(WhatsAppTemplate).where(WhatsAppTemplate.key == key))
    if template is not None:
        if (
            not template.enabled
            or template.application_id != application.id
            or template.report_type_id != report_type.id
            or template.recipient_scope_id != source.recipient_scope_id
            or template.recipient_channel != source.recipient_channel
        ):
            raise ValueError("The automatic activity template key conflicts with incompatible configuration")
        return template
    template = WhatsAppTemplate(
        application_id=application.id,
        report_type_id=report_type.id,
        recipient_scope_id=source.recipient_scope_id,
        recipient_channel=source.recipient_channel,
        key=key,
        name=f"{report_type.name} — {source.recipient_channel}",
        category="report",
        body="{{report_body}}",
    )
    session.add(template)
    session.flush()
    return template


def is_activity_clone_source(session: Session, source: WhatsAppDispatchProfile) -> bool:
    source_report = session.get(WhatsAppReportType, source.report_type_id)
    if not source.enabled or source_report is None or source_report.key not in DORMANT_SOURCE_REPORT_KEYS:
        return False
    if source.recipient_channel == "group":
        return True
    dynamic_sources = active_audience_sources(session, source.audience_id)
    return (
        source.recipient_channel == "individual"
        and bool(dynamic_sources)
        and all(item.recipient_role == "aeo" and item.route_scope_key == "markaz" for item in dynamic_sources)
    )


def clone_activity_profile(
    session: Session,
    *,
    source: WhatsAppDispatchProfile,
    activity: str,
    name: str,
) -> WhatsAppDispatchProfile:
    name = name.strip()
    if len(name) < 2:
        raise ValueError("Enter a profile name with at least two characters")
    report_key = ACTIVITY_REPORT_KEYS.get(activity)
    if report_key is None:
        raise ValueError("Select Hotspot distance or Simple Activity timing")
    application = session.get(WhatsAppApplication, source.application_id)
    source_report = session.get(WhatsAppReportType, source.report_type_id)
    if application is None or application.key != "antidengue":
        raise ValueError("Only AntiDengue routing profiles can be cloned")
    if source_report is None or source_report.key not in DORMANT_SOURCE_REPORT_KEYS:
        raise ValueError("Clone activity routing from a dormant-user profile")
    if not is_activity_clone_source(session, source):
        raise ValueError("This profile does not have a supported group or AEO-by-Markaz audience")
    report_type = session.scalar(select(WhatsAppReportType).where(
        WhatsAppReportType.application_id == application.id,
        WhatsAppReportType.key == report_key,
        WhatsAppReportType.enabled.is_(True),
    ))
    if report_type is None:
        raise ValueError("The requested activity report type is unavailable")
    linked = [
        item for item in session.scalars(select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.application_id == application.id,
            WhatsAppDispatchProfile.report_type_id == report_type.id,
        )).all()
        if str((item.presentation_policy or {}).get("linked_source_profile_id") or "") == str(source.id)
    ]
    if linked:
        raise ValueError(f"{source.name} already has a {report_type.name} clone")
    if session.scalar(select(WhatsAppDispatchProfile.id).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.name == name,
    )):
        raise ValueError("A routing profile with this name already exists")
    template = _activity_template(
        session, application=application, report_type=report_type, source=source
    )
    policy = dict(source.presentation_policy or {})
    policy.update({
        "message_style": "summary",
        "attachment_mode": "excel",
        "image_content": "summary",
        "linked_source_profile_id": str(source.id),
    })
    profile = WhatsAppDispatchProfile(
        application_id=application.id,
        key=_available_profile_key(session, application.id, source.key, activity),
        name=name,
        report_type_id=report_type.id,
        audience_id=source.audience_id,
        account_id=source.account_id,
        template_id=template.id,
        recipient_scope_id=source.recipient_scope_id,
        recipient_channel=source.recipient_channel,
        wing_id=source.wing_id,
        delivery_mode="groups" if source.recipient_channel == "group" else "individuals",
        delivery_granularity=(
            "scope" if source.recipient_channel == "individual"
            else source.delivery_granularity
        ),
        require_approval=source.require_approval,
        fallback_policy=source.fallback_policy,
        max_retries=source.max_retries,
        messages_per_minute=source.messages_per_minute,
        presentation_policy=policy,
        guided_setup=True,
        owns_audience=False,
        owns_template=False,
        notes=f"Reuses the live audience and jurisdiction routing from {source.name}.",
        created_at=utcnow(),
        updated_at=utcnow(),
    )
    session.add(profile)
    session.flush()
    return profile


__all__ = [
    "ACTIVITY_REPORT_KEYS",
    "DORMANT_SOURCE_REPORT_KEYS",
    "clone_activity_profile",
    "is_activity_clone_source",
]
