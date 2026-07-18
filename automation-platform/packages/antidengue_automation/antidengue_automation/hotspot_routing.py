from __future__ import annotations

import re
import uuid
from typing import Iterable

from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.models import (
    WhatsAppApplication,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
    WhatsAppTemplate,
)

HOTSPOT_REPORT_KEY = "hotspot_distance_activity"
ELIGIBLE_SOURCE_REPORT_KEYS = {"wing_summary", "tehsil_dormant_summary", "school_activity"}


def _linked_source_id(profile: WhatsAppDispatchProfile) -> str:
    return str((profile.presentation_policy or {}).get("linked_source_profile_id") or "")


def _configuration(session: Session) -> tuple[WhatsAppApplication, WhatsAppReportType]:
    application = session.scalar(select(WhatsAppApplication).where(WhatsAppApplication.key == "antidengue"))
    if application is None:
        ensure_defaults(session)
        application = session.scalar(select(WhatsAppApplication).where(WhatsAppApplication.key == "antidengue"))
    if application is None:
        raise ValueError("AntiDengue WhatsApp application is unavailable")
    report_type = session.scalar(select(WhatsAppReportType).where(
        WhatsAppReportType.application_id == application.id,
        WhatsAppReportType.key == HOTSPOT_REPORT_KEY,
    ))
    if report_type is None:
        ensure_defaults(session)
        report_type = session.scalar(select(WhatsAppReportType).where(
            WhatsAppReportType.application_id == application.id,
            WhatsAppReportType.key == HOTSPOT_REPORT_KEY,
        ))
    if report_type is None or not report_type.enabled:
        raise ValueError("Hotspot-distance WhatsApp report type is unavailable")
    return application, report_type


def _eligible_profiles(session: Session) -> list[WhatsAppDispatchProfile]:
    application, _ = _configuration(session)
    report_types = {
        item.id: item.key
        for item in session.exec(select(WhatsAppReportType).where(
            WhatsAppReportType.application_id == application.id
        )).all()
    }
    return [
        profile
        for profile in session.exec(select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.application_id == application.id,
            WhatsAppDispatchProfile.enabled == True,  # noqa: E712
            WhatsAppDispatchProfile.recipient_channel == "group",
        )).all()
        if report_types.get(profile.report_type_id) in ELIGIBLE_SOURCE_REPORT_KEYS
    ]


def hotspot_routing_status(session: Session) -> dict:
    application, hotspot_type = _configuration(session)
    linked = session.exec(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.report_type_id == hotspot_type.id,
    )).all()
    linked_by_source = {_linked_source_id(item): item for item in linked if _linked_source_id(item)}
    items = []
    for source in _eligible_profiles(session):
        target = linked_by_source.get(str(source.id))
        items.append({
            "source_profile_id": str(source.id),
            "source_profile_name": source.name,
            "audience_id": str(source.audience_id),
            "linked": target is not None,
            "hotspot_profile_id": str(target.id) if target else None,
            "hotspot_profile_name": target.name if target else None,
            "enabled": bool(target.enabled) if target else False,
        })
    return {
        "report_type_id": str(hotspot_type.id),
        "eligible_count": len(items),
        "linked_count": sum(item["linked"] for item in items),
        "enabled_count": sum(item["enabled"] for item in items),
        "items": items,
    }


def link_hotspot_routes(
    session: Session, source_profile_ids: Iterable[uuid.UUID | str] | None = None
) -> dict:
    application, hotspot_type = _configuration(session)
    requested = {str(uuid.UUID(str(value))) for value in source_profile_ids or []}
    eligible = _eligible_profiles(session)
    if requested:
        eligible = [item for item in eligible if str(item.id) in requested]
        missing = requested.difference(str(item.id) for item in eligible)
        if missing:
            raise ValueError("One or more selected profiles cannot be reused for hotspot-distance delivery")
    existing = session.exec(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.report_type_id == hotspot_type.id,
    )).all()
    linked_by_source = {_linked_source_id(item): item for item in existing if _linked_source_id(item)}
    created = 0
    for source in eligible:
        if str(source.id) in linked_by_source:
            continue
        scope_suffix = str(source.recipient_scope_id or "all").replace("-", "")[:12]
        template_key = f"antidengue_hotspot_review_{scope_suffix}"
        template = session.scalar(select(WhatsAppTemplate).where(WhatsAppTemplate.key == template_key))
        if template is None:
            template = WhatsAppTemplate(
                application_id=application.id,
                report_type_id=hotspot_type.id,
                recipient_scope_id=source.recipient_scope_id,
                recipient_channel="group",
                key=template_key,
                name="Hotspot distance review",
                category="report",
                body="{{report_body}}",
            )
            session.add(template)
            session.flush()
        base_key = re.sub(r"[^a-z0-9_]+", "_", f"{source.key}_hotspot_distance".lower()).strip("_")[:110]
        key = base_key
        suffix = 2
        while session.scalar(select(WhatsAppDispatchProfile.id).where(
            WhatsAppDispatchProfile.application_id == application.id,
            WhatsAppDispatchProfile.key == key,
        )):
            key = f"{base_key[:105]}_{suffix}"
            suffix += 1
        policy = dict(source.presentation_policy or {})
        policy.update({
            "message_style": "summary",
            "attachment_mode": "excel",
            "image_content": "summary",
            "linked_source_profile_id": str(source.id),
        })
        session.add(WhatsAppDispatchProfile(
            application_id=application.id,
            key=key,
            name=f"{source.name} – Hotspot distance review",
            report_type_id=hotspot_type.id,
            audience_id=source.audience_id,
            account_id=source.account_id,
            template_id=template.id,
            recipient_scope_id=source.recipient_scope_id,
            recipient_channel="group",
            wing_id=source.wing_id,
            delivery_mode="groups",
            require_approval=source.require_approval,
            fallback_policy=source.fallback_policy,
            max_retries=source.max_retries,
            messages_per_minute=source.messages_per_minute,
            presentation_policy=policy,
            guided_setup=True,
            owns_audience=False,
            owns_template=False,
            notes=f"Reuses audience and hierarchy routing from {source.name}.",
            created_at=utcnow(),
            updated_at=utcnow(),
        ))
        created += 1
    session.commit()
    result = hotspot_routing_status(session)
    result["created_count"] = created
    return result


def configure_hotspot_routes(
    session: Session, source_profile_ids: Iterable[uuid.UUID | str]
) -> dict:
    """Make the submitted source-route set the active hotspot routing policy."""
    requested = {str(uuid.UUID(str(value))) for value in source_profile_ids}
    eligible = _eligible_profiles(session)
    eligible_ids = {str(item.id) for item in eligible}
    if missing := requested.difference(eligible_ids):
        raise ValueError(
            "One or more selected profiles cannot be used for hotspot-distance delivery: "
            + ", ".join(sorted(missing))
        )

    if requested:
        link_hotspot_routes(session, requested)

    application, hotspot_type = _configuration(session)
    linked = session.exec(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.report_type_id == hotspot_type.id,
    )).all()
    changed = 0
    for profile in linked:
        source_id = _linked_source_id(profile)
        if not source_id or source_id not in eligible_ids:
            continue
        should_enable = source_id in requested
        if bool(profile.enabled) == should_enable:
            continue
        profile.enabled = should_enable
        profile.updated_at = utcnow()
        session.add(profile)
        changed += 1
    session.commit()
    result = hotspot_routing_status(session)
    result["changed_count"] = changed
    return result


def expand_hotspot_profile_ids(
    session: Session, profile_ids: Iterable[uuid.UUID | str]
) -> list[uuid.UUID]:
    normalized = {uuid.UUID(str(value)) for value in profile_ids}
    _, hotspot_type = _configuration(session)
    linked = session.exec(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.report_type_id == hotspot_type.id,
        WhatsAppDispatchProfile.enabled == True,  # noqa: E712
    )).all()
    # Activity Rules owns the hotspot delivery-route selection. Once a run
    # includes any eligible dormant-report source, include every hotspot route
    # enabled there. This keeps the dashboard profile picker concerned with
    # dormant reports and prevents a wing selection from silently dropping the
    # configured tehsil hotspot recipients.
    eligible_source_ids = {item.id for item in _eligible_profiles(session)}
    if normalized.intersection(eligible_source_ids):
        normalized.update(profile.id for profile in linked)
    return sorted(normalized, key=str)


__all__ = [
    "HOTSPOT_REPORT_KEY", "configure_hotspot_routes", "expand_hotspot_profile_ids",
    "hotspot_routing_status", "link_hotspot_routes",
]
