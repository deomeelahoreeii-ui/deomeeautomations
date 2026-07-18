from __future__ import annotations

import re
import uuid
from typing import Iterable
from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.models import WhatsAppApplication, WhatsAppDispatchProfile, WhatsAppReportType, WhatsAppTemplate

SIMPLE_ACTIVITY_REPORT_KEY = "simple_activity_timing"
SOURCE_REPORT_KEYS = {"wing_summary", "tehsil_dormant_summary", "school_activity"}


def _source_id(profile: WhatsAppDispatchProfile) -> str:
    return str((profile.presentation_policy or {}).get("linked_source_profile_id") or "")


def _configuration(session: Session) -> tuple[WhatsAppApplication, WhatsAppReportType]:
    application = session.scalar(select(WhatsAppApplication).where(WhatsAppApplication.key == "antidengue"))
    if application is None: ensure_defaults(session); application = session.scalar(select(WhatsAppApplication).where(WhatsAppApplication.key == "antidengue"))
    report = session.scalar(select(WhatsAppReportType).where(WhatsAppReportType.application_id == application.id, WhatsAppReportType.key == SIMPLE_ACTIVITY_REPORT_KEY)) if application else None
    if report is None: ensure_defaults(session); report = session.scalar(select(WhatsAppReportType).where(WhatsAppReportType.application_id == application.id, WhatsAppReportType.key == SIMPLE_ACTIVITY_REPORT_KEY)) if application else None
    if application is None or report is None or not report.enabled:
        raise ValueError("Simple Activity timing WhatsApp report type is unavailable")
    return application, report


def _eligible(session: Session) -> list[WhatsAppDispatchProfile]:
    application, _ = _configuration(session)
    types = {item.id: item.key for item in session.exec(select(WhatsAppReportType).where(WhatsAppReportType.application_id == application.id)).all()}
    return [item for item in session.exec(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.enabled == True,  # noqa: E712
        WhatsAppDispatchProfile.recipient_channel == "group",
    )).all() if types.get(item.report_type_id) in SOURCE_REPORT_KEYS]


def simple_activity_routing_status(session: Session) -> dict:
    application, report = _configuration(session)
    linked = session.exec(select(WhatsAppDispatchProfile).where(WhatsAppDispatchProfile.application_id == application.id, WhatsAppDispatchProfile.report_type_id == report.id)).all()
    by_source = {_source_id(item): item for item in linked if _source_id(item)}
    items = []
    for source in _eligible(session):
        target = by_source.get(str(source.id))
        items.append({
            "source_profile_id": str(source.id), "source_profile_name": source.name,
            "audience_id": str(source.audience_id), "linked": target is not None,
            "simple_activity_profile_id": str(target.id) if target else None,
            "simple_activity_profile_name": target.name if target else None,
            "enabled": bool(target.enabled) if target else False,
        })
    return {"report_type_id": str(report.id), "eligible_count": len(items), "linked_count": sum(item["linked"] for item in items), "enabled_count": sum(item["enabled"] for item in items), "items": items}


def link_simple_activity_routes(session: Session, source_profile_ids: Iterable[uuid.UUID | str] | None = None) -> dict:
    application, report = _configuration(session)
    requested = {str(uuid.UUID(str(value))) for value in source_profile_ids or []}
    eligible = _eligible(session)
    if requested:
        eligible = [item for item in eligible if str(item.id) in requested]
        if requested.difference(str(item.id) for item in eligible):
            raise ValueError("One or more selected profiles cannot be reused for Simple Activity timing delivery")
    existing = session.exec(select(WhatsAppDispatchProfile).where(WhatsAppDispatchProfile.application_id == application.id, WhatsAppDispatchProfile.report_type_id == report.id)).all()
    by_source = {_source_id(item): item for item in existing if _source_id(item)}
    created = 0
    for source in eligible:
        if str(source.id) in by_source: continue
        scope_suffix = str(source.recipient_scope_id or "all").replace("-", "")[:12]
        template_key = f"antidengue_simple_timing_{scope_suffix}"
        template = session.scalar(select(WhatsAppTemplate).where(WhatsAppTemplate.key == template_key))
        if template is None:
            template = WhatsAppTemplate(application_id=application.id, report_type_id=report.id, recipient_scope_id=source.recipient_scope_id, recipient_channel="group", key=template_key, name="Simple Activity timing review", category="report", body="{{report_body}}")
            session.add(template); session.flush()
        base = re.sub(r"[^a-z0-9_]+", "_", f"{source.key}_simple_activity_timing".lower()).strip("_")[:110]
        key, suffix = base, 2
        while session.scalar(select(WhatsAppDispatchProfile.id).where(WhatsAppDispatchProfile.application_id == application.id, WhatsAppDispatchProfile.key == key)):
            key, suffix = f"{base[:105]}_{suffix}", suffix + 1
        policy = dict(source.presentation_policy or {})
        policy.update({"message_style": "summary", "attachment_mode": "excel", "image_content": "summary", "linked_source_profile_id": str(source.id)})
        session.add(WhatsAppDispatchProfile(
            application_id=application.id, key=key, name=f"{source.name} – Simple activity timing review",
            report_type_id=report.id, audience_id=source.audience_id, account_id=source.account_id,
            template_id=template.id, recipient_scope_id=source.recipient_scope_id, recipient_channel="group",
            wing_id=source.wing_id, delivery_mode="groups", require_approval=source.require_approval,
            fallback_policy=source.fallback_policy, max_retries=source.max_retries,
            messages_per_minute=source.messages_per_minute, presentation_policy=policy,
            guided_setup=True, owns_audience=False, owns_template=False,
            notes=f"Reuses audience and hierarchy routing from {source.name}.", created_at=utcnow(), updated_at=utcnow(),
        )); created += 1
    session.commit(); result = simple_activity_routing_status(session); result["created_count"] = created
    return result


def configure_simple_activity_routes(session: Session, source_profile_ids: Iterable[uuid.UUID | str]) -> dict:
    requested = {str(uuid.UUID(str(value))) for value in source_profile_ids}
    eligible_ids = {str(item.id) for item in _eligible(session)}
    if missing := requested.difference(eligible_ids):
        raise ValueError("Invalid Simple Activity source profile(s): " + ", ".join(sorted(missing)))
    if requested: link_simple_activity_routes(session, requested)
    application, report = _configuration(session)
    changed = 0
    for profile in session.exec(select(WhatsAppDispatchProfile).where(WhatsAppDispatchProfile.application_id == application.id, WhatsAppDispatchProfile.report_type_id == report.id)).all():
        source = _source_id(profile)
        if source not in eligible_ids: continue
        enabled = source in requested
        if bool(profile.enabled) != enabled:
            profile.enabled = enabled; profile.updated_at = utcnow(); session.add(profile); changed += 1
    session.commit(); result = simple_activity_routing_status(session); result["changed_count"] = changed
    return result


def expand_simple_activity_profile_ids(session: Session, profile_ids: Iterable[uuid.UUID | str]) -> list[uuid.UUID]:
    normalized = {uuid.UUID(str(value)) for value in profile_ids}
    _, report = _configuration(session)
    if normalized.intersection(item.id for item in _eligible(session)):
        normalized.update(item.id for item in session.exec(select(WhatsAppDispatchProfile).where(WhatsAppDispatchProfile.report_type_id == report.id, WhatsAppDispatchProfile.enabled == True)).all())  # noqa: E712
    return sorted(normalized, key=str)


__all__ = ["SIMPLE_ACTIVITY_REPORT_KEY", "configure_simple_activity_routes", "expand_simple_activity_profile_ids", "link_simple_activity_routes", "simple_activity_routing_status"]
