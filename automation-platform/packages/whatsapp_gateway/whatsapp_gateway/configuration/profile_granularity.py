from __future__ import annotations

from sqlmodel import Session, select

from automation_core.time import utcnow
from whatsapp_gateway.models import WhatsAppApplication, WhatsAppDispatchProfile
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource


def ensure_dynamic_markaz_profile_granularity(
    session: Session, *, application: WhatsAppApplication,
) -> None:
    """Make Markaz jurisdiction the delivery unit while keeping audiences reusable."""
    audience_ids = set(session.scalars(
        select(WhatsAppAudienceSource.audience_id).where(
            WhatsAppAudienceSource.recipient_role == "aeo",
            WhatsAppAudienceSource.route_scope_key == "markaz",
            WhatsAppAudienceSource.enabled.is_(True),
        )
    ).all())
    if not audience_ids:
        return
    profiles = session.scalars(select(WhatsAppDispatchProfile).where(
        WhatsAppDispatchProfile.application_id == application.id,
        WhatsAppDispatchProfile.audience_id.in_(audience_ids),
        WhatsAppDispatchProfile.recipient_channel == "individual",
    )).all()
    for profile in profiles:
        if profile.delivery_granularity == "scope":
            continue
        profile.delivery_granularity = "scope"
        profile.version += 1
        profile.updated_at = utcnow()
        session.add(profile)


__all__ = ["ensure_dynamic_markaz_profile_granularity"]
