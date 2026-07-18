from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlmodel import Session, select

from automation_core.database import get_session
from automation_core.time import utcnow
from master_data.models import Wing
from whatsapp_gateway.api_schemas import AudienceSourceInput
from whatsapp_gateway.configuration.dynamic_audiences import resolve_audience_source
from whatsapp_gateway.models import WhatsAppAudience
from whatsapp_gateway.persistence.audience_sources import WhatsAppAudienceSource

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])


def source_dict(session: Session, item: WhatsAppAudienceSource) -> dict[str, Any]:
    wing = session.get(Wing, item.wing_id)
    resolved = resolve_audience_source(session, source=item)
    return {
        "id": str(item.id),
        "source_type": item.source_type,
        "recipient_role": item.recipient_role,
        "wing_id": str(item.wing_id),
        "wing_name": wing.name if wing else "Missing wing",
        "route_scope_key": item.route_scope_key,
        "aggregate_by_recipient": item.aggregate_by_recipient,
        "enabled": item.enabled,
        "resolved_recipient_count": len(resolved),
        "resolved_scope_count": sum(len(member.scope_ids) for member in resolved),
        "resolved_school_count": len({value for member in resolved for value in member.school_ids}),
    }


@router.get("/audiences/{audience_id}/sources")
def audience_sources(
    audience_id: uuid.UUID, session: Session = Depends(get_session)
) -> list[dict[str, Any]]:
    if session.get(WhatsAppAudience, audience_id) is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    records = session.scalars(
        select(WhatsAppAudienceSource)
        .where(WhatsAppAudienceSource.audience_id == audience_id)
        .order_by(WhatsAppAudienceSource.created_at)
    ).all()
    return [source_dict(session, item) for item in records]


@router.post("/audiences/{audience_id}/sources", status_code=status.HTTP_201_CREATED)
def save_audience_source(
    audience_id: uuid.UUID,
    data: AudienceSourceInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    audience = session.get(WhatsAppAudience, audience_id)
    wing = session.get(Wing, data.wing_id)
    if audience is None or not audience.enabled:
        raise HTTPException(status_code=404, detail="Enabled audience not found")
    if wing is None or not wing.active:
        raise HTTPException(status_code=422, detail="Select an active wing")
    expected_scope = "markaz" if data.recipient_role == "aeo" else "tehsil"
    if data.route_scope_key != expected_scope:
        raise HTTPException(
            status_code=422,
            detail=f"{data.recipient_role.upper()} sources require {expected_scope} scope",
        )
    duplicate = session.scalar(
        select(WhatsAppAudienceSource).where(
            WhatsAppAudienceSource.audience_id == audience_id,
            WhatsAppAudienceSource.source_type == data.source_type,
            WhatsAppAudienceSource.recipient_role == data.recipient_role,
            WhatsAppAudienceSource.wing_id == data.wing_id,
            WhatsAppAudienceSource.route_scope_key == data.route_scope_key,
            WhatsAppAudienceSource.id != data.id if data.id else True,
        )
    )
    if duplicate is not None:
        raise HTTPException(
            status_code=409,
            detail="This Master Data jurisdiction source is already attached to the audience",
        )
    item = session.get(WhatsAppAudienceSource, data.id) if data.id else None
    if data.id and (item is None or item.audience_id != audience_id):
        raise HTTPException(status_code=404, detail="Audience source not found")
    item = item or WhatsAppAudienceSource(
        audience_id=audience_id,
        recipient_role=data.recipient_role,
        wing_id=data.wing_id,
        route_scope_key=data.route_scope_key,
    )
    item.source_type = data.source_type
    item.recipient_role = data.recipient_role
    item.wing_id = data.wing_id
    item.route_scope_key = data.route_scope_key
    item.aggregate_by_recipient = data.aggregate_by_recipient
    item.enabled = data.enabled
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    session.refresh(item)
    return source_dict(session, item)


@router.delete("/audiences/{audience_id}/sources/{source_id}")
def delete_audience_source(
    audience_id: uuid.UUID,
    source_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    item = session.get(WhatsAppAudienceSource, source_id)
    if item is None or item.audience_id != audience_id:
        raise HTTPException(status_code=404, detail="Audience source not found")
    session.delete(item)
    session.commit()
    return {"id": str(source_id), "deleted": True}
