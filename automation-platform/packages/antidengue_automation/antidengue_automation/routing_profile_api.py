from __future__ import annotations

import uuid
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from antidengue_automation.activity_profile_cloning import clone_activity_profile
from automation_core.database import get_session
from whatsapp_gateway.configuration.profile_serialization import dispatch_profile_dict
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
)
from master_data.models import Wing

router = APIRouter(prefix="/api/v1/antidengue/routing-profiles", tags=["antidengue-routing"])


class ActivityProfileCloneInput(BaseModel):
    name: str = Field(min_length=2, max_length=200)


@router.post("/{source_profile_id}/clone/{activity}", status_code=status.HTTP_201_CREATED)
def clone_profile_for_activity(
    source_profile_id: uuid.UUID,
    activity: Literal["hotspot", "simple"],
    data: ActivityProfileCloneInput,
    session: Session = Depends(get_session),
) -> dict:
    source = session.scalar(
        select(WhatsAppDispatchProfile)
        .where(WhatsAppDispatchProfile.id == source_profile_id)
        .with_for_update()
    )
    if source is None:
        raise HTTPException(status_code=404, detail="Dormant routing profile not found")
    try:
        profile = clone_activity_profile(
            session, source=source, activity=activity, name=data.name.strip()
        )
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    session.add(WhatsAppActivity(
        account_id=profile.account_id,
        event_type="dispatch_profile_cloned",
        level="info",
        message=f"Cloned {source.name} to {profile.name}",
        details={
            "source_profile_id": str(source.id),
            "profile_id": str(profile.id),
            "activity": activity,
            "audience_id": str(profile.audience_id),
        },
    ))
    session.commit()
    session.refresh(profile)
    application = session.get(WhatsAppApplication, profile.application_id)
    report_type = session.get(WhatsAppReportType, profile.report_type_id)
    audience = session.get(WhatsAppAudience, profile.audience_id)
    account = session.get(WhatsAppAccount, profile.account_id)
    wing = session.get(Wing, profile.wing_id) if profile.wing_id else None
    scope = session.get(WhatsAppRecipientScope, profile.recipient_scope_id) if profile.recipient_scope_id else None
    return dispatch_profile_dict(profile, application, report_type, audience, account, wing, scope)
