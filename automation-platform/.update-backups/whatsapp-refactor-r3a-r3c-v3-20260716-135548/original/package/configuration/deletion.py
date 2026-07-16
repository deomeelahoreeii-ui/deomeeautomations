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
