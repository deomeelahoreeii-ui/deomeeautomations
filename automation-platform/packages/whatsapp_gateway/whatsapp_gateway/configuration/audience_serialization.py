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
