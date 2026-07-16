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
