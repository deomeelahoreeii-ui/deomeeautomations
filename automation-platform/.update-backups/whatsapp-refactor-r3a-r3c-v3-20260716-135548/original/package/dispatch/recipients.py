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
router = APIRouter()


@router.get("/recipients")
def recipients(
    search: str = "",
    recipient_type: Literal["all", "officer", "school_head"] = "all",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    if recipient_type in {"all", "officer"}:
        for officer in session.scalars(
            select(Officer).where(Officer.active.is_(True), Officer.mobile != "")
        ):
            wing = session.get(Wing, officer.wing_id)
            records.append(
                {
                    "id": str(officer.id),
                    "type": "officer",
                    "role": officer.role.upper(),
                    "name": officer.name,
                    "mobile": officer.mobile,
                    "wing": wing.name if wing else "",
                }
            )
    if recipient_type in {"all", "school_head"}:
        head_rows = session.execute(
            select(SchoolHead, School, Wing)
            .join(School, School.id == SchoolHead.school_id)
            .join(Wing, Wing.id == School.wing_id)
            .where(
                SchoolHead.active.is_(True),
                School.active.is_(True),
                SchoolHead.mobile != "",
            )
        ).all()
        for head, school, wing in head_rows:
            records.append(
                {
                    "id": str(head.id),
                    "type": "school_head",
                    "role": "School head",
                    "name": head.name,
                    "mobile": head.mobile,
                    "wing": wing.name,
                    "school": school.name,
                }
            )
    if search:
        needle = search.casefold()
        records = [
            item
            for item in records
            if needle in " ".join(str(value) for value in item.values()).casefold()
        ]
    records.sort(key=lambda item: (item["name"].casefold(), item["mobile"]))
    total = len(records)
    start = (page - 1) * page_size
    return {
        "items": records[start : start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
