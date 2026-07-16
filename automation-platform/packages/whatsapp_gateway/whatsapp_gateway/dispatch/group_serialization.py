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
def group_dict(
    item: WhatsAppGroup,
    wing_name: str,
    directory_group: WhatsAppDirectoryGroup | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "directory_group_id": (
            str(item.directory_group_id) if item.directory_group_id else None
        ),
        "wing_id": str(item.wing_id),
        "wing_name": wing_name,
        "name": item.name,
        "jid": item.jid,
        "purpose": item.purpose,
        "enabled": item.enabled,
        "verified_at": item.verified_at,
        "notes": item.notes,
        "participant_count": directory_group.participant_count if directory_group else None,
        "last_detected_at": directory_group.last_seen_at if directory_group else None,
    }
