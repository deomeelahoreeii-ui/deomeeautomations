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
def directory_group_dict(
    item: WhatsAppDirectoryGroup,
    configured: WhatsAppGroup | None = None,
    wing: Wing | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "jid": item.jid,
        "name": item.name,
        "description": item.description,
        "owner_jid": item.owner_jid,
        "participant_count": item.participant_count,
        "available": item.available,
        "discovered_at": item.discovered_at,
        "last_seen_at": item.last_seen_at,
        "last_synced_at": item.last_synced_at,
        "configured_id": str(configured.id) if configured else None,
        "wing_id": str(configured.wing_id) if configured else None,
        "wing_name": wing.name if wing else None,
        "purpose": configured.purpose if configured else None,
        "dispatch_enabled": configured.enabled if configured else False,
    }
