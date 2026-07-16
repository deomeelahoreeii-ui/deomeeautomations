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
def gateway_datetime(value: Any, fallback: datetime | None = None) -> datetime:
    if isinstance(value, datetime):
        return value
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            pass
    return fallback or utcnow()


def account_dict(account: WhatsAppAccount) -> dict[str, Any]:
    return {
        "id": str(account.id),
        "name": account.name,
        "worker_key": account.worker_key,
        "enabled": account.enabled,
        "command_subject": account.command_subject,
        "health_subject": account.health_subject,
        "status": account.status,
        "connected": account.connected,
        "qr_available": account.qr_available,
        "last_seen_at": account.last_seen_at,
        "last_error": account.last_error,
    }


def settings_dict(settings: WhatsAppSettings) -> dict[str, Any]:
    return {
        "send_delay_ms": settings.send_delay_ms,
        "acknowledgement_timeout_seconds": settings.acknowledgement_timeout_seconds,
        "max_retries": settings.max_retries,
        "require_preview": settings.require_preview,
        "live_delivery_enabled": settings.live_delivery_enabled,
    }
