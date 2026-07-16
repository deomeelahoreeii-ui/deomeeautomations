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
def report_type_dict(
    item: WhatsAppReportType, application: WhatsAppApplication
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "description": item.description,
        "artifact_kind": item.artifact_kind,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }


def recipient_scope_dict(
    item: WhatsAppRecipientScope,
    application: WhatsAppApplication,
    parent: WhatsAppRecipientScope | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "parent_id": str(item.parent_id) if item.parent_id else None,
        "parent_name": parent.name if parent else None,
        "channel": item.channel,
        "key": item.key,
        "name": item.name,
        "hierarchy_level": item.hierarchy_level,
        "description": item.description,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }
