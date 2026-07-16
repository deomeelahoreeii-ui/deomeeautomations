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
def template_dict(
    item: WhatsAppTemplate,
    application_name: str | None = None,
    report_type_name: str | None = None,
    recipient_scope_name: str | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id) if item.application_id else None,
        "report_type_id": str(item.report_type_id) if item.report_type_id else None,
        "application_name": application_name or "Shared",
        "report_type_name": report_type_name or "All report types",
        "recipient_channel": item.recipient_channel,
        "recipient_scope_id": str(item.recipient_scope_id) if item.recipient_scope_id else None,
        "recipient_scope_name": recipient_scope_name or "All roles / scopes",
        "key": item.key,
        "name": item.name,
        "category": item.category,
        "body": item.body,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }
