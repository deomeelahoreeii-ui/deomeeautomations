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
def dispatch_profile_dict(
    item: WhatsAppDispatchProfile,
    application: WhatsAppApplication,
    report_type: WhatsAppReportType,
    audience_item: WhatsAppAudience,
    account: WhatsAppAccount,
    wing: Wing | None,
    recipient_scope: WhatsAppRecipientScope | None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "report_type_id": str(item.report_type_id),
        "report_type_name": report_type.name,
        "audience_id": str(item.audience_id),
        "audience_name": audience_item.name,
        "account_id": str(item.account_id),
        "account_name": account.name,
        "template_id": str(item.template_id) if item.template_id else None,
        "recipient_channel": item.recipient_channel,
        "recipient_scope_id": str(item.recipient_scope_id) if item.recipient_scope_id else None,
        "recipient_scope_name": recipient_scope.name if recipient_scope else "Legacy / unscoped",
        "wing_id": str(item.wing_id) if item.wing_id else None,
        "wing_name": wing.name if wing else None,
        "delivery_mode": item.delivery_mode,
        "delivery_granularity": item.delivery_granularity,
        "require_approval": item.require_approval,
        "fallback_policy": item.fallback_policy,
        "max_retries": item.max_retries,
        "messages_per_minute": item.messages_per_minute,
        "presentation_policy": item.presentation_policy or {},
        "guided_setup": item.guided_setup,
        "enabled": item.enabled,
        "version": item.version,
        "notes": item.notes,
        "updated_at": item.updated_at,
    }
