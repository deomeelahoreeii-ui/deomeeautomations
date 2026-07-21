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
class TestMessageInput(BaseModel):
    __test__ = False

    target: str = PydanticField(min_length=10, max_length=80)
    recipient_name: str = PydanticField(default="Test recipient", max_length=200)
    message: str = PydanticField(min_length=1, max_length=3500)


class GroupInput(BaseModel):
    id: uuid.UUID | None = None
    directory_group_id: uuid.UUID | None = None
    wing_id: uuid.UUID
    name: str = PydanticField(min_length=1, max_length=200)
    jid: str = PydanticField(min_length=10, max_length=100)
    purpose: str = PydanticField(default="reports", max_length=80)
    notes: str = PydanticField(default="", max_length=1000)
    enabled: bool = True


class TemplateInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID | None = None
    report_type_id: uuid.UUID | None = None
    recipient_scope_id: uuid.UUID | None = None
    recipient_channel: Literal["individual", "group", "any"] = "any"
    key: str = PydanticField(min_length=1, max_length=100)
    name: str = PydanticField(min_length=1, max_length=200)
    category: Literal[
        "report", "system", "escalation", "zero_result_acknowledgement"
    ] = "report"
    body: str = PydanticField(min_length=1, max_length=5000)
    enabled: bool = True


class SettingsInput(BaseModel):
    send_delay_ms: int = PydanticField(ge=0, le=60_000)
    acknowledgement_timeout_seconds: int = PydanticField(ge=5, le=180)
    max_retries: int = PydanticField(ge=1, le=10)
    require_preview: bool
    live_delivery_enabled: bool


class ReportTypeInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    key: str = PydanticField(min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$")
    name: str = PydanticField(min_length=2, max_length=200)
    description: str = PydanticField(default="", max_length=1000)
    artifact_kind: str = PydanticField(default="document", max_length=80)
    enabled: bool = True


class RecipientScopeInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    parent_id: uuid.UUID | None = None
    channel: Literal["individual", "group"]
    key: str = PydanticField(min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$")
    name: str = PydanticField(min_length=2, max_length=200)
    hierarchy_level: int = PydanticField(default=0, ge=0, le=99)
    description: str = PydanticField(default="", max_length=1000)
    enabled: bool = True


class AudienceInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    key: str | None = PydanticField(
        default=None, min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$"
    )
    name: str = PydanticField(min_length=2, max_length=200)
    description: str = PydanticField(default="", max_length=1000)
    enabled: bool = True


class AudienceMemberInput(BaseModel):
    target_type: Literal["group", "contact"]
    target_id: uuid.UUID
    wing_id: uuid.UUID | None = None
    route_scope_key: str = PydanticField(default="", max_length=100)
    route_scope_value: str = PydanticField(default="", max_length=100)
    route_scope_label: str = PydanticField(default="", max_length=200)


class DispatchProfileInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    key: str = PydanticField(min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$")
    name: str = PydanticField(min_length=2, max_length=200)
    report_type_id: uuid.UUID
    audience_id: uuid.UUID
    account_id: uuid.UUID
    template_id: uuid.UUID | None = None
    recipient_scope_id: uuid.UUID | None = None
    recipient_channel: Literal["individual", "group"] = "group"
    wing_id: uuid.UUID | None = None
    delivery_mode: Literal["groups", "individuals"] = "groups"
    delivery_granularity: Literal["recipient", "scope"] = "recipient"
    require_approval: bool = True
    fallback_policy: Literal["none", "same_scope"] = "none"
    max_retries: int = PydanticField(default=5, ge=0, le=10)
    messages_per_minute: int = PydanticField(default=20, ge=1, le=120)
    message_style: Literal["summary", "detailed"] = "summary"
    attachment_mode: Literal["none", "image", "excel", "image_excel"] = "image_excel"
    image_content: Literal["summary", "details"] = "details"
    enabled: bool = True
    notes: str = PydanticField(default="", max_length=2000)


class ReportingRouteSetupInput(BaseModel):
    """One guided transaction for a complete group reporting route."""

    profile_id: uuid.UUID | None = None
    application_id: uuid.UUID | None = None
    report_type_id: uuid.UUID | None = None
    wing_id: uuid.UUID | None = None
    recipient_scope_id: uuid.UUID | None = None
    account_id: uuid.UUID | None = None
    directory_group_id: uuid.UUID
    route_scope_value: str = PydanticField(min_length=1, max_length=100)
    route_scope_label: str = PydanticField(min_length=1, max_length=200)
    template_id: uuid.UUID | None = None
    create_recommended_template: bool = True
    audience_name: str = PydanticField(default="", max_length=200)
    profile_name: str = PydanticField(default="", max_length=200)
    template_name: str = PydanticField(default="", max_length=200)
    template_body: str = PydanticField(default="{{report_body}}", min_length=1, max_length=5000)
    require_approval: bool = True
    max_retries: int = PydanticField(default=5, ge=0, le=10)
    messages_per_minute: int = PydanticField(default=20, ge=1, le=120)
    message_style: Literal["summary", "detailed"] = "summary"
    attachment_mode: Literal["none", "image", "excel", "image_excel"] = "image_excel"
    image_content: Literal["summary", "details"] = "details"
