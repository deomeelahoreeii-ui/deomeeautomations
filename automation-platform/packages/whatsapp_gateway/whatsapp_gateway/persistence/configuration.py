from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    Column,
    ForeignKeyConstraint,
    JSON,
    Text,
    UniqueConstraint,
)
from sqlmodel import Field, SQLModel

from automation_core.time import utcnow

class WhatsAppApplication(SQLModel, table=True):
    """A platform module that can publish reports through WhatsApp."""

    __tablename__ = "whatsapp_applications"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    key: str = Field(unique=True, index=True)
    name: str = Field(index=True)
    description: str = Field(default="", sa_column=Column(Text))
    enabled: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppReportType(SQLModel, table=True):
    __tablename__ = "whatsapp_report_types"
    __table_args__ = (
        UniqueConstraint(
            "application_id", "key", name="uq_whatsapp_report_types_application_key"
        ),
        UniqueConstraint(
            "id", "application_id", name="uq_whatsapp_report_types_id_application"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    application_id: uuid.UUID = Field(
        foreign_key="whatsapp_applications.id", index=True
    )
    key: str = Field(index=True)
    name: str = Field(index=True)
    description: str = Field(default="", sa_column=Column(Text))
    artifact_kind: str = Field(default="document", index=True)
    enabled: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppRecipientScope(SQLModel, table=True):
    """Configurable recipient role (individual) or hierarchy scope (group)."""

    __tablename__ = "whatsapp_recipient_scopes"
    __table_args__ = (
        UniqueConstraint(
            "application_id",
            "channel",
            "key",
            name="uq_whatsapp_recipient_scopes_application_channel_key",
        ),
        CheckConstraint(
            "channel IN ('individual', 'group')",
            name="ck_whatsapp_recipient_scopes_channel",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    application_id: uuid.UUID = Field(
        foreign_key="whatsapp_applications.id", index=True
    )
    parent_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_recipient_scopes.id", index=True
    )
    channel: str = Field(index=True)
    key: str = Field(index=True)
    name: str = Field(index=True)
    hierarchy_level: int = Field(default=0, index=True)
    description: str = Field(default="", sa_column=Column(Text))
    enabled: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppAudience(SQLModel, table=True):
    __tablename__ = "whatsapp_audiences"
    __table_args__ = (
        UniqueConstraint(
            "application_id", "key", name="uq_whatsapp_audiences_application_key"
        ),
        UniqueConstraint(
            "id", "application_id", name="uq_whatsapp_audiences_id_application"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    application_id: uuid.UUID = Field(
        foreign_key="whatsapp_applications.id", index=True
    )
    key: str = Field(index=True)
    name: str = Field(index=True)
    description: str = Field(default="", sa_column=Column(Text))
    enabled: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppAudienceMember(SQLModel, table=True):
    __tablename__ = "whatsapp_audience_members"
    __table_args__ = (
        UniqueConstraint(
            "audience_id", "target_key", name="uq_whatsapp_audience_members_target"
        ),
        CheckConstraint(
            "(target_type = 'group' AND directory_group_id IS NOT NULL AND directory_contact_id IS NULL) "
            "OR (target_type = 'contact' AND directory_contact_id IS NOT NULL AND directory_group_id IS NULL)",
            name="ck_whatsapp_audience_members_target",
        ),
        CheckConstraint(
            "target_type IN ('group', 'contact')",
            name="ck_whatsapp_audience_members_type",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    audience_id: uuid.UUID = Field(foreign_key="whatsapp_audiences.id", index=True)
    target_type: str = Field(index=True)
    target_key: str = Field(index=True)
    directory_group_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_groups.id", index=True
    )
    directory_contact_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_contacts.id", index=True
    )
    # A target is also bound to the hierarchy node it represents. This keeps
    # destination selection independent from legacy WhatsApp route payloads.
    route_scope_key: str = Field(default="", index=True)
    route_scope_value: str = Field(default="", index=True)
    route_scope_label: str = Field(default="")
    enabled: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)


class WhatsAppTemplate(SQLModel, table=True):
    __tablename__ = "whatsapp_templates"
    __table_args__ = (
        CheckConstraint(
            "recipient_channel IN ('individual', 'group', 'any')",
            name="ck_whatsapp_templates_recipient_channel",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    application_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_applications.id", index=True
    )
    report_type_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_report_types.id", index=True
    )
    recipient_scope_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_recipient_scopes.id", index=True
    )
    recipient_channel: str = Field(default="any", index=True)
    key: str = Field(unique=True, index=True)
    name: str
    category: str = Field(default="report", index=True)
    body: str = Field(sa_column=Column(Text))
    enabled: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppDispatchProfile(SQLModel, table=True):
    __tablename__ = "whatsapp_dispatch_profiles"
    __table_args__ = (
        UniqueConstraint(
            "application_id", "key", name="uq_whatsapp_profiles_application_key"
        ),
        ForeignKeyConstraint(
            ["report_type_id", "application_id"],
            ["whatsapp_report_types.id", "whatsapp_report_types.application_id"],
            name="fk_whatsapp_profiles_report_application",
        ),
        ForeignKeyConstraint(
            ["audience_id", "application_id"],
            ["whatsapp_audiences.id", "whatsapp_audiences.application_id"],
            name="fk_whatsapp_profiles_audience_application",
        ),
        CheckConstraint(
            "delivery_mode IN ('groups', 'individuals', 'mixed')",
            name="ck_whatsapp_profiles_delivery_mode",
        ),
        CheckConstraint(
            "delivery_granularity IN ('recipient', 'scope')",
            name="ck_whatsapp_profiles_delivery_granularity",
        ),
        CheckConstraint(
            "recipient_channel IN ('individual', 'group')",
            name="ck_whatsapp_profiles_recipient_channel",
        ),
        CheckConstraint(
            "fallback_policy IN ('none', 'same_scope')",
            name="ck_whatsapp_profiles_fallback_policy",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    application_id: uuid.UUID = Field(index=True)
    key: str = Field(index=True)
    name: str = Field(index=True)
    report_type_id: uuid.UUID = Field(index=True)
    audience_id: uuid.UUID = Field(index=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    template_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_templates.id", index=True
    )
    recipient_scope_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_recipient_scopes.id", index=True
    )
    recipient_channel: str = Field(default="group", index=True)
    wing_id: uuid.UUID | None = Field(default=None, foreign_key="wings.id", index=True)
    delivery_mode: str = Field(default="mixed", index=True)
    delivery_granularity: str = Field(default="recipient", index=True)
    require_approval: bool = True
    fallback_policy: str = Field(default="none", index=True)
    max_retries: int = 5
    messages_per_minute: int = 20
    presentation_policy: dict[str, Any] = Field(
        default_factory=dict,
        sa_column=Column(JSON, nullable=False),
    )
    guided_setup: bool = Field(default=False, index=True)
    owns_audience: bool = False
    owns_template: bool = False
    enabled: bool = Field(default=True, index=True)
    version: int = 1
    notes: str = Field(default="", sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppSettings(SQLModel, table=True):
    __tablename__ = "whatsapp_settings"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    default_account_id: uuid.UUID = Field(
        foreign_key="whatsapp_accounts.id", unique=True, index=True
    )
    send_delay_ms: int = 1500
    acknowledgement_timeout_seconds: int = 45
    max_retries: int = 5
    require_preview: bool = True
    live_delivery_enabled: bool = False
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


__all__ = [
    'WhatsAppApplication',
    'WhatsAppReportType',
    'WhatsAppRecipientScope',
    'WhatsAppAudience',
    'WhatsAppAudienceMember',
    'WhatsAppTemplate',
    'WhatsAppDispatchProfile',
    'WhatsAppSettings',
]
