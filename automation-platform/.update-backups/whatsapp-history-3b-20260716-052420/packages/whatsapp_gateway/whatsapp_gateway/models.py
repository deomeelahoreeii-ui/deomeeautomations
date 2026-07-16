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


class WhatsAppAccount(SQLModel, table=True):
    __tablename__ = "whatsapp_accounts"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    name: str
    worker_key: str = Field(unique=True, index=True)
    enabled: bool = Field(default=True, index=True)
    command_subject: str = "whatsapp.pending"
    health_subject: str = "whatsapp.worker.health"
    status: str = Field(default="unknown", index=True)
    connected: bool = Field(default=False, index=True)
    qr_available: bool = False
    last_seen_at: datetime | None = Field(default=None, index=True)
    last_error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppDirectoryGroup(SQLModel, table=True):
    """A group observed by the gateway, not yet authorized for dispatch."""

    __tablename__ = "whatsapp_directory_groups"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "jid", name="uq_whatsapp_directory_groups_account_jid"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    jid: str = Field(index=True)
    name: str = Field(default="", index=True)
    description: str = Field(default="", sa_column=Column(Text))
    owner_jid: str | None = Field(default=None, index=True)
    participant_count: int = 0
    available: bool = Field(default=True, index=True)
    discovered_at: datetime = Field(default_factory=utcnow, index=True)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)
    last_synced_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppDirectoryContact(SQLModel, table=True):
    """A normalized WhatsApp identity discovered by the gateway."""

    __tablename__ = "whatsapp_directory_contacts"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "canonical_key", name="uq_whatsapp_contacts_account_key"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    canonical_key: str = Field(index=True)
    phone_jid: str | None = Field(default=None, index=True)
    primary_lid_jid: str | None = Field(default=None, index=True)
    display_name: str = Field(default="", index=True)
    source: str = Field(default="gateway", index=True)
    confidence: float = 0.0
    active: bool = Field(default=True, index=True)
    token_status: str = Field(default="unknown", index=True)
    token_checked_at: datetime | None = Field(default=None, index=True)
    token_issued_at: datetime | None = None
    first_seen_at: datetime = Field(default_factory=utcnow, index=True)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)
    last_synced_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppIdentityAlias(SQLModel, table=True):
    __tablename__ = "whatsapp_identity_aliases"
    __table_args__ = (
        UniqueConstraint(
            "account_id", "lid_jid", name="uq_whatsapp_identity_aliases_account_lid"
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    contact_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_contacts.id", index=True
    )
    lid_jid: str = Field(index=True)
    source: str = Field(default="gateway", index=True)
    confidence: float = 0.0
    first_seen_at: datetime = Field(default_factory=utcnow)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppGroupMember(SQLModel, table=True):
    __tablename__ = "whatsapp_group_members"
    __table_args__ = (
        UniqueConstraint(
            "directory_group_id",
            "member_key",
            name="uq_whatsapp_group_members_group_key",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    directory_group_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_groups.id", index=True
    )
    contact_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_contacts.id", index=True
    )
    member_key: str = Field(index=True)
    member_jid: str | None = Field(default=None, index=True)
    phone_jid: str | None = Field(default=None, index=True)
    lid_jid: str | None = Field(default=None, index=True)
    display_name: str = Field(default="", index=True)
    admin_role: str | None = Field(default=None, index=True)
    active: bool = Field(default=True, index=True)
    first_seen_at: datetime = Field(default_factory=utcnow)
    last_seen_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppGroup(SQLModel, table=True):
    __tablename__ = "whatsapp_groups"
    __table_args__ = (
        UniqueConstraint("account_id", "jid", name="uq_whatsapp_groups_account_jid"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    directory_group_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_groups.id", index=True
    )
    wing_id: uuid.UUID = Field(foreign_key="wings.id", index=True)
    name: str = Field(index=True)
    jid: str = Field(index=True)
    purpose: str = Field(default="reports", index=True)
    enabled: bool = Field(default=True, index=True)
    verified_at: datetime | None = None
    notes: str = Field(default="", sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


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


class WhatsAppContactLink(SQLModel, table=True):
    """Verified link between a discovered WhatsApp identity and master data."""

    __tablename__ = "whatsapp_contact_links"
    __table_args__ = (
        UniqueConstraint(
            "directory_contact_id",
            "entity_type",
            "entity_key",
            name="uq_whatsapp_contact_links_target",
        ),
        CheckConstraint(
            "entity_type IN ('officer', 'school_head')",
            name="ck_whatsapp_contact_links_entity_type",
        ),
        CheckConstraint(
            "status IN ('suggested', 'verified')",
            name="ck_whatsapp_contact_links_status",
        ),
        CheckConstraint(
            "(entity_type = 'officer' AND officer_id IS NOT NULL AND school_head_id IS NULL) "
            "OR (entity_type = 'school_head' AND school_head_id IS NOT NULL AND officer_id IS NULL)",
            name="ck_whatsapp_contact_links_entity",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    directory_contact_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_contacts.id", index=True
    )
    entity_type: str = Field(index=True)
    entity_key: str = Field(index=True)
    officer_id: uuid.UUID | None = Field(
        default=None, foreign_key="officers.id", index=True
    )
    school_head_id: uuid.UUID | None = Field(
        default=None, foreign_key="school_heads.id", index=True
    )
    wing_id: uuid.UUID = Field(foreign_key="wings.id", index=True)
    status: str = Field(default="verified", index=True)
    source: str = Field(default="manual", index=True)
    confidence: float = 1.0
    active: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)


class WhatsAppDispatchPreview(SQLModel, table=True):
    """Immutable, reviewable snapshot of a resolved dispatch plan."""

    __tablename__ = "whatsapp_dispatch_previews"
    __table_args__ = (
        CheckConstraint(
            "status IN ('ready', 'blocked', 'stale')",
            name="ck_whatsapp_dispatch_previews_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    preview_key: str = Field(unique=True, index=True)
    application_id: uuid.UUID = Field(
        foreign_key="whatsapp_applications.id", index=True
    )
    source_job_id: uuid.UUID = Field(foreign_key="jobs.id", index=True)
    dispatch_profile_id: uuid.UUID = Field(
        foreign_key="whatsapp_dispatch_profiles.id", index=True
    )
    status: str = Field(default="blocked", index=True)
    profile_version: int
    application_name: str
    report_type_name: str
    audience_name: str
    profile_name: str
    account_name: str
    template_name: str = ""
    wing_name: str = ""
    ready_count: int = 0
    warning_count: int = 0
    blocked_count: int = 0
    skipped_count: int = 0
    delivery_count: int = 0
    artifact_count: int = 0
    issues: list[dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    configuration_snapshot: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON)
    )
    content_sha256: str = Field(default="", index=True)
    created_by: str = Field(default="web", index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    frozen_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppDispatchPreviewArtifact(SQLModel, table=True):
    __tablename__ = "whatsapp_dispatch_preview_artifacts"
    __table_args__ = (
        CheckConstraint(
            "status IN ('ready', 'warning', 'blocked')",
            name="ck_whatsapp_dispatch_preview_artifacts_status",
        ),
        CheckConstraint(
            "role IN ('delivery', 'manifest', 'audit', 'supporting')",
            name="ck_whatsapp_dispatch_preview_artifacts_role",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    preview_id: uuid.UUID = Field(
        foreign_key="whatsapp_dispatch_previews.id", index=True
    )
    artifact_id: int | None = Field(default=None, foreign_key="artifacts.id", index=True)
    report_type_id: uuid.UUID = Field(
        foreign_key="whatsapp_report_types.id", index=True
    )
    wing_id: uuid.UUID | None = Field(default=None, foreign_key="wings.id", index=True)
    role: str = Field(default="supporting", index=True)
    name: str = Field(index=True)
    path_snapshot: str = Field(sa_column=Column(Text))
    mime_type: str = "application/octet-stream"
    size_bytes: int = 0
    checksum_sha256: str = Field(default="", index=True)
    status: str = Field(default="ready", index=True)
    issues: list[dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppDispatchPreviewDelivery(SQLModel, table=True):
    __tablename__ = "whatsapp_dispatch_preview_deliveries"
    __table_args__ = (
        UniqueConstraint(
            "preview_id",
            "idempotency_key",
            name="uq_whatsapp_dispatch_preview_delivery_idempotency",
        ),
        CheckConstraint(
            "status IN ('ready', 'warning', 'blocked', 'skipped')",
            name="ck_whatsapp_dispatch_preview_deliveries_status",
        ),
        CheckConstraint(
            "target_type IN ('group', 'contact')",
            name="ck_whatsapp_dispatch_preview_deliveries_target_type",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    preview_id: uuid.UUID = Field(
        foreign_key="whatsapp_dispatch_previews.id", index=True
    )
    sequence: int = Field(index=True)
    source_route_key: str = Field(default="", index=True)
    target_type: str = Field(index=True)
    target_name: str = Field(default="", index=True)
    target_jid: str = Field(default="", index=True)
    directory_group_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_groups.id", index=True
    )
    directory_contact_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_directory_contacts.id", index=True
    )
    contact_link_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_contact_links.id", index=True
    )
    wing_id: uuid.UUID | None = Field(default=None, foreign_key="wings.id", index=True)
    wing_name: str = ""
    route_kind: str = Field(default="", index=True)
    route_scope: str = Field(default="", index=True)
    message: str = Field(sa_column=Column(Text))
    attachment_ids: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    routing_snapshot: dict[str, Any] = Field(
        default_factory=dict, sa_column=Column(JSON)
    )
    issues: list[dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    status: str = Field(default="blocked", index=True)
    idempotency_key: str = Field(index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppDispatchApproval(SQLModel, table=True):
    """One explicit, auditable authorization to send a frozen preview."""

    __tablename__ = "whatsapp_dispatch_approvals"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'sending', 'completed', 'failed')",
            name="ck_whatsapp_dispatch_approvals_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    preview_id: uuid.UUID = Field(
        foreign_key="whatsapp_dispatch_previews.id", unique=True, index=True
    )
    job_id: uuid.UUID = Field(foreign_key="jobs.id", unique=True, index=True)
    approved_by: str = Field(default="web-operator", index=True)
    warnings_acknowledged: bool = False
    preview_content_sha256: str = Field(index=True)
    delivery_count: int = 0
    status: str = Field(default="queued", index=True)
    error: str | None = Field(default=None, sa_column=Column(Text))
    approved_at: datetime = Field(default_factory=utcnow, index=True)
    completed_at: datetime | None = Field(default=None, index=True)


class WhatsAppDelivery(SQLModel, table=True):
    __tablename__ = "whatsapp_deliveries"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'delivered', 'sent_pending_confirmation', 'failed', 'timed_out')",
            name="ck_whatsapp_deliveries_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    approval_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_dispatch_approvals.id", index=True
    )
    preview_delivery_id: uuid.UUID | None = Field(
        default=None,
        foreign_key="whatsapp_dispatch_preview_deliveries.id",
        unique=True,
        index=True,
    )
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    wing_id: uuid.UUID | None = Field(default=None, foreign_key="wings.id", index=True)
    recipient_type: str = Field(default="test", index=True)
    recipient_name: str = ""
    target: str = Field(index=True)
    message: str = Field(sa_column=Column(Text))
    attachments: list[dict[str, Any]] = Field(default_factory=list, sa_column=Column(JSON))
    status: str = Field(default="queued", index=True)
    status_subject: str = Field(unique=True)
    queue_stream: str | None = None
    queue_sequence: int | None = None
    error: str | None = Field(default=None, sa_column=Column(Text))
    provider_result: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    queued_at: datetime = Field(default_factory=utcnow, index=True)
    completed_at: datetime | None = Field(default=None, index=True)


class WhatsAppActivity(SQLModel, table=True):
    __tablename__ = "whatsapp_activity"

    id: int | None = Field(default=None, primary_key=True)
    account_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_accounts.id", index=True
    )
    level: str = Field(default="info", index=True)
    event_type: str = Field(index=True)
    message: str = Field(sa_column=Column(Text))
    details: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utcnow, index=True)


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

class WhatsAppInboundMessage(SQLModel, table=True):
    """Normalized inbound message metadata mirrored from the WhatsApp worker."""
    __tablename__ = "whatsapp_inbound_messages"
    __table_args__ = (UniqueConstraint("account_id", "remote_jid", "message_id", name="uq_whatsapp_inbound_message_identity"),)
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    worker_key: str = Field(index=True)
    message_id: str = Field(index=True)
    remote_jid: str = Field(index=True)
    participant_jid: str | None = Field(default=None, index=True)
    sender_jid: str = Field(index=True)
    directory_contact_id: uuid.UUID | None = Field(default=None, foreign_key="whatsapp_directory_contacts.id", index=True)
    from_me: bool = Field(default=False, index=True)
    chat_scope: str = Field(index=True)
    message_timestamp: datetime = Field(index=True)
    push_name: str | None = Field(default=None, index=True)
    text_content: str | None = Field(default=None, sa_column=Column(Text))
    message_type: str = Field(index=True)
    ingestion_source: str = Field(index=True)
    payload_sha256: str = Field(index=True)
    raw_payload: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    first_ingested_at: datetime = Field(default_factory=utcnow, index=True)
    last_ingested_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppInboundAttachment(SQLModel, table=True):
    __tablename__ = "whatsapp_inbound_attachments"
    __table_args__ = (UniqueConstraint("message_id", name="uq_whatsapp_inbound_attachment_message"),)
    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    message_id: uuid.UUID = Field(foreign_key="whatsapp_inbound_messages.id", index=True)
    media_kind: str = Field(index=True)
    message_key: str = Field(index=True)
    original_filename: str | None = Field(default=None, index=True)
    mime_type: str | None = Field(default=None, index=True)
    detected_mime_type: str | None = Field(default=None, index=True)
    media_category: str | None = Field(default=None, index=True)
    safe_extension: str | None = Field(default=None, index=True)
    declared_size: int | None = None
    actual_size: int | None = None
    media_sha256: str | None = Field(default=None, index=True)
    actual_sha256: str | None = Field(default=None, index=True)
    stored_path: str | None = Field(default=None, sa_column=Column(Text))
    caption: str | None = Field(default=None, sa_column=Column(Text))
    download_status: str = Field(default="metadata_only", index=True)
    download_attempts: int = 0
    last_error: str | None = Field(default=None, sa_column=Column(Text))
    archived_at: datetime | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppInboundExportRun(SQLModel, table=True):
    __tablename__ = "whatsapp_inbound_export_runs"
    __table_args__ = (
        CheckConstraint(
            "chat_scope IN ('direct', 'direct_and_groups')",
            name="ck_whatsapp_inbound_export_runs_scope",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="jobs.id", unique=True, index=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    contact_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_contacts.id", index=True
    )
    contact_name: str = Field(default="", index=True)
    date_from: datetime | None = Field(default=None, index=True)
    date_to: datetime | None = Field(default=None, index=True)
    chat_scope: str = Field(default="direct", index=True)
    media_types: list[str] = Field(default_factory=list, sa_column=Column(JSON))
    status: str = Field(default="queued", index=True)
    files_matched: int = 0
    files_downloaded: int = 0
    files_reused: int = 0
    files_unavailable: int = 0
    total_bytes: int = 0
    coverage_earliest_at: datetime | None = None
    coverage_latest_at: datetime | None = None
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    started_at: datetime | None = Field(default=None, index=True)
    finished_at: datetime | None = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppInboundExportItem(SQLModel, table=True):
    __tablename__ = "whatsapp_inbound_export_items"
    __table_args__ = (
        UniqueConstraint(
            "export_run_id",
            "attachment_id",
            name="uq_whatsapp_inbound_export_items_run_attachment",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    export_run_id: uuid.UUID = Field(
        foreign_key="whatsapp_inbound_export_runs.id", index=True
    )
    attachment_id: uuid.UUID = Field(
        foreign_key="whatsapp_inbound_attachments.id", index=True
    )
    media_category: str = Field(index=True)
    status: str = Field(default="pending", index=True)
    output_path: str | None = Field(default=None, sa_column=Column(Text))
    output_name: str | None = Field(default=None)
    duplicate_of_item_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_inbound_export_items.id", index=True
    )
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)

