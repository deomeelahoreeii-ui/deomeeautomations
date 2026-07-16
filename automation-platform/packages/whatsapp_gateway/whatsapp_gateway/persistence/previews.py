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


__all__ = [
    'WhatsAppDispatchPreview',
    'WhatsAppDispatchPreviewArtifact',
    'WhatsAppDispatchPreviewDelivery',
    'WhatsAppDispatchApproval',
]
