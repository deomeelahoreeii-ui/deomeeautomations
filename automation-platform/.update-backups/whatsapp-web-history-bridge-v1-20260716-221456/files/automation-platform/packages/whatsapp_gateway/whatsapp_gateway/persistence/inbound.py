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


class WhatsAppInboundHistoryRequest(SQLModel, table=True):
    """Auditable lifecycle for one on-demand WhatsApp history request."""

    __tablename__ = "whatsapp_inbound_history_requests"
    __table_args__ = (
        CheckConstraint(
            "status IN ('requested', 'accepted', 'syncing', 'succeeded', 'no_results', 'failed', 'timed_out')",
            name="ck_whatsapp_inbound_history_requests_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    request_id: str = Field(unique=True, index=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    contact_id: uuid.UUID = Field(
        foreign_key="whatsapp_directory_contacts.id", index=True
    )
    worker_key: str = Field(index=True)
    requested_count: int = 50
    remote_jid: str | None = Field(default=None, index=True)
    anchor_message_id: str | None = Field(default=None, index=True)
    anchor_timestamp: datetime | None = Field(default=None, index=True)
    operation_id: str | None = Field(default=None, index=True)
    status: str = Field(default="requested", index=True)
    baseline_messages: int = 0
    baseline_attachments: int = 0
    messages_received: int = 0
    attachments_discovered: int = 0
    error: str | None = Field(default=None, sa_column=Column(Text))
    requested_at: datetime = Field(default_factory=utcnow, index=True)
    accepted_at: datetime | None = Field(default=None, index=True)
    last_activity_at: datetime | None = Field(default=None, index=True)
    finished_at: datetime | None = Field(default=None, index=True)
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


__all__ = [
    'WhatsAppInboundMessage',
    'WhatsAppInboundAttachment',
    'WhatsAppInboundHistoryRequest',
    'WhatsAppInboundExportRun',
    'WhatsAppInboundExportItem',
]
