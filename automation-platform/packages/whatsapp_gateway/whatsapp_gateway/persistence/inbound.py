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
    stored_object_id: uuid.UUID | None = Field(default=None, foreign_key="whatsapp_inbound_stored_objects.id", index=True)
    storage_status: str = Field(default="local_only", index=True)
    storage_error: str | None = Field(default=None, sa_column=Column(Text))
    storage_uploaded_at: datetime | None = Field(default=None, index=True)
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
    provider: str = Field(default="baileys", index=True)
    batch_id: uuid.UUID | None = Field(default=None, foreign_key="whatsapp_inbound_batches.id", unique=True, index=True)
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


class WhatsAppInboundStoredObject(SQLModel, table=True):
    __tablename__ = "whatsapp_inbound_stored_objects"
    __table_args__ = (
        UniqueConstraint(
            "backend", "bucket", "object_key",
            name="uq_whatsapp_inbound_stored_object_location",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    backend: str = Field(default="s3", index=True)
    bucket: str = Field(index=True)
    object_key: str = Field(index=True)
    sha256: str = Field(index=True)
    size_bytes: int
    content_type: str | None = Field(default=None, index=True)
    etag: str | None = Field(default=None, index=True)
    version_id: str | None = Field(default=None, index=True)
    metadata_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    status: str = Field(default="available", index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    verified_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppInboundBatch(SQLModel, table=True):
    __tablename__ = "whatsapp_inbound_batches"
    __table_args__ = (
        CheckConstraint(
            "status IN ('created', 'fetching', 'storing', 'completed', 'completed_with_errors', 'failed', 'cancelled')",
            name="ck_whatsapp_inbound_batches_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_code: str = Field(unique=True, index=True)
    account_id: uuid.UUID = Field(foreign_key="whatsapp_accounts.id", index=True)
    contact_id: uuid.UUID = Field(foreign_key="whatsapp_directory_contacts.id", index=True)
    worker_key: str = Field(index=True)
    provider: str = Field(default="wwebjs", index=True)
    requested_count: int = 50
    remote_jid: str | None = Field(default=None, index=True)
    anchor_message_id: str | None = Field(default=None, index=True)
    anchor_timestamp: datetime | None = Field(default=None, index=True)
    status: str = Field(default="created", index=True)
    messages_discovered: int = 0
    files_discovered: int = 0
    files_stored: int = 0
    files_reused: int = 0
    files_failed: int = 0
    total_bytes: int = 0
    storage_backend: str = Field(default="local", index=True)
    raw_bucket: str | None = Field(default=None, index=True)
    manifest_bucket: str | None = Field(default=None, index=True)
    manifest_object_key: str | None = Field(default=None, sa_column=Column(Text))
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    started_at: datetime | None = Field(default=None, index=True)
    finished_at: datetime | None = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppInboundBatchItem(SQLModel, table=True):
    __tablename__ = "whatsapp_inbound_batch_items"
    __table_args__ = (
        UniqueConstraint(
            "batch_id", "attachment_id",
            name="uq_whatsapp_inbound_batch_item_attachment",
        ),
        CheckConstraint(
            "status IN ('discovered', 'downloading', 'hashing', 'uploading', 'stored', 'already_stored', 'storage_pending', 'unsupported', 'failed')",
            name="ck_whatsapp_inbound_batch_items_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_id: uuid.UUID = Field(foreign_key="whatsapp_inbound_batches.id", index=True)
    attachment_id: uuid.UUID = Field(foreign_key="whatsapp_inbound_attachments.id", index=True)
    message_id: uuid.UUID = Field(foreign_key="whatsapp_inbound_messages.id", index=True)
    stored_object_id: uuid.UUID | None = Field(default=None, foreign_key="whatsapp_inbound_stored_objects.id", index=True)
    status: str = Field(default="discovered", index=True)
    original_filename: str | None = Field(default=None, index=True)
    message_timestamp: datetime | None = Field(default=None, index=True)
    mime_type: str | None = Field(default=None, index=True)
    sha256: str | None = Field(default=None, index=True)
    size_bytes: int | None = None
    object_key: str | None = Field(default=None, sa_column=Column(Text))
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    stored_at: datetime | None = Field(default=None, index=True)
    updated_at: datetime = Field(default_factory=utcnow, index=True)


class WhatsAppInboundBatchEvent(SQLModel, table=True):
    __tablename__ = "whatsapp_inbound_batch_events"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    batch_id: uuid.UUID = Field(foreign_key="whatsapp_inbound_batches.id", index=True)
    batch_item_id: uuid.UUID | None = Field(
        default=None, foreign_key="whatsapp_inbound_batch_items.id", index=True
    )
    level: str = Field(default="info", index=True)
    event_type: str = Field(index=True)
    message: str = Field(sa_column=Column(Text))
    details_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=utcnow, index=True)


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
    'WhatsAppInboundStoredObject',
    'WhatsAppInboundBatch',
    'WhatsAppInboundBatchItem',
    'WhatsAppInboundBatchEvent',
    'WhatsAppInboundExportRun',
    'WhatsAppInboundExportItem',
]
