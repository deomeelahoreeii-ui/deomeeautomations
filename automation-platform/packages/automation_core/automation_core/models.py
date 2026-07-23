from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import CheckConstraint, Column, DateTime, ForeignKey, JSON, Text, UniqueConstraint, Uuid
from sqlmodel import Field, SQLModel

from automation_core.time import utcnow


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


class JobType(str, Enum):
    antidengue_report = "antidengue.report"
    whatsapp_dispatch_preview = "whatsapp.dispatch_preview"
    whatsapp_dispatch_send = "whatsapp.dispatch_send"
    whatsapp_inbound_export = "whatsapp.inbound_export"
    crm_sheet_filter = "crm.sheet_filter"
    crm_pdf_filter = "crm.pdf_filter"
    crm_sheet_to_pdf = "crm.sheet_to_pdf"


class JobBase(SQLModel):
    type: str = Field(index=True)
    title: str
    status: str = Field(default=JobStatus.queued.value, index=True)
    parameters: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    result: dict[str, Any] | None = Field(default=None, sa_column=Column(JSON))
    error: str | None = Field(default=None, sa_column=Column(Text))
    task_id: str | None = Field(default=None, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
    updated_at: datetime = Field(default_factory=utcnow)
    started_at: datetime | None = Field(default=None)
    finished_at: datetime | None = Field(default=None)


class Job(JobBase, table=True):
    __tablename__ = "jobs"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)


class TaskOutbox(SQLModel, table=True):
    """Durable broker publication request committed with its owning job."""

    __tablename__ = "task_outbox"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_task_outbox_idempotency_key"),
        UniqueConstraint("task_id", name="uq_task_outbox_task_id"),
        CheckConstraint(
            "status IN ('pending', 'publishing', 'published', 'failed', 'cancelled')",
            name="ck_task_outbox_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    job_id: uuid.UUID = Field(
        sa_column=Column(Uuid, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True)
    )
    task_name: str = Field(index=True, max_length=180)
    queue: str = Field(index=True, max_length=80)
    args: list[Any] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    kwargs: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    task_id: str = Field(index=True, max_length=80)
    idempotency_key: str = Field(index=True, max_length=240)
    status: str = Field(default="pending", index=True)
    attempts: int = Field(default=0)
    available_at: datetime = Field(default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False, index=True))
    locked_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True, index=True))
    published_at: datetime | None = Field(default=None, sa_column=Column(DateTime(timezone=True), nullable=True, index=True))
    last_error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False, index=True))
    updated_at: datetime = Field(default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False, index=True))


class AutomationWorkerRuntime(SQLModel, table=True):
    """Durable, expiring declaration of a worker's executable contracts."""

    __tablename__ = "automation_worker_runtimes"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    worker_name: str = Field(unique=True, index=True, max_length=180)
    queues: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    protocols: dict[str, int] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    capabilities: dict[str, int] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    capability_fingerprint: str = Field(default="", index=True, max_length=64)
    build_id: str = Field(default="", index=True, max_length=180)
    database_fingerprint: str = Field(default="", index=True, max_length=64)
    started_at: datetime = Field(default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False))
    last_seen_at: datetime = Field(default_factory=utcnow, sa_column=Column(DateTime(timezone=True), nullable=False, index=True))


class JobPublic(JobBase):
    id: uuid.UUID


class JobLog(SQLModel, table=True):
    __tablename__ = "job_logs"

    id: int | None = Field(default=None, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="jobs.id", index=True)
    level: str = Field(default="info", index=True)
    message: str = Field(sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)


class JobLogPublic(SQLModel):
    id: int
    job_id: uuid.UUID
    level: str
    message: str
    created_at: datetime


class StoredObject(SQLModel, table=True):
    """One verified immutable object in the platform storage backend."""

    __tablename__ = "stored_objects"
    __table_args__ = (
        UniqueConstraint("backend", "bucket", "object_key", name="uq_stored_objects_location"),
        CheckConstraint("status IN ('ready', 'error')", name="ck_stored_objects_status"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    backend: str = Field(default="s3", index=True)
    bucket: str = Field(index=True)
    object_key: str = Field(sa_column=Column(Text, nullable=False))
    sha256: str = Field(index=True, max_length=64)
    size_bytes: int = 0
    content_type: str | None = None
    etag: str | None = None
    version_id: str | None = None
    status: str = Field(default="ready", index=True)
    last_error: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=utcnow, index=True)
    verified_at: datetime | None = Field(default=None, index=True)


class Artifact(SQLModel, table=True):
    __tablename__ = "artifacts"

    id: int | None = Field(default=None, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="jobs.id", index=True)
    module_key: str = Field(default="legacy", index=True)
    kind: str = Field(default="file", index=True)
    name: str
    path: str = Field(sa_column=Column(Text))
    size_bytes: int = 0
    sha256: str = Field(default="", index=True, max_length=64)
    content_type: str | None = None
    stored_object_id: uuid.UUID | None = Field(default=None, foreign_key="stored_objects.id", index=True)
    storage_status: str = Field(default="local", index=True)
    storage_error: str | None = Field(default=None, sa_column=Column(Text))
    archived_at: datetime | None = Field(default=None, index=True)
    local_evicted_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True, index=True),
    )
    last_hydrated_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True, index=True),
    )
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ArtifactPublic(SQLModel):
    id: int
    job_id: uuid.UUID
    module_key: str
    kind: str
    name: str
    path: str
    size_bytes: int
    sha256: str
    content_type: str | None
    stored_object_id: uuid.UUID | None
    storage_status: str
    storage_error: str | None
    archived_at: datetime | None
    local_evicted_at: datetime | None
    last_hydrated_at: datetime | None
    created_at: datetime


class SourceFile(SQLModel, table=True):
    """Immutable file received by a platform module."""

    __tablename__ = "source_files"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    module_key: str = Field(index=True)
    source_kind: str = Field(default="manual_upload", index=True)
    original_name: str
    stored_path: str = Field(sa_column=Column(Text, nullable=False))
    content_type: str | None = None
    extension: str = Field(index=True)
    size_bytes: int
    sha256: str = Field(index=True, max_length=64)
    stored_object_id: uuid.UUID | None = Field(default=None, foreign_key="stored_objects.id", index=True)
    storage_status: str = Field(default="local", index=True)
    storage_error: str | None = Field(default=None, sa_column=Column(Text))
    archived_at: datetime | None = Field(default=None, index=True)
    local_evicted_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True, index=True),
    )
    last_hydrated_at: datetime | None = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), nullable=True, index=True),
    )
    validation_status: str = Field(default="pending", index=True)
    schema_version: str | None = None
    detected_metadata: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    validation_errors: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    validation_warnings: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    duplicate_of_id: uuid.UUID | None = Field(default=None, foreign_key="source_files.id", index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)


class SourceFileRun(SQLModel, table=True):
    """Links an immutable input to each processing attempt."""

    __tablename__ = "source_file_runs"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    source_file_id: uuid.UUID = Field(foreign_key="source_files.id", index=True)
    job_id: uuid.UUID = Field(foreign_key="jobs.id", unique=True, index=True)
    created_at: datetime = Field(default_factory=utcnow, index=True)
