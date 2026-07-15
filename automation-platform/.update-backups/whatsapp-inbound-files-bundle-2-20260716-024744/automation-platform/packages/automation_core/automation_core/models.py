from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any

from sqlalchemy import Column, JSON, Text
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


class Artifact(SQLModel, table=True):
    __tablename__ = "artifacts"

    id: int | None = Field(default=None, primary_key=True)
    job_id: uuid.UUID = Field(foreign_key="jobs.id", index=True)
    kind: str = Field(default="file", index=True)
    name: str
    path: str = Field(sa_column=Column(Text))
    size_bytes: int = 0
    created_at: datetime = Field(default_factory=utcnow, index=True)


class ArtifactPublic(SQLModel):
    id: int
    job_id: uuid.UUID
    kind: str
    name: str
    path: str
    size_bytes: int
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
