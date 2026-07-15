from __future__ import annotations

import uuid

from sqlmodel import Field, SQLModel


class SheetFilterJobRequest(SQLModel):
    source_file_id: uuid.UUID


class PdfFilterJobRequest(SQLModel):
    source_file_id: uuid.UUID
    paperless_limit: int | None = Field(default=None, ge=1, le=10000)


class PdfBatchProcessRequest(SQLModel):
    paperless_limit: int | None = Field(default=None, ge=1, le=10000)
