from __future__ import annotations

import uuid

from sqlmodel import Field, SQLModel


class SheetFilterJobRequest(SQLModel):
    source_file_id: uuid.UUID


class PdfFilterJobRequest(SQLModel):
    input_dir: str = Field(default="crm-main-complaints")
    output_dir: str = Field(default="phase1-crm/unprocessed-crm/filtered")
    db: str = Field(default="crm-cache.sqlite")
    skip_paperless_refresh: bool = Field(default=False)
    paperless_limit: int | None = Field(default=None, ge=1)
