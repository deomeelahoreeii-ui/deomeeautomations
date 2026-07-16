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


__all__ = [
    'WhatsAppAccount',
]
