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


__all__ = [
    'WhatsAppDelivery',
    'WhatsAppActivity',
]
