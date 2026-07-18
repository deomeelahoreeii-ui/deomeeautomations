from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlmodel import Field, SQLModel

from automation_core.time import utcnow


class WhatsAppAudienceSource(SQLModel, table=True):
    """Declarative source whose targets are resolved when a preview is compiled."""

    __tablename__ = "whatsapp_audience_sources"
    __table_args__ = (
        UniqueConstraint(
            "audience_id", "source_type", "recipient_role", "wing_id", "route_scope_key",
            name="uq_whatsapp_audience_sources_selector",
        ),
        CheckConstraint("source_type IN ('master_data_jurisdictions')", name="ck_whatsapp_audience_sources_type"),
        CheckConstraint("recipient_role IN ('aeo', 'ddeo')", name="ck_whatsapp_audience_sources_role"),
        CheckConstraint("route_scope_key IN ('markaz', 'tehsil')", name="ck_whatsapp_audience_sources_scope"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    audience_id: uuid.UUID = Field(foreign_key="whatsapp_audiences.id", index=True)
    source_type: str = Field(default="master_data_jurisdictions", index=True)
    recipient_role: str = Field(index=True)
    wing_id: uuid.UUID = Field(foreign_key="wings.id", index=True)
    route_scope_key: str = Field(index=True)
    aggregate_by_recipient: bool = Field(default=True)
    enabled: bool = Field(default=True, index=True)
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
