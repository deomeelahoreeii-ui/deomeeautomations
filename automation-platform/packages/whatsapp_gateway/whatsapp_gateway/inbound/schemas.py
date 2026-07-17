from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class AttachmentEvent(BaseModel):
    mediaKind: str
    messageKey: str
    originalFilename: str | None = None
    mimeType: str | None = None
    declaredSize: int | None = None
    mediaSha256: str | None = None
    caption: str | None = None


class InboundMessageEvent(BaseModel):
    workerId: str
    batchId: uuid.UUID | None = None
    messageId: str
    remoteJid: str
    participantJid: str | None = None
    senderJid: str
    fromMe: bool = False
    chatScope: str
    messageTimestamp: datetime
    pushName: str | None = None
    text: str | None = None
    messageType: str
    ingestionSource: str
    payloadSha256: str
    rawPayload: dict[str, Any] = Field(default_factory=dict)
    attachment: AttachmentEvent | None = None


class InboundFileFilter(BaseModel):
    contact_id: uuid.UUID
    date_from: datetime | None = None
    date_to: datetime | None = None
    chat_scope: str = Field(default="direct", pattern="^(direct|direct_and_groups)$")
    media_types: list[str] = Field(
        default_factory=lambda: ["image", "pdf", "spreadsheet"]
    )


class CreateInboundExportRequest(InboundFileFilter):
    requested_by: str = Field(default="web-operator", max_length=100)


MAX_INBOUND_HISTORY_MESSAGES = 5000


class RequestInboundHistory(BaseModel):
    contact_id: uuid.UUID
    count: int = Field(default=50, ge=1, le=MAX_INBOUND_HISTORY_MESSAGES)
    all_history: bool = False
