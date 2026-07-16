
from __future__ import annotations

import uuid
from dataclasses import dataclass

from whatsapp_gateway.models import WhatsAppAccount, WhatsAppInboundAttachment, WhatsAppInboundMessage

@dataclass(frozen=True, slots=True)
class _AccountSnapshot:
    worker_key: str
    health_subject: str


@dataclass(frozen=True, slots=True)
class _AttachmentSnapshot:
    id: uuid.UUID
    original_filename: str | None
    mime_type: str | None


@dataclass(frozen=True, slots=True)
class _MessageSnapshot:
    remote_jid: str
    message_id: str


def _snapshot_account(account: WhatsAppAccount) -> _AccountSnapshot:
    return _AccountSnapshot(
        worker_key=account.worker_key,
        health_subject=account.health_subject,
    )


def _snapshot_attachment(
    attachment: WhatsAppInboundAttachment,
) -> _AttachmentSnapshot:
    return _AttachmentSnapshot(
        id=attachment.id,
        original_filename=attachment.original_filename,
        mime_type=attachment.mime_type,
    )


def _snapshot_message(message: WhatsAppInboundMessage) -> _MessageSnapshot:
    return _MessageSnapshot(
        remote_jid=message.remote_jid,
        message_id=message.message_id,
    )
