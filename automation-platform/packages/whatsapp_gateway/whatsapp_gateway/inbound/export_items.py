
from __future__ import annotations

import uuid

from sqlmodel import Session, select

from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundExportItem,
    WhatsAppInboundMessage,
)

def _load_export_item(
    session: Session, item_id: uuid.UUID
) -> tuple[WhatsAppInboundExportItem, WhatsAppInboundAttachment, WhatsAppInboundMessage]:
    row = session.exec(
        select(
            WhatsAppInboundExportItem,
            WhatsAppInboundAttachment,
            WhatsAppInboundMessage,
        )
        .join(
            WhatsAppInboundAttachment,
            WhatsAppInboundAttachment.id == WhatsAppInboundExportItem.attachment_id,
        )
        .join(
            WhatsAppInboundMessage,
            WhatsAppInboundMessage.id == WhatsAppInboundAttachment.message_id,
        )
        .where(WhatsAppInboundExportItem.id == item_id)
    ).first()
    if row is None:
        raise ValueError(f"Inbound export item not found: {item_id}")
    return row
