
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import func
from sqlmodel import Session, select

from automation_core.database import get_session
from whatsapp_gateway.models import WhatsAppInboundAttachment, WhatsAppInboundMessage

router = APIRouter()

@router.get("/status")
def inbound_status(session: Session = Depends(get_session)) -> dict[str, Any]:
    message_count = session.exec(
        select(func.count()).select_from(WhatsAppInboundMessage)
    ).one()
    attachment_count = session.exec(
        select(func.count()).select_from(WhatsAppInboundAttachment)
    ).one()
    archived_count = session.exec(
        select(func.count())
        .select_from(WhatsAppInboundAttachment)
        .where(WhatsAppInboundAttachment.download_status == "archived")
    ).one()
    bounds = session.exec(
        select(
            func.min(WhatsAppInboundMessage.message_timestamp),
            func.max(WhatsAppInboundMessage.message_timestamp),
        )
    ).one()
    unresolved = session.exec(
        select(func.count())
        .select_from(WhatsAppInboundMessage)
        .where(
            WhatsAppInboundMessage.directory_contact_id.is_(None),
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
    return {
        "messages": int(message_count),
        "attachments": int(attachment_count),
        "archived_attachments": int(archived_count),
        "unresolved_messages": int(unresolved),
        "earliest_message_at": bounds[0],
        "latest_message_at": bounds[1],
    }
