from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import func, or_
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppIdentityAlias,
    WhatsAppInboundAttachment,
    WhatsAppInboundMessage,
)

router = APIRouter(prefix="/api/v1/whatsapp/inbound", tags=["whatsapp-inbound"])


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


def _verify_worker_token(
    x_whatsapp_worker_token: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> None:
    expected = settings.whatsapp_inbound_ingest_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WHATSAPP_INBOUND_INGEST_TOKEN is not configured",
        )
    if not x_whatsapp_worker_token or x_whatsapp_worker_token != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid worker token")


def _resolve_account(session: Session, worker_key: str) -> WhatsAppAccount:
    account = session.exec(
        select(WhatsAppAccount).where(WhatsAppAccount.worker_key == worker_key)
    ).first()
    if account is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Unknown WhatsApp worker account: {worker_key}",
        )
    return account


def _resolve_contact_id(session: Session, account_id, sender_jid: str):
    contact = session.exec(
        select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account_id,
            or_(
                WhatsAppDirectoryContact.phone_jid == sender_jid,
                WhatsAppDirectoryContact.primary_lid_jid == sender_jid,
            ),
        )
    ).first()
    if contact:
        return contact.id
    alias = session.exec(
        select(WhatsAppIdentityAlias).where(
            WhatsAppIdentityAlias.account_id == account_id,
            WhatsAppIdentityAlias.lid_jid == sender_jid,
        )
    ).first()
    return alias.contact_id if alias else None


@router.post("/events", dependencies=[Depends(_verify_worker_token)])
def ingest_event(
    event: InboundMessageEvent,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account = _resolve_account(session, event.workerId)
    message = session.exec(
        select(WhatsAppInboundMessage).where(
            WhatsAppInboundMessage.account_id == account.id,
            WhatsAppInboundMessage.remote_jid == event.remoteJid,
            WhatsAppInboundMessage.message_id == event.messageId,
        )
    ).first()
    now = utcnow()
    values = {
        "worker_key": event.workerId,
        "participant_jid": event.participantJid,
        "sender_jid": event.senderJid,
        "directory_contact_id": _resolve_contact_id(session, account.id, event.senderJid),
        "from_me": event.fromMe,
        "chat_scope": event.chatScope,
        "message_timestamp": event.messageTimestamp.astimezone(timezone.utc).replace(tzinfo=None),
        "push_name": event.pushName,
        "text_content": event.text,
        "message_type": event.messageType,
        "ingestion_source": event.ingestionSource,
        "payload_sha256": event.payloadSha256,
        "raw_payload": event.rawPayload,
        "last_ingested_at": now,
    }
    created = message is None
    if message is None:
        message = WhatsAppInboundMessage(
            account_id=account.id,
            message_id=event.messageId,
            remote_jid=event.remoteJid,
            **values,
        )
        session.add(message)
        session.flush()
    else:
        for key, value in values.items():
            setattr(message, key, value)

    if event.attachment:
        attachment = session.exec(
            select(WhatsAppInboundAttachment).where(
                WhatsAppInboundAttachment.message_id == message.id
            )
        ).first()
        data = event.attachment.model_dump()
        mapped = {
            "media_kind": data["mediaKind"],
            "message_key": data["messageKey"],
            "original_filename": data.get("originalFilename"),
            "mime_type": data.get("mimeType"),
            "declared_size": data.get("declaredSize"),
            "media_sha256": data.get("mediaSha256"),
            "caption": data.get("caption"),
            "updated_at": now,
        }
        if attachment is None:
            attachment = WhatsAppInboundAttachment(message_id=message.id, **mapped)
            session.add(attachment)
        else:
            for key, value in mapped.items():
                setattr(attachment, key, value)
    session.commit()
    return {"accepted": True, "created": created, "message_id": str(message.id)}


@router.get("/status")
def inbound_status(session: Session = Depends(get_session)) -> dict[str, Any]:
    message_count = session.exec(select(func.count()).select_from(WhatsAppInboundMessage)).one()
    attachment_count = session.exec(select(func.count()).select_from(WhatsAppInboundAttachment)).one()
    bounds = session.exec(
        select(
            func.min(WhatsAppInboundMessage.message_timestamp),
            func.max(WhatsAppInboundMessage.message_timestamp),
        )
    ).one()
    unresolved = session.exec(
        select(func.count()).select_from(WhatsAppInboundMessage).where(
            WhatsAppInboundMessage.directory_contact_id.is_(None),
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
    return {
        "messages": int(message_count),
        "attachments": int(attachment_count),
        "unresolved_messages": int(unresolved),
        "earliest_message_at": bounds[0],
        "latest_message_at": bounds[1],
    }
