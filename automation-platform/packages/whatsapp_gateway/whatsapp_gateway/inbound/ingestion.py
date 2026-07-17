from __future__ import annotations

from datetime import timezone
from typing import Any

from fastapi import Depends
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.time import utcnow
from whatsapp_gateway.inbound.accounts import resolve_account, resolve_contact_id
from whatsapp_gateway.inbound.batches import register_batch_item
from whatsapp_gateway.inbound.history_tracking import record_history_progress
from whatsapp_gateway.inbound.schemas import InboundMessageEvent
from whatsapp_gateway.inbound.upserts import (
    upsert_inbound_attachment,
    upsert_inbound_message,
)
from whatsapp_gateway.directory.master_contacts import ensure_master_contact
from whatsapp_gateway.models import WhatsAppDirectoryContact


def attachment_values(event: InboundMessageEvent, now) -> dict[str, Any] | None:
    if event.attachment is None:
        return None
    data = event.attachment.model_dump()
    return {
        "media_kind": data["mediaKind"],
        "message_key": data["messageKey"],
        "original_filename": data.get("originalFilename"),
        "mime_type": data.get("mimeType"),
        "declared_size": data.get("declaredSize"),
        "media_sha256": data.get("mediaSha256"),
        "caption": data.get("caption"),
        "updated_at": now,
    }


def message_values(
    session: Session,
    *,
    event: InboundMessageEvent,
    account_id,
    now,
) -> dict[str, Any]:
    return {
        "worker_key": event.workerId,
        "participant_jid": event.participantJid,
        "sender_jid": event.senderJid,
        "directory_contact_id": resolve_contact_id(
            session,
            account_id,
            event.senderJid,
        ),
        "from_me": event.fromMe,
        "chat_scope": event.chatScope,
        "message_timestamp": event.messageTimestamp.astimezone(timezone.utc).replace(
            tzinfo=None
        ),
        # Baileys can attach the connected account's own push name to outgoing
        # messages. It is not evidence about the remote contact.
        "push_name": None if event.fromMe else event.pushName,
        "text_content": event.text,
        "message_type": event.messageType,
        "ingestion_source": event.ingestionSource,
        "payload_sha256": event.payloadSha256,
        "raw_payload": event.rawPayload,
        "last_ingested_at": now,
    }


def ingest_event(
    event: InboundMessageEvent,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account = resolve_account(session, event.workerId)
    now = utcnow()
    values = message_values(
        session,
        event=event,
        account_id=account.id,
        now=now,
    )
    message_id, created = upsert_inbound_message(
        session,
        account_id=account.id,
        event=event,
        values=values,
        now=now,
    )

    if values["directory_contact_id"] and event.pushName and not event.fromMe:
        directory_contact = session.get(
            WhatsAppDirectoryContact,
            values["directory_contact_id"],
        )
        if directory_contact is not None:
            ensure_master_contact(
                session,
                directory_contact,
                observed_name=event.pushName,
                name_source="whatsapp_push",
                observed_at=now,
            )

    attachment_id = None
    mapped_attachment = attachment_values(event, now)
    if mapped_attachment is not None:
        attachment_id = upsert_inbound_attachment(
            session,
            message_id=message_id,
            mapped=mapped_attachment,
            now=now,
        )
        register_batch_item(
            session,
            batch_id=event.batchId,
            attachment_id=attachment_id,
            message_id=message_id,
        )

    record_history_progress(
        session,
        account_id=account.id,
        contact_id=values["directory_contact_id"],
        created_message=created,
        has_attachment=event.attachment is not None,
        ingestion_source=event.ingestionSource,
    )
    session.commit()
    return {
        "accepted": True,
        "created": created,
        "message_id": str(message_id),
        "attachment_id": str(attachment_id) if attachment_id else None,
    }
