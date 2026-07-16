from __future__ import annotations

import uuid

from fastapi import HTTPException, status
from sqlalchemy import or_
from sqlmodel import Session, select

from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppIdentityAlias,
)


def resolve_account(session: Session, worker_key: str) -> WhatsAppAccount:
    account = session.exec(
        select(WhatsAppAccount).where(WhatsAppAccount.worker_key == worker_key)
    ).first()
    if account is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Unknown WhatsApp worker account: {worker_key}",
        )
    return account


def resolve_contact_id(
    session: Session,
    account_id: uuid.UUID,
    sender_jid: str,
) -> uuid.UUID | None:
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
