
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import and_, func, or_
from sqlmodel import Session, select

from whatsapp_gateway.inbound.common import (
    normalize_media_types,
    normalize_utc_naive,
    require_contact,
)
from whatsapp_gateway.inbound.media_types import classify_attachment_metadata
from whatsapp_gateway.models import (
    WhatsAppDirectoryContact,
    WhatsAppIdentityAlias,
    WhatsAppInboundAttachment,
    WhatsAppInboundMessage,
)

def contact_identity_jids(
    session: Session, contact: WhatsAppDirectoryContact
) -> set[str]:
    values = {
        value
        for value in [contact.phone_jid, contact.primary_lid_jid]
        if value
    }
    values.update(
        session.exec(
            select(WhatsAppIdentityAlias.lid_jid).where(
                WhatsAppIdentityAlias.contact_id == contact.id,
                WhatsAppIdentityAlias.account_id == contact.account_id,
            )
        ).all()
    )
    return {str(value) for value in values if value}


def contact_message_filter(
    session: Session, contact: WhatsAppDirectoryContact
):
    """Return a strict, account-scoped identity predicate for one contact.

    The account constraint must be combined with the identity alternatives via
    ``AND``.  Treating ``account_id`` as an independent ``OR`` clause causes
    every message captured by the same WhatsApp account to match every contact.

    For group chats, ``remote_jid`` is the group JID and must never identify the
    sender.  It is therefore used only for direct chats.
    """

    jids = contact_identity_jids(session, contact)
    if jids:
        identity_match = or_(
            WhatsAppInboundMessage.sender_jid.in_(jids),
            WhatsAppInboundMessage.participant_jid.in_(jids),
            and_(
                WhatsAppInboundMessage.chat_scope == "direct",
                WhatsAppInboundMessage.remote_jid.in_(jids),
            ),
        )
    else:
        # A directory record without any JID can only use its already-resolved
        # contact assignment.  This fallback remains account-scoped.
        identity_match = WhatsAppInboundMessage.directory_contact_id == contact.id

    return and_(
        WhatsAppInboundMessage.account_id == contact.account_id,
        identity_match,
    )


def attachment_matches_category(
    attachment: WhatsAppInboundAttachment,
    categories: set[str],
) -> tuple[bool, str | None]:
    category = attachment.media_category or classify_attachment_metadata(
        media_kind=attachment.media_kind,
        mime_type=attachment.detected_mime_type or attachment.mime_type,
        original_filename=attachment.original_filename,
    )
    return category in categories, category


def find_matching_attachments(
    session: Session,
    *,
    contact_id: uuid.UUID,
    date_from: datetime | None,
    date_to: datetime | None,
    chat_scope: str,
    media_types: Iterable[str],
) -> list[tuple[WhatsAppInboundAttachment, WhatsAppInboundMessage, str]]:
    if chat_scope not in {"direct", "direct_and_groups"}:
        raise ValueError("Invalid chat scope")
    categories = set(normalize_media_types(media_types))
    contact = require_contact(session, contact_id)
    filters: list[Any] = [
        contact_message_filter(session, contact),
        WhatsAppInboundMessage.from_me.is_(False),
    ]
    if chat_scope == "direct":
        filters.append(WhatsAppInboundMessage.chat_scope == "direct")
    normalized_from = normalize_utc_naive(date_from)
    normalized_to = normalize_utc_naive(date_to)
    if normalized_from and normalized_to and normalized_from > normalized_to:
        raise ValueError("The start date must be before or equal to the end date")
    if normalized_from:
        filters.append(WhatsAppInboundMessage.message_timestamp >= normalized_from)
    if normalized_to:
        filters.append(WhatsAppInboundMessage.message_timestamp <= normalized_to)

    rows = session.exec(
        select(WhatsAppInboundAttachment, WhatsAppInboundMessage)
        .join(
            WhatsAppInboundMessage,
            WhatsAppInboundMessage.id == WhatsAppInboundAttachment.message_id,
        )
        .where(*filters)
        .order_by(
            WhatsAppInboundMessage.message_timestamp,
            WhatsAppInboundMessage.message_id,
        )
    ).all()
    matches: list[tuple[WhatsAppInboundAttachment, WhatsAppInboundMessage, str]] = []
    for attachment, message in rows:
        # Preview and export scans are deliberately read-only.  Identity
        # reconciliation is performed by the explicit repair service, never as
        # a side effect of selecting a contact in the web interface.
        is_match, category = attachment_matches_category(attachment, categories)
        if is_match and category:
            matches.append((attachment, message, category))
    return matches


def contact_coverage(
    session: Session,
    contact_id: uuid.UUID,
) -> tuple[datetime | None, datetime | None]:
    contact = require_contact(session, contact_id)
    return session.exec(
        select(
            func.min(WhatsAppInboundMessage.message_timestamp),
            func.max(WhatsAppInboundMessage.message_timestamp),
        ).where(
            contact_message_filter(session, contact),
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
