
from __future__ import annotations

import uuid
from collections import Counter
from datetime import datetime
from typing import Any, Iterable

from sqlmodel import Session

from whatsapp_gateway.inbound.common import normalize_media_types, normalize_utc_naive, require_contact
from whatsapp_gateway.inbound.contact_matching import contact_coverage, find_matching_attachments

def build_preview(
    session: Session,
    *,
    contact_id: uuid.UUID,
    date_from: datetime | None,
    date_to: datetime | None,
    chat_scope: str,
    media_types: Iterable[str],
    item_limit: int = 100,
) -> dict[str, Any]:
    contact = require_contact(session, contact_id)
    normalized_types = normalize_media_types(media_types)
    matches = find_matching_attachments(
        session,
        contact_id=contact.id,
        date_from=date_from,
        date_to=date_to,
        chat_scope=chat_scope,
        media_types=normalized_types,
    )
    earliest, latest = contact_coverage(session, contact.id)
    category_counts = Counter(category for _, _, category in matches)
    status_counts = Counter(attachment.download_status for attachment, _, _ in matches)
    declared_bytes = sum(int(attachment.declared_size or 0) for attachment, _, _ in matches)
    items = []
    for attachment, message, category in matches[:item_limit]:
        items.append(
            {
                "attachment_id": str(attachment.id),
                "message_id": message.message_id,
                "timestamp": message.message_timestamp,
                "category": category,
                "filename": attachment.original_filename
                or f"{category}-{message.message_id}",
                "mime_type": attachment.detected_mime_type or attachment.mime_type,
                "size_bytes": attachment.actual_size or attachment.declared_size,
                "chat_scope": message.chat_scope,
                "caption": attachment.caption or message.text_content,
                "download_status": attachment.download_status,
            }
        )
    return {
        "contact": {
            "id": str(contact.id),
            "display_name": contact.display_name,
            "phone_jid": contact.phone_jid,
            "primary_lid_jid": contact.primary_lid_jid,
        },
        "filters": {
            "date_from": normalize_utc_naive(date_from),
            "date_to": normalize_utc_naive(date_to),
            "chat_scope": chat_scope,
            "media_types": normalized_types,
        },
        "files_found": len(matches),
        "declared_bytes": declared_bytes,
        "category_counts": dict(category_counts),
        "download_status_counts": dict(status_counts),
        "coverage": {
            "earliest_message_at": earliest,
            "latest_message_at": latest,
            "historical_complete": False,
            "note": "Coverage is complete only for messages captured by this worker. Older WhatsApp history may be partial.",
        },
        "items": items,
        "items_truncated": len(matches) > item_limit,
    }
