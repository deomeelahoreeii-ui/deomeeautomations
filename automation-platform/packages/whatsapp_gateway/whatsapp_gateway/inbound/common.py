
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Iterable

from sqlmodel import Session

from whatsapp_gateway.inbound.media_types import SUPPORTED_CATEGORIES
from whatsapp_gateway.models import WhatsAppDirectoryContact

def normalize_utc_naive(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def normalize_media_types(values: Iterable[str]) -> list[str]:
    normalized = sorted({str(value).strip().lower() for value in values if value})
    invalid = [value for value in normalized if value not in SUPPORTED_CATEGORIES]
    if invalid:
        raise ValueError(f"Unsupported inbound file type selection: {', '.join(invalid)}")
    if not normalized:
        raise ValueError("Select at least one file type")
    return normalized


def require_contact(session: Session, contact_id: uuid.UUID) -> WhatsAppDirectoryContact:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or not contact.active:
        raise ValueError("WhatsApp directory contact was not found or is inactive")
    return contact
