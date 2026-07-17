from __future__ import annotations

from datetime import datetime

from sqlmodel import Session, select

from automation_core.time import utcnow
from master_data.models import MasterContact
from master_data.repository import normalize_phone


NAME_SOURCE_PRIORITY = {
    "unknown": 0,
    "whatsapp_push": 20,
    "whatsapp_profile": 30,
    "import": 60,
    "verified_entity": 80,
    "manual": 100,
}


def clean_contact_name(value: object) -> str:
    candidate = " ".join(str(value or "").strip().split())
    candidate = candidate.strip(" *_~`.,;:|/\\-")[:200]
    return candidate if any(character.isalnum() for character in candidate) else ""


def normalized_contact_mobile(value: object) -> str | None:
    value = normalize_phone(value)
    return value or None


def observe_contact_name(
    contact: MasterContact,
    name: object,
    *,
    source: str,
    verified: bool = False,
    observed_at: datetime | None = None,
) -> bool:
    """Apply a sourced name without allowing weak observations to replace authority."""

    candidate = clean_contact_name(name)
    if not candidate:
        return False
    if source not in NAME_SOURCE_PRIORITY:
        raise ValueError(f"Unsupported master-contact name source: {source}")

    current_priority = NAME_SOURCE_PRIORITY.get(contact.name_source, 0)
    incoming_priority = NAME_SOURCE_PRIORITY[source]
    may_replace = (
        not contact.name
        or (verified and not contact.name_verified)
        or (verified == contact.name_verified and incoming_priority >= current_priority)
    )
    if not may_replace:
        return False

    changed = (
        contact.name != candidate
        or contact.name_source != source
        or contact.name_verified != verified
    )
    contact.name = candidate
    contact.name_source = source
    contact.name_verified = verified
    contact.last_observed_at = observed_at or utcnow()
    contact.updated_at = utcnow()
    return changed


def get_or_create_contact(
    session: Session,
    *,
    mobile: object = None,
    name: object = None,
    name_source: str = "unknown",
    name_verified: bool = False,
    observed_at: datetime | None = None,
) -> MasterContact:
    normalized_mobile = normalized_contact_mobile(mobile)
    contact = None
    if normalized_mobile:
        contact = session.scalar(
            select(MasterContact).where(
                MasterContact.normalized_mobile == normalized_mobile
            )
        )
    if contact is None:
        contact = MasterContact(normalized_mobile=normalized_mobile)
    contact.active = True
    if name:
        observe_contact_name(
            contact,
            name,
            source=name_source,
            verified=name_verified,
            observed_at=observed_at,
        )
    session.add(contact)
    session.flush()
    return contact


def set_manual_contact_name(
    session: Session,
    contact: MasterContact,
    name: object,
) -> MasterContact:
    candidate = clean_contact_name(name)
    if not candidate:
        raise ValueError("Contact name is required")
    observe_contact_name(
        contact,
        candidate,
        source="manual",
        verified=True,
    )
    session.add(contact)
    return contact


def master_contact_dict(contact: MasterContact) -> dict[str, object]:
    return {
        "id": str(contact.id),
        "name": contact.name,
        "normalized_mobile": contact.normalized_mobile,
        "name_source": contact.name_source,
        "name_verified": contact.name_verified,
        "active": contact.active,
        "last_observed_at": contact.last_observed_at,
        "updated_at": contact.updated_at,
    }
