from __future__ import annotations

from datetime import datetime

from sqlmodel import Session

from master_data.contacts import (
    get_or_create_contact,
    master_contact_dict,
    observe_contact_name,
    set_manual_contact_name,
)
from master_data.models import MasterContact
from whatsapp_gateway.models import WhatsAppDirectoryContact


def _phone_value(contact: WhatsAppDirectoryContact) -> str | None:
    return contact.phone_jid or None


def ensure_master_contact(
    session: Session,
    directory_contact: WhatsAppDirectoryContact,
    *,
    observed_name: object = None,
    name_source: str = "unknown",
    name_verified: bool = False,
    observed_at: datetime | None = None,
) -> MasterContact:
    master = (
        session.get(MasterContact, directory_contact.master_contact_id)
        if directory_contact.master_contact_id
        else None
    )
    if master is None:
        master = get_or_create_contact(
            session,
            mobile=_phone_value(directory_contact),
            name=directory_contact.display_name,
            name_source=("whatsapp_profile" if directory_contact.display_name else "unknown"),
            observed_at=observed_at,
        )
        directory_contact.master_contact_id = master.id
        session.add(directory_contact)
    if observed_name:
        observe_contact_name(
            master,
            observed_name,
            source=name_source,
            verified=name_verified,
            observed_at=observed_at,
        )
        session.add(master)
    return master


def set_directory_master_name(
    session: Session,
    directory_contact: WhatsAppDirectoryContact,
    name: object,
) -> MasterContact:
    master = ensure_master_contact(session, directory_contact)
    return set_manual_contact_name(session, master, name)


def resolved_contact_name(
    session: Session,
    directory_contact: WhatsAppDirectoryContact,
) -> str:
    master = (
        session.get(MasterContact, directory_contact.master_contact_id)
        if directory_contact.master_contact_id
        else None
    )
    return str(resolved_contact_dict(directory_contact, master)["display_name"] or "")


def resolved_contact_dict(
    directory_contact: WhatsAppDirectoryContact,
    master: MasterContact | None,
) -> dict[str, object]:
    master_name = master.name if master and master.active else ""
    channel_name = directory_contact.display_name.strip()
    return {
        "display_name": master_name or channel_name,
        "channel_display_name": channel_name,
        "master_contact": master_contact_dict(master) if master else None,
        "name_source": (
            master.name_source if master_name else ("whatsapp_profile" if channel_name else "unknown")
        ),
        "name_verified": bool(master and master_name and master.name_verified),
    }
