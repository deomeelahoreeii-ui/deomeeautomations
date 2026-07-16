
from __future__ import annotations

import uuid
from collections import defaultdict
from typing import Iterable

from sqlmodel import Session, select

from whatsapp_gateway.models import WhatsAppDirectoryContact, WhatsAppIdentityAlias

def _clean_jids(values: Iterable[str | None]) -> set[str]:
    return {str(value).strip() for value in values if value and str(value).strip()}


def build_contact_identity_index(
    session: Session,
) -> dict[tuple[uuid.UUID, str], set[uuid.UUID]]:
    """Map each account-scoped WhatsApp JID to every matching active contact."""

    index: dict[tuple[uuid.UUID, str], set[uuid.UUID]] = defaultdict(set)
    contacts = session.exec(
        select(WhatsAppDirectoryContact).where(WhatsAppDirectoryContact.active.is_(True))
    ).all()
    active_ids = {contact.id for contact in contacts}
    for contact in contacts:
        for jid in _clean_jids([contact.phone_jid, contact.primary_lid_jid]):
            index[(contact.account_id, jid)].add(contact.id)

    aliases = session.exec(select(WhatsAppIdentityAlias)).all()
    for alias in aliases:
        if alias.contact_id in active_ids and alias.lid_jid:
            index[(alias.account_id, alias.lid_jid)].add(alias.contact_id)
    return dict(index)
