
from __future__ import annotations

import uuid

from whatsapp_gateway.inbound.identity_index import _clean_jids
from whatsapp_gateway.models import WhatsAppInboundMessage

def message_identity_jids(message: WhatsAppInboundMessage) -> set[str]:
    """Return JIDs that can actually identify the sender of this message."""

    values = [message.sender_jid, message.participant_jid]
    if message.chat_scope == "direct":
        values.append(message.remote_jid)
    return _clean_jids(values)


def resolve_message_contact_id(
    message: WhatsAppInboundMessage,
    identity_index: dict[tuple[uuid.UUID, str], set[uuid.UUID]],
) -> tuple[uuid.UUID | None, str]:
    matches: set[uuid.UUID] = set()
    for jid in message_identity_jids(message):
        matches.update(identity_index.get((message.account_id, jid), set()))
    if len(matches) == 1:
        return next(iter(matches)), "resolved"
    if len(matches) > 1:
        return None, "ambiguous"
    return None, "unresolved"
