
"""Compatibility facade for inbound WhatsApp identity repair."""

from whatsapp_gateway.inbound.identity_index import _clean_jids, build_contact_identity_index
from whatsapp_gateway.inbound.identity_repair_service import repair_inbound_message_identities
from whatsapp_gateway.inbound.identity_resolution import message_identity_jids, resolve_message_contact_id
from whatsapp_gateway.inbound.identity_types import IdentityRepairRow

__all__ = [
    "IdentityRepairRow",
    "_clean_jids",
    "build_contact_identity_index",
    "message_identity_jids",
    "repair_inbound_message_identities",
    "resolve_message_contact_id",
]
