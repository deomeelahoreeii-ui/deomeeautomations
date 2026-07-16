
"""Compatibility facade for modular inbound file services."""

from whatsapp_gateway.inbound.common import (
    normalize_media_types,
    normalize_utc_naive,
    require_contact,
)
from whatsapp_gateway.inbound.contact_matching import (
    attachment_matches_category,
    contact_coverage,
    contact_identity_jids,
    contact_message_filter,
    find_matching_attachments,
)
from whatsapp_gateway.inbound.export_packaging import _export_name, package_export
from whatsapp_gateway.inbound.export_preview import build_preview
from whatsapp_gateway.inbound.export_runs import create_export_run, serialize_run

__all__ = [
    "_export_name",
    "attachment_matches_category",
    "build_preview",
    "contact_coverage",
    "contact_identity_jids",
    "contact_message_filter",
    "create_export_run",
    "find_matching_attachments",
    "normalize_media_types",
    "normalize_utc_naive",
    "package_export",
    "require_contact",
    "serialize_run",
]
