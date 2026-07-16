"""Compatibility exports for inbound attachment type detection.

New code should import from :mod:`whatsapp_gateway.inbound.media_types`.
"""

from whatsapp_gateway.inbound.media_types import (
    SUPPORTED_CATEGORIES,
    _EXTENSION_CATEGORY,
    _looks_like_csv,
    _MIME_CATEGORY,
    classify_attachment_metadata,
    detect_file_type,
    normalize_mime,
    safe_filename,
    safe_slug,
)

__all__ = [
    "SUPPORTED_CATEGORIES",
    "classify_attachment_metadata",
    "detect_file_type",
    "normalize_mime",
    "safe_filename",
    "safe_slug",
]
