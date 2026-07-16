"""Compatibility facade for the modular WhatsApp preview API."""
from whatsapp_gateway.previews.router import CHILD_ROUTERS as _CHILD_ROUTERS
from whatsapp_gateway.previews.router import router
from whatsapp_gateway.previews.schemas import *
from whatsapp_gateway.previews.serialization import (
    _artifact_dict,
    _delivery_dict,
    _digits,
    _with_approval,
)
from whatsapp_gateway.previews.options import preview_options
from whatsapp_gateway.previews.creation import create_preview, create_previews_bulk
from whatsapp_gateway.previews.queries import previews, preview_selection, preview_detail
from whatsapp_gateway.previews.approval import approve_preview, approve_previews_bulk
from whatsapp_gateway.previews.deletion import hard_delete_preview, hard_delete_previews_bulk
from whatsapp_gateway.previews.deliveries import preview_deliveries
from whatsapp_gateway.previews.artifacts import preview_artifacts, preview_issues, download_preview_artifact
from whatsapp_gateway.previews.contact_links import contact_links, contact_link_targets, save_contact_link, remove_contact_link
from whatsapp_gateway.tasks import compile_dispatch_preview_job, send_approved_preview_job

if not router.routes:
    raise RuntimeError(
        "WhatsApp preview compatibility facade received an empty router; "
        f"child route counts={[len(item.routes) for item in _CHILD_ROUTERS]}"
    )

__all__ = [name for name in globals() if not name.startswith("__")]
