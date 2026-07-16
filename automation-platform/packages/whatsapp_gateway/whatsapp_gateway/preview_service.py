"""Compatibility facade for the modular immutable preview compiler."""
from whatsapp_gateway.previews.compiler.errors import PreviewCompileError, issue
from whatsapp_gateway.previews.artifact_storage import (
    sha256_file, _managed_artifact_path, freeze_artifact, is_managed_preview_artifact,
)
from whatsapp_gateway.previews.compiler.keys import _preview_key
from whatsapp_gateway.previews.compiler.attachments import (
    _artifact_role, _attachment_paths, _xlsx_emis_values, _classify_attachment_wings,
    _classify_scoped_emis_wings,
)
from whatsapp_gateway.previews.compiler.messages import (
    PLACEHOLDER_RE, PHONE_JID_SUFFIX, _render_message, _delivery_status, _canonical_contact_target,
)
from whatsapp_gateway.previews.compiler.routes import (
    _plan_report_type, _plan_recipient_channel, _plan_recipient_scope, _plan_target,
    _plan_route_label, _same_route_value, _plan_matches_audience_route, _retarget_group_plan,
)
from whatsapp_gateway.previews.compiler.orchestrator import compile_antidengue_preview
from whatsapp_gateway.previews.maintenance import (
    preview_is_stale, preview_dict, delete_preview_records, cleanup_unreferenced_preview_files, entity_link_details,
)

__all__ = [name for name in globals() if not name.startswith("__")]
