from __future__ import annotations

from typing import Any

from master_data.models import Officer
from whatsapp_gateway.configuration.dynamic_audiences import ResolvedAudienceMember
from whatsapp_gateway.persistence.configuration import WhatsAppDispatchProfile


def dynamic_runtime_route_rows(
    profile: WhatsAppDispatchProfile,
    member: ResolvedAudienceMember,
    officer: Officer | None,
) -> list[dict[str, Any]]:
    """Expand an aggregated officer target into exact routes for the source dry run."""
    if not member.target_jid:
        return []
    rows: list[dict[str, Any]] = []
    for scope_id, scope_label in zip(member.scope_ids, member.scope_labels, strict=True):
        scope_value = str(scope_id)
        rows.append({
            "enabled": True,
            "name": officer.name if officer else scope_label or profile.name,
            "type": "contact",
            "target": member.target_jid,
            "text": "",
            "image_path": "",
            "excel_path": "",
            "excel_filename": "",
            "attachment_text_mode": "separate",
            "delay_ms": 0,
            "message_mode": "full_report",
            "route_kind": member.route_scope_key,
            "attach_excel": True,
            "manual_only": False,
            "tehsil_ref": scope_value if member.route_scope_key == "tehsil" else "",
            "markaz_ref": scope_value if member.route_scope_key == "markaz" else "",
            "tehsil": scope_label if member.route_scope_key == "tehsil" else "",
            "markaz": scope_label if member.route_scope_key == "markaz" else "",
        })
    return rows
