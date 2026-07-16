from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from master_data.models import Wing
from whatsapp_gateway.models import (
    WhatsAppContactLink, WhatsAppDirectoryContact, WhatsAppDirectoryGroup,
    WhatsAppDispatchPreviewArtifact,
)
from whatsapp_gateway.previews.compiler.routes import _plan_recipient_channel
from whatsapp_gateway.previews.compiler.messages import _canonical_contact_target

@dataclass(slots=True)
class DeliveryState:
    sequence: int
    plan: dict[str, Any]
    attachment_paths: list[Path]
    payload: dict[str, Any]
    requested_target: str
    recipient_name: str
    target_type: str
    target: str
    source_status: str
    source_skipped: bool
    legacy_message: str
    dispatch_route: dict[str, Any]
    route_kind: str
    route_scope: str
    problems: list[dict[str, Any]] = field(default_factory=list)
    native_context: dict[str, Any] = field(default_factory=dict)
    native_issues: list[Any] = field(default_factory=list)
    directory_group: WhatsAppDirectoryGroup | None = None
    directory_contact: WhatsAppDirectoryContact | None = None
    contact_link: WhatsAppContactLink | None = None
    target_wing: Wing | None = None
    attachment_snapshots: list[WhatsAppDispatchPreviewArtifact] = field(default_factory=list)
    message: str = ""


def initialize_delivery_state(sequence: int, plan: dict[str, Any], attachment_paths: list[Path]) -> DeliveryState:
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    requested_target = str(payload.get("target") or plan.get("target") or "").strip()
    recipient_name = str(payload.get("recipient_name") or plan.get("recipient_name") or requested_target)
    target_type = str(payload.get("type") or plan.get("channel") or "contact").lower()
    target_type = "group" if target_type == "group" or requested_target.endswith("@g.us") else "contact"
    target = requested_target if target_type == "group" else _canonical_contact_target(requested_target)
    source_status = str(plan.get("status") or "planned").strip().lower()
    dispatch_route = payload.get("dispatch_route") if isinstance(payload.get("dispatch_route"), dict) else {}
    route_kind = str(dispatch_route.get("route_kind") or plan.get("route_kind") or "")
    route_scope = str(dispatch_route.get("markaz") or dispatch_route.get("tehsil") or dispatch_route.get("wing")
        or ("District" if route_kind == "district" else ""))
    return DeliveryState(
        sequence=sequence, plan=plan, attachment_paths=attachment_paths, payload=payload,
        requested_target=requested_target, recipient_name=recipient_name, target_type=target_type, target=target,
        source_status=source_status, source_skipped=source_status == "skipped",
        legacy_message=str(payload.get("text") or ""), dispatch_route=dispatch_route,
        route_kind=route_kind, route_scope=route_scope,
        native_context=plan.get("native_context") if isinstance(plan.get("native_context"), dict) else {},
        native_issues=plan.get("native_issues") if isinstance(plan.get("native_issues"), list) else [],
    )
