from __future__ import annotations

import copy
import re
from typing import Any

from whatsapp_gateway.previews.compiler.messages import _canonical_contact_target

def _plan_report_type(plan: dict[str, Any]) -> str:
    """Classify a legacy boundary record into a platform report type."""
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    target = str(payload.get("target") or plan.get("target") or "").strip()
    target_type = str(payload.get("type") or plan.get("channel") or "contact").lower()
    if target_type not in {"group", "contact"}:
        target_type = "group" if target.endswith("@g.us") else "contact"
    if target_type == "contact":
        return "officer_summary"
    dispatch_route = (
        payload.get("dispatch_route")
        if isinstance(payload.get("dispatch_route"), dict)
        else {}
    )
    route_kind = str(
        dispatch_route.get("route_kind") or plan.get("route_kind") or ""
    ).lower()
    return "wing_summary" if route_kind == "district" else "school_activity"

def _plan_recipient_channel(plan: dict[str, Any]) -> str:
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    target = str(payload.get("target") or plan.get("target") or "").strip()
    target_type = str(payload.get("type") or plan.get("channel") or "contact").lower()
    if target_type in {"individual", "contact"}:
        return "individual"
    if target_type == "group" or target.endswith("@g.us"):
        return "group"
    return "individual"

def _plan_recipient_scope(plan: dict[str, Any], report_type_key: str) -> str:
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    dispatch_route = (
        payload.get("dispatch_route")
        if isinstance(payload.get("dispatch_route"), dict)
        else {}
    )
    explicit = str(
        dispatch_route.get("recipient_scope")
        or payload.get("recipient_scope")
        or payload.get("recipient_role")
        or plan.get("recipient_scope")
        or plan.get("role")
        or ""
    ).strip().lower()
    if explicit:
        return re.sub(r"[^a-z0-9]+", "_", explicit).strip("_")
    if _plan_recipient_channel(plan) == "group":
        if report_type_key == "wing_summary":
            return "wing"
        route_kind = str(
            dispatch_route.get("route_kind") or plan.get("route_kind") or ""
        ).lower()
        return route_kind if route_kind in {"district", "tehsil", "markaz"} else "other"
    message = str(payload.get("text") or "")
    role_match = re.search(r"\*?Role:\*?\s*([A-Za-z -]+)", message, re.IGNORECASE)
    role = role_match.group(1).strip().lower() if role_match else ""
    if "ddeo" in role:
        return "ddeo"
    if "aeo" in role:
        return "aeo"
    if role == "deo" or "district education officer" in role:
        return "deo"
    if "head" in role:
        return "school_head"
    return "other"

def _plan_target(plan: dict[str, Any]) -> str:
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    value = str(payload.get("target") or plan.get("target") or "").strip()
    return value if _plan_recipient_channel(plan) == "group" else _canonical_contact_target(value)

def _plan_route_label(plan: dict[str, Any], scope_key: str) -> str:
    payload = plan.get("payload") if isinstance(plan.get("payload"), dict) else {}
    route = payload.get("dispatch_route") if isinstance(payload.get("dispatch_route"), dict) else {}
    if scope_key == "district":
        return "District"
    if scope_key == "wing":
        return "Wing"
    if scope_key == "tehsil":
        return str(route.get("tehsil") or "").strip()
    if scope_key == "markaz":
        return str(route.get("markaz") or "").strip()
    return str(route.get("markaz") or route.get("tehsil") or "").strip()

def _same_route_value(left: str, right: str) -> bool:
    normalize = lambda value: re.sub(r"[^a-z0-9]+", "", value.lower())
    return bool(normalize(left)) and normalize(left) == normalize(right)

def _plan_matches_audience_route(
    plan: dict[str, Any],
    *,
    scope_key: str,
    scope_label: str,
    report_type_key: str,
) -> bool:
    if _plan_recipient_channel(plan) != "group":
        return False
    if _plan_recipient_scope(plan, report_type_key) != scope_key:
        return False
    # District and wing source routes have no PostgreSQL area identifier. The
    # profile wing plus configured group authorization are their exact guards.
    if scope_key in {"district", "wing"}:
        return True
    return _same_route_value(_plan_route_label(plan, scope_key), scope_label)

def _retarget_group_plan(
    plan: dict[str, Any],
    *,
    target: str,
    recipient_name: str,
    scope_key: str,
    report_is_message_only: bool,
) -> dict[str, Any]:
    """Adapt source report data to a PostgreSQL-owned audience destination."""
    resolved = copy.deepcopy(plan)
    payload = resolved.get("payload") if isinstance(resolved.get("payload"), dict) else {}
    resolved["target"] = target
    resolved["recipient_name"] = recipient_name
    resolved["channel"] = "group"
    payload["target"] = target
    payload["recipient_name"] = recipient_name
    payload["type"] = "group"
    route = payload.get("dispatch_route") if isinstance(payload.get("dispatch_route"), dict) else {}
    route["recipient_scope"] = scope_key
    payload["dispatch_route"] = route
    if report_is_message_only:
        payload.pop("excel_path", None)
        payload.pop("image_path", None)
        payload.pop("documents", None)
        resolved.pop("excel_path", None)
    resolved["payload"] = payload
    return resolved

