from __future__ import annotations

from typing import Any

from whatsapp_gateway.models import WhatsAppDirectoryGroup
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import issue
from whatsapp_gateway.previews.compiler.zero_result_acknowledgements import build_zero_result_plan


def plan_empty_group_result(
    ctx: CompileContext,
    *,
    member_id,
    directory_group: WhatsAppDirectoryGroup,
    rendered_issues: list[dict[str, Any]],
    batch_issues: list[dict[str, Any]],
    scope_key: str,
    scope_value: str,
    scope_label: str,
    route_metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    try:
        acknowledgement = build_zero_result_plan(
            ctx,
            member_id=member_id,
            target_type="group",
            target_jid=directory_group.jid,
            recipient_name=directory_group.name,
            scope_key=scope_key,
            scope_value=scope_value,
            scope_label=scope_label,
            route_metadata=route_metadata,
        )
    except ValueError as exc:
        batch_issues.append(issue("acknowledgement_template_error", "blocked", str(exc)))
        return None
    if acknowledgement is None:
        batch_issues.extend(rendered_issues)
        return None
    acknowledgement["native_issues"] = [
        item for item in rendered_issues if item.get("code") != "nothing_to_dispatch"
    ] + list(acknowledgement.get("native_issues") or [])
    return acknowledgement


__all__ = ["plan_empty_group_result"]
