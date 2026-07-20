from __future__ import annotations

from typing import Any

from master_data.models import Tehsil
from whatsapp_gateway.models import WhatsAppDirectoryGroup
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import issue
from whatsapp_gateway.previews.compiler.zero_result_claims import existing_claim, semantic_key
from whatsapp_gateway.previews.compiler.zero_result_templates import (
    BUSINESS_TIMEZONE,
    PURPOSE,
    resolve_message,
)


def build_zero_result_plan(
    ctx: CompileContext,
    *,
    member_id,
    target_type: str | None = None,
    target_jid: str | None = None,
    recipient_name: str | None = None,
    scope_key: str | None = None,
    scope_value: str | None = None,
    scope_label: str | None = None,
    route_metadata: dict[str, Any] | None = None,
    dynamic_snapshot: dict[str, Any] | None = None,
    # Backward-compatible arguments used by the original Tehsil planner/tests.
    directory_group: WhatsAppDirectoryGroup | None = None,
    tehsil: Tehsil | None = None,
) -> dict[str, Any] | None:
    quality = ctx.summary.get("quality_gate") if isinstance(ctx.summary, dict) else {}
    if isinstance(quality, dict) and quality.get("passed") is False:
        return None

    if directory_group is not None:
        target_type = target_type or "group"
        target_jid = target_jid or directory_group.jid
        recipient_name = recipient_name or directory_group.name
    if tehsil is not None:
        scope_key = scope_key or "tehsil"
        scope_value = scope_value or str(tehsil.id)
        scope_label = scope_label or tehsil.name

    target_type = target_type or "group"
    recipient_channel = "individual" if target_type == "contact" else "group"
    target_jid = (target_jid or "").strip()
    recipient_name = (recipient_name or "").strip()
    scope_key = (scope_key or "").strip().lower()
    scope_value = (scope_value or "").strip()
    scope_label = (scope_label or "").strip()
    if target_type not in {"contact", "group"}:
        raise ValueError(f"Unsupported acknowledgement target type: {target_type}")
    if not target_jid:
        raise ValueError("The zero-result acknowledgement has no WhatsApp target")
    if not scope_key or not scope_value:
        raise ValueError("The zero-result acknowledgement has no stable administrative scope")
    if not scope_label:
        scope_label = recipient_name or scope_key.replace("_", " ").title()
    if not recipient_name:
        recipient_name = scope_label

    template, values, message, business_date = resolve_message(
        ctx,
        recipient_channel=recipient_channel,
        recipient_name=recipient_name,
        scope_key=scope_key,
        scope_value=scope_value,
        scope_label=scope_label,
    )
    claim_key = semantic_key(
        ctx,
        target_jid=target_jid,
        scope_key=scope_key,
        scope_value=scope_value,
        business_date=business_date,
    )
    skipped = existing_claim(
        ctx,
        claim_key=claim_key,
        target_jid=target_jid,
        scope_key=scope_key,
        scope_value=scope_value,
        business_date=business_date,
    ) is not None
    daily_claim = {
        "semantic_key": claim_key,
        "business_date": business_date.isoformat(),
        "purpose": PURPOSE,
        "scope_key": scope_key,
        "scope_value": scope_value,
        "scope_label": scope_label,
        "template_id": str(template.id) if template.id is not None else None,
        "template_key": template.key,
    }
    dispatch_route = {
        "route_kind": scope_key,
        "recipient_scope": ctx.recipient_scope.key if ctx.recipient_scope else scope_key,
        "scope_label": scope_label,
        scope_key: scope_label,
        "scope_id": scope_value,
        "wing": ctx.wing.name,
        "wing_id": str(ctx.wing.id),
        "row_count": 0,
        "delivery_kind": PURPOSE,
        **dict(route_metadata or {}),
    }
    plan: dict[str, Any] = {
        "job_id": f"antidengue.zero_result_acknowledgement:{member_id}:{business_date}",
        "status": "skipped" if skipped else "planned",
        "cause": (
            f"{scope_label} was already acknowledged on {business_date.isoformat()}."
            if skipped
            else ""
        ),
        "skip_issue_code": "acknowledgement_already_sent_today" if skipped else "",
        "skip_issue_severity": "info",
        "channel": "contact" if target_type == "contact" else "group",
        "target": target_jid,
        "recipient_name": recipient_name,
        "route_kind": scope_key,
        "row_count": 0,
        "native_renderer": "antidengue.zero_result_acknowledgement.v2",
        "native_context": values,
        "native_issues": [
            issue(
                "zero_result_acknowledgement_planned",
                "info",
                f"A once-daily zero-dormancy acknowledgement is planned for {scope_label}.",
                template_key=template.key,
                business_date=business_date.isoformat(),
                scope_key=scope_key,
            )
        ],
        "daily_claim": daily_claim,
        "message_template_final": True,
        "message_only": True,
        "payload": {
            "type": target_type,
            "target": target_jid,
            "recipient_name": recipient_name,
            "text": message,
            "attachment_paths": [],
            "dispatch_route": dispatch_route,
        },
    }
    if dynamic_snapshot is not None:
        plan["dynamic_audience"] = dynamic_snapshot
    return plan


__all__ = ["BUSINESS_TIMEZONE", "PURPOSE", "build_zero_result_plan"]
