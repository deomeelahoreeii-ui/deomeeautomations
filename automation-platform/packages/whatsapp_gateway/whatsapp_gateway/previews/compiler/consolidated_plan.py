from __future__ import annotations

import uuid
from typing import Any

from master_data.models import Officer
from whatsapp_gateway.configuration.dynamic_audiences import ResolvedAudienceMember
from whatsapp_gateway.models import WhatsAppAudienceMember, WhatsAppDirectoryGroup
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import extend_unique_issues, issue
from whatsapp_gateway.rendering.antidengue.digest_models import (
    CONSOLIDATED_DIGEST_RENDERER_KEY,
    CONSOLIDATED_DIGEST_REPORT_KEY,
)
from whatsapp_gateway.rendering.antidengue.digest_report import render_consolidated_action_digest


def _plan(
    ctx: CompileContext, *, member_id: Any, target_type: str, target: str,
    recipient_name: str, scope_key: str, scope_value: str, scope_values: list[str],
    scope_label: str, dynamic_snapshot: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    rendered = render_consolidated_action_digest(
        ctx.session, source_job=ctx.source_job, wing=ctx.wing,
        recipient_name=recipient_name, scope_key=scope_key, scope_value=scope_value,
        scope_values=scope_values, scope_label=scope_label,
        presentation_policy=ctx.profile.presentation_policy,
        deadline=ctx.deadline.label if ctx.deadline else "12:30 PM",
        deadline_timezone=ctx.deadline.timezone if ctx.deadline else "Asia/Karachi",
    )
    if not rendered.message:
        return None, rendered.issues
    route = {
        "route_kind": scope_key, "recipient_scope": scope_key,
        "scope_label": scope_label, "scope_id": scope_value,
        "scope_ids": scope_values, "wing": ctx.wing.name, "wing_id": str(ctx.wing.id),
        "row_count": len(rendered.schools), "delivery_kind": "consolidated_action_digest",
    }
    plan = {
        "job_id": f"{CONSOLIDATED_DIGEST_RENDERER_KEY}:{member_id}", "status": "planned",
        "channel": "contact" if target_type == "contact" else "group", "target": target,
        "recipient_name": recipient_name, "route_kind": scope_key,
        "row_count": len(rendered.schools), "native_renderer": CONSOLIDATED_DIGEST_RENDERER_KEY,
        "native_context": rendered.context, "native_issues": rendered.issues,
        "source_artifact_id": rendered.source_artifact_id,
        "source_artifact_sha256": rendered.source_artifact_sha256,
        "scoped_emis": [school.emis for school in rendered.schools],
        "payload": {
            "type": target_type, "target": target, "recipient_name": recipient_name,
            "text": rendered.message,
            "attachment_paths": [str(path) for path in rendered.attachment_paths],
            "dispatch_route": route,
        },
    }
    if dynamic_snapshot is not None:
        plan["dynamic_audience"] = dynamic_snapshot
    return plan, rendered.issues


def plan_consolidated_group_member(
    ctx: CompileContext,
    member: WhatsAppAudienceMember,
    directory_group: WhatsAppDirectoryGroup,
    batch_issues: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, bool]:
    expected_scope = ctx.recipient_scope.key if ctx.recipient_scope else member.route_scope_key
    if member.route_scope_key != expected_scope or expected_scope not in {"district", "wing", "tehsil", "markaz"}:
        batch_issues.append(issue(
            "audience_scope_mismatch", "blocked",
            f"{directory_group.name} must have a Wing, Tehsil or Markaz binding for the action digest.",
        ))
        return None, True
    if expected_scope in {"district", "wing"}:
        try:
            area_id = uuid.UUID(member.route_scope_value)
        except ValueError:
            batch_issues.append(issue("invalid_audience_area", "blocked", f"{directory_group.name} has an invalid hierarchy-area binding."))
            return None, True
        expected_id = ctx.wing.id if expected_scope == "wing" else ctx.wing.district_id
        if area_id != expected_id:
            batch_issues.append(issue("cross_wing_audience_area", "blocked", f"{directory_group.name} is not bound to {ctx.wing.name}."))
            return None, True
    try:
        plan, native_issues = _plan(
            ctx, member_id=member.id, target_type="group", target=directory_group.jid,
            recipient_name=directory_group.name, scope_key=expected_scope,
            scope_value=member.route_scope_value, scope_values=[member.route_scope_value],
            scope_label=member.route_scope_label or expected_scope.title(),
        )
    except (ValueError, OSError) as exc:
        batch_issues.append(issue("native_report_error", "blocked", str(exc)))
        return None, True
    if plan is None:
        extend_unique_issues(batch_issues, native_issues)
    return plan, True


def plan_consolidated_dynamic_member(
    ctx: CompileContext,
    member: ResolvedAudienceMember,
    officer: Officer,
    batch_issues: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, bool]:
    scope_values = [str(value) for value in member.scope_ids]
    snapshot = {
        "source_id": str(member.source_id), "member_id": str(member.id),
        "fingerprint": member.source_fingerprint, "officer_id": str(member.officer_id),
        "jurisdiction_ids": [str(value) for value in member.jurisdiction_ids],
        "markaz_ids": scope_values, "school_ids": [str(value) for value in member.school_ids],
    }
    try:
        plan, native_issues = _plan(
            ctx, member_id=member.id, target_type="contact", target=member.target_jid,
            recipient_name=officer.name, scope_key="markaz",
            scope_value=scope_values[0] if scope_values else "", scope_values=scope_values,
            scope_label=member.route_scope_label, dynamic_snapshot=snapshot,
        )
    except (ValueError, OSError) as exc:
        batch_issues.append(issue("native_report_error", "blocked", str(exc)))
        return None, True
    if plan is None:
        extend_unique_issues(batch_issues, native_issues)
    return plan, True


__all__ = ["plan_consolidated_dynamic_member", "plan_consolidated_group_member"]
