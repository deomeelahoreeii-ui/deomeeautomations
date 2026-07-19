from __future__ import annotations

from typing import Any

from master_data.models import Officer
from whatsapp_gateway.configuration.dynamic_audiences import ResolvedAudienceMember
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import extend_unique_issues, issue
from whatsapp_gateway.rendering.antidengue.hotspot_report import (
    HOTSPOT_RENDERER_KEY,
    HOTSPOT_REPORT_KEY,
    render_hotspot_distance_report,
)
from whatsapp_gateway.rendering.antidengue.simple_activity_report import (
    SIMPLE_ACTIVITY_RENDERER_KEY,
    SIMPLE_ACTIVITY_REPORT_KEY,
    render_simple_activity_report,
)


def plan_dynamic_activity_member(
    ctx: CompileContext,
    member: ResolvedAudienceMember,
    officer: Officer,
    batch_issues: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, bool]:
    report_key = ctx.report_type.key
    scope_values = [str(value) for value in member.scope_ids]
    common = {
        "source_job": ctx.source_job,
        "wing": ctx.wing,
        "recipient_name": officer.name,
        "scope_key": member.route_scope_key,
        "scope_value": member.route_scope_value,
        "scope_values": scope_values,
        "scope_label": member.route_scope_label,
        "presentation_policy": ctx.profile.presentation_policy,
    }
    try:
        if report_key == HOTSPOT_REPORT_KEY:
            rendered = render_hotspot_distance_report(ctx.session, **common)
            renderer_key = HOTSPOT_RENDERER_KEY
            delivery_kind = "aeo_markaz_hotspot_distance_review"
            source_issues: list[dict[str, Any]] = []
        elif report_key == SIMPLE_ACTIVITY_REPORT_KEY:
            rendered = render_simple_activity_report(ctx.session, **common)
            renderer_key = SIMPLE_ACTIVITY_RENDERER_KEY
            delivery_kind = "aeo_markaz_simple_activity_timing_review"
            source_issues = rendered.source_issues
        else:
            return None, False
    except (ValueError, OSError) as exc:
        batch_issues.append(issue("native_report_error", "blocked", str(exc)))
        return None, True
    extend_unique_issues(batch_issues, source_issues)
    if not rendered.rows:
        batch_issues.extend(rendered.issues)
        return None, True
    dynamic_snapshot = {
        "source_id": str(member.source_id),
        "member_id": str(member.id),
        "fingerprint": member.source_fingerprint,
        "officer_id": str(member.officer_id),
        "jurisdiction_ids": [str(value) for value in member.jurisdiction_ids],
        "markaz_ids": scope_values,
        "school_ids": [str(value) for value in member.school_ids],
    }
    return {
        "job_id": f"{renderer_key}:{member.id}", "status": "planned",
        "channel": "contact", "target": member.target_jid, "recipient_name": officer.name,
        "route_kind": "markaz", "row_count": len(rendered.rows),
        "native_renderer": renderer_key, "native_context": rendered.context,
        "native_issues": rendered.issues, "source_artifact_id": rendered.source_artifact_id,
        "source_artifact_sha256": rendered.source_artifact_sha256,
        "scoped_emis": sorted({row.emis for row in rendered.rows if row.emis}),
        "dynamic_audience": dynamic_snapshot,
        "payload": {
            "type": "contact", "target": member.target_jid, "recipient_name": officer.name,
            "text": rendered.message,
            "attachment_paths": [str(path) for path in rendered.attachment_paths],
            "dispatch_route": {
                "route_kind": "markaz", "recipient_scope": "aeo",
                "markaz": member.route_scope_label, "markaz_ids": scope_values,
                "wing": ctx.wing.name, "wing_id": str(ctx.wing.id),
                "row_count": len(rendered.rows), "delivery_kind": delivery_kind,
            },
        },
    }, True
