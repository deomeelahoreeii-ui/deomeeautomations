from __future__ import annotations

from typing import Any

from master_data.models import Officer
from whatsapp_gateway.configuration.dynamic_audiences import ResolvedAudienceMember
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import issue
from whatsapp_gateway.previews.compiler.dynamic_activity_plan import plan_dynamic_activity_member
from whatsapp_gateway.rendering.antidengue.markaz_report import render_aeo_markaz_dormant_report
from whatsapp_gateway.rendering.antidengue.models import MARKAZ_RENDERER_KEY
from whatsapp_gateway.previews.compiler.consolidated_plan import plan_consolidated_dynamic_member


def plan_dynamic_contact_member(
    ctx: CompileContext,
    member: ResolvedAudienceMember,
    batch_issues: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, bool]:
    officer = ctx.session.get(Officer, member.officer_id)
    if officer is None or not officer.active:
        batch_issues.append(issue("inactive_dynamic_recipient", "blocked", "A resolved Master Data officer is no longer active."))
        return None, True
    if ctx.recipient_scope is None or ctx.recipient_scope.key != "aeo":
        batch_issues.append(issue("dynamic_scope_mismatch", "blocked", "The dynamic Markaz audience requires the AEO recipient scope."))
        return None, True
    planner_key = ctx.planner_capability.planner_key if ctx.planner_capability else ""
    if planner_key == "dynamic_digest":
        return plan_consolidated_dynamic_member(ctx, member, officer, batch_issues)
    if planner_key == "dynamic_activity":
        return plan_dynamic_activity_member(ctx, member, officer, batch_issues)
    if planner_key != "dynamic_dormant":
        batch_issues.append(issue(
            "dynamic_report_mismatch", "blocked",
            "The AEO-by-Markaz audience requires the Markaz Dormant Summary report.",
        ))
        return None, True
    try:
        rendered = render_aeo_markaz_dormant_report(
            ctx.session,
            source_job=ctx.source_job,
            wing=ctx.wing,
            markaz_ids=member.scope_ids,
            recipient_name=officer.name,
            deadline=ctx.deadline.label if ctx.deadline else "12:30 PM",
            presentation_policy=ctx.profile.presentation_policy,
        )
    except (ValueError, OSError) as exc:
        batch_issues.append(issue("native_report_error", "blocked", str(exc)))
        return None, True
    if not rendered.schools:
        batch_issues.extend(rendered.issues)
        return None, True
    dynamic_snapshot = {
        "source_id": str(member.source_id),
        "member_id": str(member.id),
        "fingerprint": member.source_fingerprint,
        "officer_id": str(member.officer_id),
        "jurisdiction_ids": [str(value) for value in member.jurisdiction_ids],
        "markaz_ids": [str(value) for value in member.scope_ids],
        "school_ids": [str(value) for value in member.school_ids],
    }
    return {
        "job_id": f"{MARKAZ_RENDERER_KEY}:{member.id}", "status": "planned",
        "channel": "contact", "target": member.target_jid, "recipient_name": officer.name,
        "route_kind": "markaz", "row_count": len(rendered.schools),
        "native_renderer": MARKAZ_RENDERER_KEY, "native_context": rendered.context,
        "native_issues": rendered.issues, "source_artifact_id": rendered.source_artifact_id,
        "source_artifact_sha256": rendered.source_artifact_sha256,
        "scoped_emis": [school.emis for school in rendered.schools],
        "dynamic_audience": dynamic_snapshot,
        "payload": {
            "type": "contact", "target": member.target_jid, "recipient_name": officer.name,
            "text": rendered.message,
            "attachment_paths": [str(path) for path in rendered.attachment_paths],
            "dispatch_route": {
                "route_kind": "markaz", "recipient_scope": "aeo",
                "markaz": member.route_scope_label,
                "markaz_ids": [str(value) for value in member.scope_ids],
                "wing": ctx.wing.name, "wing_id": str(ctx.wing.id),
                "row_count": len(rendered.schools),
                "delivery_kind": "aeo_markaz_dormant_summary",
            },
        },
    }, True
