from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import issue
from whatsapp_gateway.previews.compiler.plan_members import plan_contact_member, plan_group_member

@dataclass(slots=True)
class DispatchPlanResult:
    dispatch_plan: list[dict[str, Any]]
    batch_issues: list[dict[str, Any]]
    native_renderer_used: bool


def build_dispatch_plan(ctx: CompileContext) -> DispatchPlanResult:
    batch_issues: list[dict[str, Any]] = []
    dispatch_plan: list[dict[str, Any]] = []
    native_renderer_used = False
    for member in ctx.members:
        if ctx.profile.recipient_channel == "group" and member.target_type != "group":
            continue
        if ctx.profile.recipient_channel == "individual" and member.target_type != "contact":
            continue
        if member.target_type == "group":
            plan, native = plan_group_member(ctx, member, batch_issues)
            native_renderer_used = native_renderer_used or native
        else:
            plan = plan_contact_member(ctx, member, batch_issues)
        if plan is not None:
            dispatch_plan.append(plan)

    quality = ctx.summary.get("quality_gate") if isinstance(ctx.summary, dict) else {}
    for warning in (quality.get("warnings") or []) if isinstance(quality, dict) else []:
        batch_issues.append(issue("report_quality_warning", "warning", str(warning)))
    zero_result = (
        (isinstance(quality, dict) and quality.get("final_school_count") == 0)
        or (isinstance(ctx.whatsapp_summary, dict)
            and str(ctx.whatsapp_summary.get("skipped_reason") or "").lower() == "zero inactive schools")
    )
    if not dispatch_plan:
        if zero_result:
            batch_issues.append(issue("nothing_to_dispatch", "warning",
                "The dry run found no dormant schools, so there are no WhatsApp deliveries to preview."))
        elif native_renderer_used and batch_issues:
            pass
        elif ctx.source_dispatch_plan and not batch_issues:
            batch_issues.append(issue("no_audience_routes", "blocked",
                f"The selected audience has no routes for {ctx.report_type.name}."))
        else:
            batch_issues.append(issue("empty_dispatch_plan", "blocked",
                "The dry run did not produce an exact WhatsApp dispatch plan."))
    return DispatchPlanResult(dispatch_plan, batch_issues, native_renderer_used)
