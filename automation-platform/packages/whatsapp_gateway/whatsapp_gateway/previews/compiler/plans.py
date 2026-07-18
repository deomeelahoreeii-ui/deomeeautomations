from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import IssueCollector, issue
from whatsapp_gateway.previews.compiler.plan_members import (
    plan_contact_member, plan_group_member,
)
from whatsapp_gateway.previews.compiler.dynamic_plan_members import plan_dynamic_contact_member
from whatsapp_gateway.configuration.dynamic_audiences import ResolvedAudienceMember

@dataclass(slots=True)
class DispatchPlanResult:
    dispatch_plan: list[dict[str, Any]]
    batch_issues: list[dict[str, Any]]
    native_renderer_used: bool


def build_dispatch_plan(ctx: CompileContext) -> DispatchPlanResult:
    batch_issues = IssueCollector()
    dispatch_plan: list[dict[str, Any]] = []
    native_renderer_used = False
    eligible_members = [
        member for member in ctx.members
        if not (ctx.profile.recipient_channel == "group" and member.target_type != "group")
        and not (ctx.profile.recipient_channel == "individual" and member.target_type != "contact")
    ]
    delivery_granularity = getattr(ctx.profile, "delivery_granularity", "recipient")
    scope_members = [
        member for member in eligible_members
        if isinstance(member, ResolvedAudienceMember)
    ] if delivery_granularity == "scope" else []
    invalid_scope_members = [member for member in scope_members if len(member.scope_ids) != 1]
    if invalid_scope_members:
        batch_issues.add_batch(
            issue(
                "invalid_scope_delivery_cardinality", "blocked",
                "Per-jurisdiction profiles require exactly one active Master Data scope per resolved route.",
            ),
            affected_count=len(invalid_scope_members),
            affected_entity_type="jurisdiction route",
        )
    if ctx.planner_capability is None and any(
        isinstance(member, ResolvedAudienceMember) for member in eligible_members
    ):
        batch_issues.add_batch(
            issue(
                "profile_audience_capability_mismatch", "blocked",
                f"{ctx.report_type.name} is not registered for this dynamic audience, channel and recipient scope.",
            ),
            affected_count=len(eligible_members),
            affected_entity_type="recipient",
        )
        return DispatchPlanResult([], list(batch_issues), True)
    for member in eligible_members:
        if ctx.profile.recipient_channel == "group" and member.target_type != "group":
            continue
        if ctx.profile.recipient_channel == "individual" and member.target_type != "contact":
            continue
        affected_key = str(getattr(member, "id", ""))
        affected_label = str(getattr(member, "route_scope_label", "") or getattr(member, "target_key", "") or affected_key)
        entity_type = "AEO recipient" if isinstance(member, ResolvedAudienceMember) else member.target_type
        with batch_issues.affecting(key=affected_key, label=affected_label, entity_type=entity_type):
            if member.target_type == "group":
                plan, native = plan_group_member(ctx, member, batch_issues)
                native_renderer_used = native_renderer_used or native
            elif isinstance(member, ResolvedAudienceMember):
                plan, native = plan_dynamic_contact_member(ctx, member, batch_issues)
                native_renderer_used = native_renderer_used or native
            else:
                plan = plan_contact_member(ctx, member, batch_issues)
        if plan is not None:
            dispatch_plan.append(plan)

    if scope_members:
        planned_scope_routes = sum(bool(plan.get("dynamic_audience")) for plan in dispatch_plan)
        if planned_scope_routes != len(scope_members):
            batch_issues.append(issue(
                "scope_delivery_cardinality_mismatch", "blocked",
                "The compiler did not prepare one delivery for every resolved Master Data jurisdiction.",
                expected_scope_routes=len(scope_members),
                planned_scope_routes=planned_scope_routes,
                missing_scope_routes=max(0, len(scope_members) - planned_scope_routes),
            ))

    quality = ctx.summary.get("quality_gate") if isinstance(ctx.summary, dict) else {}
    quality_issues = (quality.get("issues") or []) if isinstance(quality, dict) else []
    if quality_issues:
        for quality_issue in quality_issues:
            if not isinstance(quality_issue, dict):
                continue
            severity = str(quality_issue.get("severity") or "warning")
            if severity != "warning":
                # A blocked source quality gate never reaches preview compilation.
                continue
            details = {
                key: value
                for key, value in quality_issue.items()
                if key not in {"code", "severity", "message"}
            }
            batch_issues.append(
                issue(
                    str(quality_issue.get("code") or "report_quality_warning"),
                    severity,
                    str(quality_issue.get("message") or "Report quality warning."),
                    **details,
                )
            )
    else:
        # Compatibility with summaries produced before structured quality issues.
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
    return DispatchPlanResult(dispatch_plan, list(batch_issues), native_renderer_used)
