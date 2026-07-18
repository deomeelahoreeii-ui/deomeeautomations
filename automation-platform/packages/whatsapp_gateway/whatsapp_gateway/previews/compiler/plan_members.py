from __future__ import annotations

import copy
import uuid
from typing import Any

from master_data.models import Tehsil
from automation_core.config import get_settings
from whatsapp_gateway.antidengue_renderer import (
    HOTSPOT_RENDERER_KEY, HOTSPOT_REPORT_KEY,
    RENDERER_KEY as TEHSIL_DORMANT_RENDERER_KEY, WING_RENDERER_KEY,
    render_hotspot_distance_report, render_tehsil_dormant_report, render_wing_dormant_report,
    SIMPLE_ACTIVITY_RENDERER_KEY, SIMPLE_ACTIVITY_REPORT_KEY, render_simple_activity_report,
)
from whatsapp_gateway.models import (WhatsAppAudienceMember, WhatsAppDirectoryContact, WhatsAppDirectoryGroup)
from whatsapp_gateway.directory.master_contacts import resolved_contact_name
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import extend_unique_issues, issue
from whatsapp_gateway.previews.compiler.routes import (
    _canonical_contact_target, _plan_matches_audience_route, _plan_recipient_channel,
    _plan_recipient_scope, _plan_target, _retarget_group_plan,
)
from whatsapp_gateway.previews.compiler.zero_result_acknowledgements import (
    build_zero_result_plan,
)

def plan_group_member(
    ctx: CompileContext, member: WhatsAppAudienceMember, batch_issues: list[dict[str, Any]]
) -> tuple[dict[str, Any] | None, bool]:
    session, profile, report_type = ctx.session, ctx.profile, ctx.report_type
    wing, recipient_scope, source_plans = ctx.wing, ctx.recipient_scope, ctx.source_plans
    directory_group = session.get(WhatsAppDirectoryGroup, member.directory_group_id)
    if directory_group is None:
        batch_issues.append(issue("missing_audience_target", "blocked", "An audience group no longer exists in the synchronized directory."))
        return None, False
    expected_scope = recipient_scope.key if recipient_scope else member.route_scope_key
    if report_type.key == SIMPLE_ACTIVITY_REPORT_KEY:
        if member.route_scope_key != expected_scope:
            batch_issues.append(issue("audience_scope_mismatch", "blocked", f"{directory_group.name} must be bound to this profile's {expected_scope or 'configured'} scope."))
            return None, True
        try:
            rendered = render_simple_activity_report(
                session, source_job=ctx.source_job, wing=wing,
                recipient_name=directory_group.name, scope_key=expected_scope,
                scope_value=member.route_scope_value,
                scope_label=member.route_scope_label or expected_scope.title(),
                presentation_policy=profile.presentation_policy,
            )
        except (ValueError, OSError) as exc:
            batch_issues.append(issue("native_report_error", "blocked", str(exc))); return None, True
        extend_unique_issues(batch_issues, rendered.source_issues)
        if not rendered.rows:
            batch_issues.extend(rendered.issues); return None, True
        return {
            "job_id": f"{SIMPLE_ACTIVITY_RENDERER_KEY}:{member.id}", "status": "planned",
            "channel": "group", "target": directory_group.jid,
            "recipient_name": directory_group.name, "route_kind": expected_scope,
            "row_count": len(rendered.rows), "native_renderer": SIMPLE_ACTIVITY_RENDERER_KEY,
            "native_context": rendered.context, "native_issues": rendered.issues,
            "source_artifact_id": rendered.source_artifact_id,
            "source_artifact_sha256": rendered.source_artifact_sha256,
            "scoped_emis": sorted({row.emis for row in rendered.rows if row.emis}),
            "payload": {"type": "group", "target": directory_group.jid,
                "recipient_name": directory_group.name, "text": rendered.message,
                "attachment_paths": [str(path) for path in rendered.attachment_paths],
                "dispatch_route": {"route_kind": expected_scope, "recipient_scope": expected_scope,
                    "scope_label": member.route_scope_label, "scope_id": member.route_scope_value,
                    "wing": wing.name, "wing_id": str(wing.id), "row_count": len(rendered.rows),
                    "delivery_kind": "simple_activity_timing_review"}},
        }, True
    if report_type.key == HOTSPOT_REPORT_KEY:
        if member.route_scope_key != expected_scope:
            batch_issues.append(issue(
                "audience_scope_mismatch", "blocked",
                f"{directory_group.name} must be bound to this profile's {expected_scope or 'configured'} scope.",
            ))
            return None, True
        try:
            rendered = render_hotspot_distance_report(
                session,
                source_job=ctx.source_job,
                wing=wing,
                recipient_name=directory_group.name,
                scope_key=expected_scope,
                scope_value=member.route_scope_value,
                scope_label=member.route_scope_label or expected_scope.title(),
                presentation_policy=profile.presentation_policy,
            )
        except (ValueError, OSError) as exc:
            batch_issues.append(issue("native_report_error", "blocked", str(exc)))
            return None, True
        if not rendered.rows:
            batch_issues.extend(rendered.issues)
            return None, True
        return {
            "job_id": f"{HOTSPOT_RENDERER_KEY}:{member.id}", "status": "planned",
            "channel": "group", "target": directory_group.jid,
            "recipient_name": directory_group.name, "route_kind": expected_scope,
            "row_count": len(rendered.rows), "native_renderer": HOTSPOT_RENDERER_KEY,
            "native_context": rendered.context, "native_issues": rendered.issues,
            "source_artifact_id": rendered.source_artifact_id,
            "source_artifact_sha256": rendered.source_artifact_sha256,
            "scoped_emis": sorted({row.emis for row in rendered.rows if row.emis}),
            "payload": {
                "type": "group", "target": directory_group.jid,
                "recipient_name": directory_group.name, "text": rendered.message,
                "attachment_paths": [str(path) for path in rendered.attachment_paths],
                "dispatch_route": {
                    "route_kind": expected_scope, "recipient_scope": expected_scope,
                    "scope_label": member.route_scope_label, "scope_id": member.route_scope_value,
                    "wing": wing.name, "wing_id": str(wing.id),
                    "row_count": len(rendered.rows), "delivery_kind": "hotspot_distance_review",
                },
            },
        }, True
    if report_type.key == "wing_summary":
        if member.route_scope_key != expected_scope or expected_scope not in {"wing", "district"}:
            batch_issues.append(issue("audience_scope_mismatch", "blocked", f"{directory_group.name} must be bound to this profile's Wing or District scope."))
            return None, True
        try:
            area_id = uuid.UUID(member.route_scope_value)
        except ValueError:
            batch_issues.append(issue("invalid_audience_area", "blocked", f"{directory_group.name} has an invalid hierarchy-area binding."))
            return None, True
        expected_area_id = wing.id if expected_scope == "wing" else wing.district_id
        if area_id != expected_area_id:
            batch_issues.append(issue("cross_wing_audience_area", "blocked", f"{directory_group.name} is not bound to {wing.name}."))
            return None, True
        try:
            rendered = render_wing_dormant_report(
                session, source_job=ctx.source_job, wing=wing, recipient_name=directory_group.name,
                presentation_policy=profile.presentation_policy,
            )
        except (ValueError, OSError) as exc:
            batch_issues.append(issue("native_report_error", "blocked", str(exc)))
            return None, True
        if not rendered.schools:
            batch_issues.extend(rendered.issues)
            return None, True
        return {
            "job_id": f"{WING_RENDERER_KEY}:{member.id}", "status": "planned", "channel": "group",
            "target": directory_group.jid, "recipient_name": directory_group.name,
            "route_kind": expected_scope, "row_count": len(rendered.schools),
            "native_renderer": WING_RENDERER_KEY, "native_context": rendered.context,
            "native_issues": rendered.issues, "source_artifact_id": rendered.source_artifact_id,
            "source_artifact_sha256": rendered.source_artifact_sha256,
            "scoped_emis": [school.emis for school in rendered.schools],
            "payload": {
                "type": "group", "target": directory_group.jid, "recipient_name": directory_group.name,
                "text": rendered.message, "attachment_paths": [str(path) for path in rendered.attachment_paths],
                "dispatch_route": {"route_kind": expected_scope, "recipient_scope": expected_scope,
                    "wing": wing.name, "wing_id": str(wing.id), "row_count": len(rendered.schools)},
            },
        }, True
    if report_type.key == "tehsil_dormant_summary":
        if expected_scope != "tehsil" or member.route_scope_key != "tehsil":
            batch_issues.append(issue("audience_scope_mismatch", "blocked", f"{directory_group.name} must be bound to a Tehsil for {report_type.name}."))
            return None, True
        try:
            tehsil_id = uuid.UUID(member.route_scope_value)
            rendered = render_tehsil_dormant_report(
                session, source_job=ctx.source_job, wing=wing, tehsil_id=tehsil_id,
                recipient_name=directory_group.name, deadline=get_settings().antidengue_submission_deadline,
                presentation_policy=profile.presentation_policy,
            )
        except (ValueError, OSError) as exc:
            batch_issues.append(issue("native_report_error", "blocked", str(exc)))
            return None, True
        if not rendered.schools:
            tehsil = session.get(Tehsil, tehsil_id)
            if tehsil is not None:
                try:
                    acknowledgement = build_zero_result_plan(
                        ctx,
                        member_id=member.id,
                        directory_group=directory_group,
                        tehsil=tehsil,
                    )
                except ValueError as exc:
                    batch_issues.append(issue("acknowledgement_template_error", "blocked", str(exc)))
                    return None, True
                if acknowledgement is not None:
                    acknowledgement["native_issues"] = [
                        item
                        for item in rendered.issues
                        if item.get("code") != "nothing_to_dispatch"
                    ] + list(acknowledgement.get("native_issues") or [])
                    return acknowledgement, True
            batch_issues.extend(rendered.issues)
            return None, True
        return {
            "job_id": f"{TEHSIL_DORMANT_RENDERER_KEY}:{member.id}", "status": "planned",
            "channel": "group", "target": directory_group.jid, "recipient_name": directory_group.name,
            "route_kind": "tehsil", "row_count": len(rendered.schools),
            "native_renderer": TEHSIL_DORMANT_RENDERER_KEY, "native_context": rendered.context,
            "native_issues": rendered.issues, "source_artifact_id": rendered.source_artifact_id,
            "source_artifact_sha256": rendered.source_artifact_sha256,
            "scoped_emis": [school.emis for school in rendered.schools],
            "payload": {
                "type": "group", "target": directory_group.jid, "recipient_name": directory_group.name,
                "text": rendered.message, "attachment_paths": [str(path) for path in rendered.attachment_paths],
                "dispatch_route": {"route_kind": "tehsil", "recipient_scope": "tehsil",
                    "tehsil": member.route_scope_label, "tehsil_id": member.route_scope_value,
                    "row_count": len(rendered.schools)},
            },
        }, True

    exact_target_plan = next((plan for plan in source_plans if _plan_recipient_channel(plan) == "group"
        and _plan_target(plan) == directory_group.jid
        and (not expected_scope or _plan_recipient_scope(plan, report_type.key) == expected_scope)), None)
    if recipient_scope and member.route_scope_key != recipient_scope.key:
        batch_issues.append(issue("audience_scope_mismatch", "blocked", f"{directory_group.name} is not bound to {recipient_scope.name}."))
        return None, False
    if not member.route_scope_key or not member.route_scope_label:
        if exact_target_plan is None:
            batch_issues.append(issue("unbound_audience_route", "blocked", f"Bind {directory_group.name} to the hierarchy area it represents."))
            return None, False
        selected_plan = exact_target_plan
        expected_scope = _plan_recipient_scope(selected_plan, report_type.key)
    else:
        selected_plan = next((plan for plan in source_plans if _plan_matches_audience_route(
            plan, scope_key=member.route_scope_key, scope_label=member.route_scope_label,
            report_type_key=report_type.key)), None)
        if selected_plan is None:
            batch_issues.append(issue("missing_scope_report", "blocked", f"The dry run has no {member.route_scope_key} report for {member.route_scope_label}."))
            return None, False
    return _retarget_group_plan(
        selected_plan, target=directory_group.jid, recipient_name=directory_group.name,
        scope_key=expected_scope or "other", report_is_message_only=report_type.artifact_kind == "message",
    ), False


def plan_contact_member(
    ctx: CompileContext, member: WhatsAppAudienceMember, batch_issues: list[dict[str, Any]]
) -> dict[str, Any] | None:
    directory_contact = ctx.session.get(WhatsAppDirectoryContact, member.directory_contact_id)
    if directory_contact is None:
        batch_issues.append(issue("missing_audience_target", "blocked", "An audience contact no longer exists in the synchronized directory."))
        return None
    targets = {_canonical_contact_target(value) for value in (
        directory_contact.phone_jid, directory_contact.primary_lid_jid, directory_contact.canonical_key
    ) if value}
    selected_plan = next((plan for plan in ctx.source_plans
        if _plan_recipient_channel(plan) == "individual" and _plan_target(plan) in targets
        and (ctx.recipient_scope is None or _plan_recipient_scope(plan, ctx.report_type.key) == ctx.recipient_scope.key)), None)
    if selected_plan is None:
        batch_issues.append(issue("missing_individual_report", "blocked",
            f"The dry run has no matching individual report for {resolved_contact_name(ctx.session, directory_contact) or directory_contact.canonical_key}."))
        return None
    return copy.deepcopy(selected_plan)
