from __future__ import annotations

from typing import Any

from whatsapp_gateway.antidengue_renderer import (
    HOTSPOT_RENDERER_KEY, HOTSPOT_REPORT_KEY,
    RENDERER_KEY as TEHSIL_DORMANT_RENDERER_KEY, WING_RENDERER_KEY,
)
from whatsapp_gateway.models import WhatsAppDispatchPreview
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.keys import preview_key


def configuration_snapshot(ctx: CompileContext) -> dict[str, Any]:
    application, profile, report_type = ctx.application, ctx.profile, ctx.report_type
    audience, account, template, wing = ctx.audience, ctx.account, ctx.template, ctx.wing
    recipient_scope = ctx.recipient_scope
    snapshot = {
        "application": {"id": str(application.id), "key": application.key, "name": application.name},
        "profile": {
            "id": str(profile.id), "key": profile.key, "name": profile.name, "version": profile.version,
            "delivery_mode": profile.delivery_mode, "require_approval": profile.require_approval,
            "recipient_channel": profile.recipient_channel,
            "recipient_scope_id": str(recipient_scope.id) if recipient_scope else None,
            "recipient_scope_key": recipient_scope.key if recipient_scope else None,
            "recipient_scope_name": recipient_scope.name if recipient_scope else None,
            "presentation_policy": profile.presentation_policy or {},
        },
        "report_type": {"id": str(report_type.id), "key": report_type.key, "name": report_type.name},
        "audience": {"id": str(audience.id), "key": audience.key, "name": audience.name},
        "account": {"id": str(account.id), "worker_key": account.worker_key, "name": account.name},
        "template": {"id": str(template.id) if template else None, "key": template.key if template else None,
            "name": template.name if template else None, "body": template.body if template else None},
        "wing": {"id": str(wing.id), "code": wing.code, "name": wing.name},
        "renderer": {"key": TEHSIL_DORMANT_RENDERER_KEY if report_type.key == "tehsil_dormant_summary"
            else WING_RENDERER_KEY if report_type.key == "wing_summary"
            else HOTSPOT_RENDERER_KEY if report_type.key == HOTSPOT_REPORT_KEY
            else "legacy.dispatch_plan_adapter"},
    }
    snapshot["audience"]["target_keys"] = sorted(member.target_key for member in ctx.members)
    snapshot["audience"]["target_routes"] = sorted(
        f"{member.target_key}:{member.route_scope_key}:{member.route_scope_value}" for member in ctx.members
    )
    return snapshot


def create_preview_record(
    ctx: CompileContext, *, batch_issues: list[dict[str, Any]], snapshot: dict[str, Any], created_by: str
) -> WhatsAppDispatchPreview:
    preview = WhatsAppDispatchPreview(
        preview_key=preview_key(),
        application_id=ctx.application.id, source_job_id=ctx.source_job.id, dispatch_profile_id=ctx.profile.id,
        profile_version=ctx.profile.version, application_name=ctx.application.name,
        report_type_name=ctx.report_type.name, audience_name=ctx.audience.name, profile_name=ctx.profile.name,
        account_name=ctx.account.name, template_name=ctx.template.name if ctx.template else "Exact dry-run message",
        wing_name=ctx.wing.name, issues=batch_issues, configuration_snapshot=snapshot, created_by=created_by,
    )
    ctx.session.add(preview)
    ctx.session.flush()
    return preview
