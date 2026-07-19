from __future__ import annotations

import hashlib
import json

from whatsapp_gateway.models import WhatsAppDispatchPreview, WhatsAppDispatchPreviewDelivery
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.delivery_state import DeliveryState
from whatsapp_gateway.previews.compiler.errors import issue
from whatsapp_gateway.previews.compiler.messages import _delivery_status
from whatsapp_gateway.previews.compiler.routes import _plan_recipient_scope


def persist_delivery(
    ctx: CompileContext, preview: WhatsAppDispatchPreview, state: DeliveryState, seen_signatures: set[str]
) -> None:
    dynamic_scope_ids = list(
        (state.plan.get("dynamic_audience") or {}).get("markaz_ids") or []
    )
    route_scope_ids = list(state.dispatch_route.get("scope_ids") or [])
    route_identity = sorted(str(value) for value in (
        dynamic_scope_ids
        or route_scope_ids
        or [state.dispatch_route.get("scope_id") or state.route_scope]
    ) if value)
    dedupe_payload = {
        "target": state.target, "message": state.message,
        "attachments": [item.checksum_sha256 or item.path_snapshot for item in state.attachment_snapshots],
        "route_identity": route_identity,
    }
    signature = hashlib.sha256(json.dumps(dedupe_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    if not state.source_skipped and signature in seen_signatures:
        state.problems.append(issue("duplicate_route", "blocked",
            "This exact recipient, message and attachment route appears more than once."))
    if not state.source_skipped:
        seen_signatures.add(signature)
    idempotency_key = hashlib.sha256(f"{ctx.source_job.id}:{state.sequence}:{signature}".encode("utf-8")).hexdigest()
    status = "skipped" if state.source_skipped else _delivery_status(state.problems)
    delivery = WhatsAppDispatchPreviewDelivery(
        preview_id=preview.id, sequence=state.sequence,
        source_route_key=str(state.plan.get("job_id") or state.payload.get("job_id") or ""),
        target_type=state.target_type, target_name=state.recipient_name, target_jid=state.target,
        directory_group_id=state.directory_group.id if state.directory_group else None,
        directory_contact_id=state.directory_contact.id if state.directory_contact else None,
        contact_link_id=state.contact_link.id if state.contact_link else None,
        wing_id=state.target_wing.id if state.target_wing else None,
        wing_name=state.target_wing.name if state.target_wing else "Unresolved",
        route_kind=state.route_kind, route_scope=state.route_scope, message=state.message,
        attachment_ids=[str(item.id) for item in state.attachment_snapshots],
        routing_snapshot={
            "dispatch_route": state.dispatch_route, "profile_version": ctx.profile.version,
            "delivery_granularity": ctx.profile.delivery_granularity,
            "route_identity": route_identity,
            "audience_id": str(ctx.audience.id), "account_id": str(ctx.account.id),
            "report_type_id": str(ctx.report_type.id),
            "template_id": str(ctx.template.id) if ctx.template else None,
            "legacy_plan_job_id": str(state.plan.get("job_id") or state.payload.get("job_id") or ""),
            "source_status": state.source_status, "source_cause": str(state.plan.get("cause") or ""),
            "requested_target": state.requested_target, "recipient_channel": ctx.profile.recipient_channel,
            "recipient_scope_key": _plan_recipient_scope(state.plan, ctx.report_type.key),
            "native_renderer": str(state.plan.get("native_renderer") or ""),
            "presentation_metadata": state.plan.get("presentation_metadata") or {},
            "source_artifact_id": state.plan.get("source_artifact_id"),
            "source_artifact_sha256": str(state.plan.get("source_artifact_sha256") or ""),
            "scoped_emis": state.plan.get("scoped_emis") or [],
            "delivery_kind": str(state.dispatch_route.get("delivery_kind") or "report"),
            "daily_claim": state.plan.get("daily_claim"),
            "dynamic_audience": state.plan.get("dynamic_audience"),
            "contact_entity_type": state.contact_link.entity_type if state.contact_link else None,
            "contact_entity_key": state.contact_link.entity_key if state.contact_link else (
                f"officer:{state.plan['dynamic_audience']['officer_id']}"
                if state.plan.get("dynamic_audience") else None
            ),
        },
        issues=state.problems, status=status, idempotency_key=idempotency_key,
    )
    ctx.session.add(delivery)
