from __future__ import annotations

import hashlib
import json
import uuid

from sqlalchemy import select
from sqlmodel import Session

from whatsapp_gateway.models import (
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchProfile,
)
from whatsapp_gateway.previews.compiler.artifact_snapshots import ArtifactSnapshotStore
from whatsapp_gateway.previews.compiler.context import load_compile_context
from whatsapp_gateway.previews.compiler.deliveries import persist_deliveries
from whatsapp_gateway.previews.compiler.finalize import finalize_preview
from whatsapp_gateway.previews.compiler.issue_reconciliation import reconcile_zero_result_issues
from whatsapp_gateway.previews.compiler.plans import build_dispatch_plan
from whatsapp_gateway.previews.compiler.preview_record import configuration_snapshot, create_preview_record

def compile_antidengue_preview(
    session: Session, *, source_job_id: uuid.UUID, dispatch_profile_id: uuid.UUID | None = None,
    dispatch_profile_ids: list[uuid.UUID] | None = None, created_by: str = "web",
    compiler_runtime: dict | None = None, deadline_snapshot: dict | None = None,
) -> WhatsAppDispatchPreview:
    profile_ids = _profile_ids(dispatch_profile_ids, dispatch_profile_id)
    if len(profile_ids) > 1:
        return _compile_many(session, source_job_id, profile_ids, created_by, compiler_runtime, deadline_snapshot)
    ctx = load_compile_context(session, source_job_id=source_job_id, dispatch_profile_id=profile_ids[0],
                               deadline_snapshot=deadline_snapshot)
    plan_result = build_dispatch_plan(ctx)
    snapshot = configuration_snapshot(ctx)
    snapshot["compiler_runtime"] = dict(compiler_runtime or {})
    preview = create_preview_record(ctx, batch_issues=plan_result.batch_issues, snapshot=snapshot, created_by=created_by)
    artifact_store = ArtifactSnapshotStore(ctx, preview, plan_result.dispatch_plan)
    artifact_store.snapshot_all()
    persist_deliveries(ctx, preview, artifact_store)
    return finalize_preview(ctx, preview, artifact_store, plan_result.batch_issues, snapshot)


def _profile_ids(values: list[uuid.UUID] | None, primary: uuid.UUID | None) -> list[uuid.UUID]:
    profile_ids = sorted(set(values or ([] if primary is None else [primary])), key=str)
    if not profile_ids:
        raise ValueError("At least one dispatch profile is required")
    return profile_ids


def _compile_many(
    session: Session, source_job_id: uuid.UUID, profile_ids: list[uuid.UUID], created_by: str,
    compiler_runtime: dict | None, deadline_snapshot: dict | None,
) -> WhatsAppDispatchPreview:
    previews = [
        _compile_one(
            session, source_job_id=source_job_id, dispatch_profile_id=value,
            created_by=created_by, compiler_runtime=compiler_runtime,
            deadline_snapshot=deadline_snapshot,
        )
        for value in profile_ids
    ]
    return _merge_profile_previews(session, previews)


def _compile_one(
    session: Session, *, source_job_id: uuid.UUID, dispatch_profile_id: uuid.UUID,
    created_by: str, compiler_runtime: dict | None, deadline_snapshot: dict | None,
) -> WhatsAppDispatchPreview:
    ctx = load_compile_context(
        session, source_job_id=source_job_id, dispatch_profile_id=dispatch_profile_id,
        deadline_snapshot=deadline_snapshot,
    )
    plan_result = build_dispatch_plan(ctx)
    snapshot = configuration_snapshot(ctx)
    snapshot["compiler_runtime"] = dict(compiler_runtime or {})
    preview = create_preview_record(ctx, batch_issues=plan_result.batch_issues, snapshot=snapshot, created_by=created_by)
    artifact_store = ArtifactSnapshotStore(ctx, preview, plan_result.dispatch_plan)
    artifact_store.snapshot_all()
    persist_deliveries(ctx, preview, artifact_store)
    return finalize_preview(ctx, preview, artifact_store, plan_result.batch_issues, snapshot)


def _merge_profile_previews(
    session: Session, previews: list[WhatsAppDispatchPreview]
) -> WhatsAppDispatchPreview:
    """Collapse profile previews into one shared exact send plan.

    Exact duplicate recipient/message/attachment payloads are retained once. If
    profiles resolve the same recipient to different frozen payloads, every
    competing delivery is blocked before approval.
    """
    primary = previews[0]
    profile_snapshots = [dict(item.configuration_snapshot or {}) for item in previews]
    profiles = [session.get(WhatsAppDispatchProfile, item.dispatch_profile_id) for item in previews]
    account_ids = {item.account_id for item in profiles if item is not None}
    batch_issues = list(primary.issues or [])
    if len(account_ids) > 1:
        batch_issues.append({
            "code": "conflicting_profile_accounts",
            "severity": "blocked",
            "message": "Selected routing profiles use different WhatsApp accounts and cannot share one send plan.",
        })

    for extra in previews[1:]:
        batch_issues.extend(extra.issues or [])
        for artifact in session.scalars(
            select(WhatsAppDispatchPreviewArtifact).where(
                WhatsAppDispatchPreviewArtifact.preview_id == extra.id
            )
        ).all():
            artifact.preview_id = primary.id
            session.add(artifact)
        for delivery in session.scalars(
            select(WhatsAppDispatchPreviewDelivery).where(
                WhatsAppDispatchPreviewDelivery.preview_id == extra.id
            )
        ).all():
            delivery.preview_id = primary.id
            session.add(delivery)
        session.delete(extra)
    session.flush()

    artifacts = session.scalars(select(WhatsAppDispatchPreviewArtifact).where(
        WhatsAppDispatchPreviewArtifact.preview_id == primary.id
    )).all()
    artifacts_by_id = {str(item.id): item for item in artifacts}
    deliveries = list(session.scalars(select(WhatsAppDispatchPreviewDelivery).where(
        WhatsAppDispatchPreviewDelivery.preview_id == primary.id
    ).order_by(WhatsAppDispatchPreviewDelivery.sequence)))

    exact_seen: dict[tuple[str, str, tuple[str, ...], str, tuple[str, ...]], WhatsAppDispatchPreviewDelivery] = {}
    by_target_report: dict[tuple[str, str, str, tuple[str, ...]], list[WhatsAppDispatchPreviewDelivery]] = {}
    retained: list[WhatsAppDispatchPreviewDelivery] = []
    for delivery in deliveries:
        checksums = tuple(
            artifacts_by_id[item].checksum_sha256
            for item in delivery.attachment_ids
            if item in artifacts_by_id
        )
        report_type_id = str((delivery.routing_snapshot or {}).get("report_type_id") or "")
        route_identity = tuple(sorted(str(value) for value in (
            (delivery.routing_snapshot or {}).get("route_identity") or []
        )))
        exact_key = (delivery.target_jid, report_type_id, route_identity, delivery.message, checksums)
        if exact_key in exact_seen:
            session.delete(delivery)
            continue
        exact_seen[exact_key] = delivery
        by_target_report.setdefault(
            (delivery.target_type, delivery.target_jid, report_type_id, route_identity), []
        ).append(delivery)
        retained.append(delivery)

    conflict_issue = {
        "code": "conflicting_profile_payloads",
        "severity": "blocked",
        "message": "Overlapping routing profiles resolve this recipient to different messages or attachments.",
    }
    for candidates in by_target_report.values():
        payloads = {
            (item.message, tuple(artifacts_by_id[value].checksum_sha256 for value in item.attachment_ids if value in artifacts_by_id))
            for item in candidates
        }
        if len(payloads) > 1:
            for item in candidates:
                item.issues = [*list(item.issues or []), conflict_issue]
                item.status = "blocked"
                session.add(item)

    for sequence, delivery in enumerate(retained, start=1):
        delivery.sequence = sequence
        checksum_key = [artifacts_by_id[value].checksum_sha256 for value in delivery.attachment_ids if value in artifacts_by_id]
        delivery.idempotency_key = hashlib.sha256(json.dumps({
            "preview": str(primary.id), "target": delivery.target_jid,
            "report_type_id": str((delivery.routing_snapshot or {}).get("report_type_id") or ""),
            "route_identity": (delivery.routing_snapshot or {}).get("route_identity") or [],
            "message": delivery.message, "attachments": checksum_key,
        }, sort_keys=True).encode()).hexdigest()
        session.add(delivery)

    snapshot = dict(primary.configuration_snapshot or {})
    snapshot["profiles"] = [item.get("profile", {}) for item in profile_snapshots]
    snapshot["profile_count"] = len(previews)
    primary.configuration_snapshot = snapshot
    primary.profile_name = f"{len(previews)} routing profiles"
    primary.audience_name = "Multiple audiences"
    primary.report_type_name = "Multiple reports"
    primary.wing_name = "Multiple wings"
    # Run-level quality issues are copied into every per-profile preview. Keep
    # one canonical copy when those previews are combined, while preserving
    # genuinely different profile-specific issue payloads.
    deduplicated_batch_issues: list[dict] = []
    seen_batch_issues: set[str] = set()
    for batch_issue in batch_issues:
        identity = json.dumps(batch_issue, sort_keys=True, ensure_ascii=False, default=str)
        if identity in seen_batch_issues:
            continue
        seen_batch_issues.add(identity)
        deduplicated_batch_issues.append(batch_issue)
    batch_issues = reconcile_zero_result_issues(deduplicated_batch_issues, retained)
    primary.issues = batch_issues
    primary.delivery_count = len(retained)
    primary.ready_count = sum(item.status == "ready" for item in retained)
    primary.skipped_count = sum(item.status == "skipped" for item in retained)
    primary.warning_count = sum(
        issue.get("severity") == "warning" for item in retained for issue in item.issues
    ) + sum(issue.get("severity") == "warning" for issue in batch_issues)
    primary.blocked_count = sum(
        issue.get("severity") == "blocked" for item in retained for issue in item.issues
    ) + sum(issue.get("severity") == "blocked" for issue in batch_issues)
    primary.artifact_count = len(artifacts)
    primary.status = "blocked" if primary.blocked_count else "ready"
    frozen = {
        "configuration": snapshot,
        "deliveries": [{
            "sequence": item.sequence, "target": item.target_jid, "message": item.message,
            "attachments": [artifacts_by_id[value].checksum_sha256 for value in item.attachment_ids if value in artifacts_by_id],
            "status": item.status, "issues": item.issues,
        } for item in retained],
    }
    primary.content_sha256 = hashlib.sha256(
        json.dumps(frozen, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    session.add(primary)
    session.commit()
    session.refresh(primary)
    return primary
