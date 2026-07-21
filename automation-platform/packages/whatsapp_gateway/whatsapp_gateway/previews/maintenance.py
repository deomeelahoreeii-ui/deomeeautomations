from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlmodel import Session

from master_data.models import School, SchoolHead, Wing
from whatsapp_gateway.models import (
    WhatsAppContactLink, WhatsAppDispatchPreview, WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
)
from whatsapp_gateway.previews.artifact_storage import is_managed_preview_artifact
from whatsapp_gateway.previews.staleness import preview_is_stale, preview_stale_reasons
from whatsapp_gateway.previews.state import summarize_preview_state

def preview_dict(session: Session, preview: WhatsAppDispatchPreview, *, check_files: bool = False) -> dict[str, Any]:
    stale_reasons = preview_stale_reasons(session, preview, check_files=check_files)
    stale = bool(stale_reasons)
    all_deliveries = list(session.scalars(select(WhatsAppDispatchPreviewDelivery).where(
        WhatsAppDispatchPreviewDelivery.preview_id == preview.id,
    )).all())
    deliveries = [item for item in all_deliveries if item.status != "skipped"]
    artifacts = list(session.scalars(select(WhatsAppDispatchPreviewArtifact).where(
        WhatsAppDispatchPreviewArtifact.preview_id == preview.id,
    )).all())
    summary = summarize_preview_state(all_deliveries, preview.issues or [], artifacts)
    effective_status = summary.status
    unique_recipient_count = len({item.target_jid for item in deliveries if item.target_jid})
    jurisdiction_ids = {
        scope_id
        for item in deliveries
        for scope_id in (
            ((item.routing_snapshot or {}).get("dynamic_audience") or {}).get("markaz_ids")
            or []
        )
    }
    return {
        "id": str(preview.id),
        "preview_key": preview.preview_key,
        "application_id": str(preview.application_id),
        "source_job_id": str(preview.source_job_id) if preview.source_job_id else None,
        "source_kind": preview.source_kind,
        "source_reference_id": str(preview.source_reference_id) if preview.source_reference_id else None,
        "source_revision": preview.source_revision,
        "dispatch_profile_id": str(preview.dispatch_profile_id),
        "status": "stale" if stale else effective_status,
        "display_status": (
            "stale" if stale else
            "ready_with_exclusions" if summary.partial_approval_available else
            effective_status
        ),
        "stored_status": preview.status,
        "stale": stale,
        "stale_reasons": stale_reasons,
        "profile_version": preview.profile_version,
        "application_name": preview.application_name,
        "report_type_name": preview.report_type_name,
        "audience_name": preview.audience_name,
        "profile_name": preview.profile_name,
        "account_name": preview.account_name,
        "template_name": preview.template_name,
        "wing_name": preview.wing_name,
        "ready_count": summary.ready_delivery_count,
        "warning_count": summary.warning_delivery_count,
        "blocked_count": summary.blocked_delivery_count,
        "skipped_count": summary.skipped_delivery_count,
        "delivery_count": summary.delivery_count,
        "eligible_delivery_count": summary.eligible_delivery_count,
        "excluded_delivery_count": summary.excluded_delivery_count,
        "partial_approval_available": summary.partial_approval_available,
        "warning_issue_count": summary.warning_issue_count,
        "blocked_issue_count": summary.blocked_issue_count,
        "batch_warning_count": summary.batch_warning_count,
        "batch_blocked_count": summary.batch_blocked_count,
        "unique_recipient_count": unique_recipient_count,
        "jurisdiction_count": len(jurisdiction_ids),
        "additional_charge_route_count": max(0, len(deliveries) - unique_recipient_count),
        "artifact_count": preview.artifact_count,
        "issues": preview.issues,
        "configuration_snapshot": preview.configuration_snapshot,
        "content_sha256": preview.content_sha256,
        "created_by": preview.created_by,
        "created_at": preview.created_at,
        "frozen_at": preview.frozen_at,
    }

def delete_preview_records(
    session: Session,
    preview: WhatsAppDispatchPreview,
) -> set[Path]:
    """Delete one immutable preview and return managed files eligible for cleanup."""
    deliveries = session.scalars(
        select(WhatsAppDispatchPreviewDelivery).where(
            WhatsAppDispatchPreviewDelivery.preview_id == preview.id
        )
    ).all()
    snapshots = session.scalars(
        select(WhatsAppDispatchPreviewArtifact).where(
            WhatsAppDispatchPreviewArtifact.preview_id == preview.id
        )
    ).all()
    paths = {
        Path(item.path_snapshot)
        for item in snapshots
        if is_managed_preview_artifact(Path(item.path_snapshot))
    }
    for item in deliveries:
        session.delete(item)
    for item in snapshots:
        session.delete(item)
    session.flush()
    session.delete(preview)
    session.flush()
    return paths

def cleanup_unreferenced_preview_files(session: Session, paths: set[Path]) -> None:
    for path in paths:
        remaining = session.scalar(
            select(WhatsAppDispatchPreviewArtifact.id).where(
                WhatsAppDispatchPreviewArtifact.path_snapshot == str(path)
            )
        )
        if remaining is None:
            path.unlink(missing_ok=True)

def entity_link_details(session: Session, link: WhatsAppContactLink) -> dict[str, Any]:
    wing = session.get(Wing, link.wing_id)
    if link.entity_type == "officer":
        from master_data.models import Officer

        entity = session.get(Officer, link.officer_id)
        name = entity.name if entity else "Deleted officer"
        detail = entity.role.upper() if entity else "Officer"
    else:
        head = session.get(SchoolHead, link.school_head_id)
        school = session.get(School, head.school_id) if head else None
        name = head.name if head else "Deleted school head"
        detail = f"{school.emis} · {school.name}" if school else "School head"
    return {
        "id": str(link.id),
        "contact_id": str(link.directory_contact_id),
        "entity_type": link.entity_type,
        "entity_key": link.entity_key,
        "entity_name": name,
        "entity_detail": detail,
        "wing_id": str(link.wing_id),
        "wing_name": wing.name if wing else "Unknown wing",
        "status": link.status,
        "source": link.source,
        "confidence": link.confidence,
        "active": link.active,
        "updated_at": link.updated_at,
    }
