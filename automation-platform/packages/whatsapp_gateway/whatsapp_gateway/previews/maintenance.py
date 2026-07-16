from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlmodel import Session

from master_data.models import School, SchoolHead, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppContactLink, WhatsAppDispatchPreview, WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery, WhatsAppDispatchProfile, WhatsAppAudience,
    WhatsAppAudienceMember, WhatsAppDirectoryContact, WhatsAppDirectoryGroup,
    WhatsAppTemplate,
)
from whatsapp_gateway.previews.artifact_storage import is_managed_preview_artifact, sha256_file

def preview_is_stale(session: Session, preview: WhatsAppDispatchPreview, *, check_files: bool = False) -> bool:
    profile = session.get(WhatsAppDispatchProfile, preview.dispatch_profile_id)
    if profile is None or profile.version != preview.profile_version or not profile.enabled:
        return True
    snapshot = preview.configuration_snapshot or {}
    audience_snapshot = snapshot.get("audience") or {}
    audience = session.get(WhatsAppAudience, profile.audience_id)
    if audience is None or not audience.enabled:
        return True
    current_targets = sorted(
        session.scalars(
            select(WhatsAppAudienceMember.target_key).where(
                WhatsAppAudienceMember.audience_id == audience.id,
                WhatsAppAudienceMember.enabled.is_(True),
            )
        ).all()
    )
    if current_targets != sorted(audience_snapshot.get("target_keys") or []):
        return True
    current_routes = sorted(
        f"{member.target_key}:{member.route_scope_key}:{member.route_scope_value}"
        for member in session.scalars(
            select(WhatsAppAudienceMember).where(
                WhatsAppAudienceMember.audience_id == audience.id,
                WhatsAppAudienceMember.enabled.is_(True),
            )
        ).all()
    )
    if current_routes != sorted(audience_snapshot.get("target_routes") or []):
        return True
    account_snapshot = snapshot.get("account") or {}
    account = session.get(WhatsAppAccount, profile.account_id)
    if account is None or not account.enabled or account.worker_key != account_snapshot.get("worker_key"):
        return True
    template_snapshot = snapshot.get("template") or {}
    template_id = template_snapshot.get("id")
    if template_id:
        template = session.get(WhatsAppTemplate, uuid.UUID(template_id))
        if template is None or not template.enabled or template.body != template_snapshot.get("body"):
            return True
    elif profile.template_id is not None:
        return True
    if not check_files:
        return False
    artifacts = session.scalars(
        select(WhatsAppDispatchPreviewArtifact).where(
            WhatsAppDispatchPreviewArtifact.preview_id == preview.id,
            WhatsAppDispatchPreviewArtifact.role == "delivery",
        )
    ).all()
    for artifact in artifacts:
        path = Path(artifact.path_snapshot)
        if not path.is_file() or path.stat().st_size != artifact.size_bytes:
            return True
        if artifact.checksum_sha256 and sha256_file(path) != artifact.checksum_sha256:
            return True
    return False

def preview_dict(session: Session, preview: WhatsAppDispatchPreview, *, check_files: bool = False) -> dict[str, Any]:
    stale = preview_is_stale(session, preview, check_files=check_files)
    return {
        "id": str(preview.id),
        "preview_key": preview.preview_key,
        "application_id": str(preview.application_id),
        "source_job_id": str(preview.source_job_id),
        "dispatch_profile_id": str(preview.dispatch_profile_id),
        "status": "stale" if stale else preview.status,
        "stored_status": preview.status,
        "stale": stale,
        "profile_version": preview.profile_version,
        "application_name": preview.application_name,
        "report_type_name": preview.report_type_name,
        "audience_name": preview.audience_name,
        "profile_name": preview.profile_name,
        "account_name": preview.account_name,
        "template_name": preview.template_name,
        "wing_name": preview.wing_name,
        "ready_count": preview.ready_count,
        "warning_count": preview.warning_count,
        "blocked_count": preview.blocked_count,
        "skipped_count": preview.skipped_count,
        "delivery_count": preview.delivery_count,
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

