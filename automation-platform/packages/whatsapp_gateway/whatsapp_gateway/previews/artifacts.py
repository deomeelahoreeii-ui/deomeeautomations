from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from sqlalchemy import func, or_, select
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.job_service import append_log, create_job, get_job, mark_job_failed, set_task_id
from automation_core.models import Job, JobPublic, JobStatus, JobType
from automation_core.time import utcnow
from master_data.models import Officer, School, SchoolHead, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppApplication, WhatsAppAudience, WhatsAppAudienceMember,
    WhatsAppContactLink, WhatsAppDirectoryContact, WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact, WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchApproval, WhatsAppDispatchProfile, WhatsAppDelivery,
    WhatsAppRecipientScope, WhatsAppReportType, WhatsAppSettings,
)
from whatsapp_gateway.preview_service import (
    cleanup_unreferenced_preview_files, delete_preview_records, entity_link_details,
    is_managed_preview_artifact, preview_dict, preview_is_stale, sha256_file,
)
from whatsapp_gateway.tasks import compile_dispatch_preview_job, send_approved_preview_job
from whatsapp_gateway.previews.schemas import (
    PreviewInput, BulkPreviewInput, PreviewIdsInput, BulkPreviewApprovalInput,
    ContactLinkInput, PreviewApprovalInput,
)
from whatsapp_gateway.previews.serialization import _artifact_dict, _delivery_dict, _digits, _with_approval

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp-previews"])

@router.get("/previews/{preview_id}/artifacts")
def preview_artifacts(
    preview_id: uuid.UUID,
    role: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if session.get(WhatsAppDispatchPreview, preview_id) is None:
        raise HTTPException(status_code=404, detail="Dispatch preview not found")
    filters: list[Any] = [WhatsAppDispatchPreviewArtifact.preview_id == preview_id]
    if role:
        filters.append(WhatsAppDispatchPreviewArtifact.role == role)
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDispatchPreviewArtifact).where(*filters)
    ) or 0
    artifacts = session.scalars(
        select(WhatsAppDispatchPreviewArtifact)
        .where(*filters)
        .order_by(WhatsAppDispatchPreviewArtifact.role, WhatsAppDispatchPreviewArtifact.name)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [_artifact_dict(item) for item in artifacts],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
@router.get("/previews/{preview_id}/issues")
def preview_issues(
    preview_id: uuid.UUID,
    severity: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    preview = session.get(WhatsAppDispatchPreview, preview_id)
    if preview is None:
        raise HTTPException(status_code=404, detail="Dispatch preview not found")
    issues: list[dict[str, Any]] = [
        {**item, "source_type": "batch", "source_name": preview.preview_key}
        for item in preview.issues
    ]
    deliveries = session.scalars(
        select(WhatsAppDispatchPreviewDelivery)
        .where(WhatsAppDispatchPreviewDelivery.preview_id == preview_id)
        .order_by(WhatsAppDispatchPreviewDelivery.sequence)
    ).all()
    for delivery in deliveries:
        issues.extend(
            {
                **item,
                "source_type": "delivery",
                "source_id": str(delivery.id),
                "source_name": delivery.target_name,
                "source_detail": delivery.target_jid,
            }
            for item in delivery.issues
        )
    artifacts = session.scalars(
        select(WhatsAppDispatchPreviewArtifact).where(
            WhatsAppDispatchPreviewArtifact.preview_id == preview_id
        )
    ).all()
    for artifact in artifacts:
        issues.extend(
            {
                **item,
                "source_type": "artifact",
                "source_id": str(artifact.id),
                "source_name": artifact.name,
            }
            for item in artifact.issues
        )
    if severity:
        issues = [item for item in issues if item.get("severity") == severity]
    start = (page - 1) * page_size
    return {
        "items": issues[start : start + page_size],
        "total": len(issues),
        "page": page,
        "page_size": page_size,
    }
@router.get("/previews/{preview_id}/artifacts/{artifact_id}/download")
def download_preview_artifact(
    preview_id: uuid.UUID,
    artifact_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> FileResponse:
    artifact = session.get(WhatsAppDispatchPreviewArtifact, artifact_id)
    if artifact is None or artifact.preview_id != preview_id:
        raise HTTPException(status_code=404, detail="Preview artifact not found")
    path = Path(artifact.path_snapshot)
    preview = session.get(WhatsAppDispatchPreview, preview_id)
    from automation_core.models import Artifact

    source_artifact = session.get(Artifact, artifact.artifact_id) if artifact.artifact_id else None
    if preview is None or source_artifact is None or source_artifact.job_id != preview.source_job_id:
        raise HTTPException(status_code=403, detail="Artifact is not registered to the source dry run")
    if not is_managed_preview_artifact(path):
        raise HTTPException(status_code=403, detail="Artifact is outside managed preview storage")
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Frozen artifact is missing")
    if path.stat().st_size != artifact.size_bytes or sha256_file(path) != artifact.checksum_sha256:
        raise HTTPException(status_code=409, detail="Frozen artifact integrity check failed")
    return FileResponse(path, filename=artifact.name, media_type=artifact.mime_type)
