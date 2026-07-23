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
from whatsapp_gateway.previews.maintenance import preview_deletion_block_reason
from whatsapp_gateway.tasks import compile_dispatch_preview_job, send_approved_preview_job
from whatsapp_gateway.previews.schemas import (
    PreviewInput, BulkPreviewInput, PreviewIdsInput, BulkPreviewApprovalInput,
    ContactLinkInput, PreviewApprovalInput,
)
from whatsapp_gateway.previews.serialization import _artifact_dict, _delivery_dict, _digits, _with_approval

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp-previews"])

@router.delete("/preview-bulk")
def hard_delete_previews_bulk(
    data: PreviewIdsInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    preview_ids = list(dict.fromkeys(data.preview_ids))
    if not preview_ids or len(preview_ids) > 100:
        raise HTTPException(status_code=422, detail="Select between 1 and 100 previews")
    previews = session.scalars(
        select(WhatsAppDispatchPreview).where(WhatsAppDispatchPreview.id.in_(preview_ids))
    ).all()
    if len(previews) != len(preview_ids):
        raise HTTPException(status_code=404, detail="One or more selected previews no longer exist")
    approved_ids = set(session.scalars(
        select(WhatsAppDispatchApproval.preview_id).where(
            WhatsAppDispatchApproval.preview_id.in_(preview_ids)
        )
    ).all())
    if approved_ids:
        raise HTTPException(
            status_code=409,
            detail="Approved previews are retained as delivery audit records; remove them from the selection",
        )
    protected = [
        reason
        for preview in previews
        if (reason := preview_deletion_block_reason(session, preview))
    ]
    if protected:
        raise HTTPException(
            status_code=409,
            detail=f"{protected[0]} Remove protected previews from the selection.",
        )
    paths: list[Path] = []
    for preview in previews:
        paths.extend(delete_preview_records(session, preview))
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"deleted": len(previews), "ids": [str(item) for item in preview_ids]}
@router.delete("/previews/{preview_id}")
def hard_delete_preview(
    preview_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    preview = session.get(WhatsAppDispatchPreview, preview_id)
    if preview is None:
        raise HTTPException(status_code=404, detail="Dispatch preview not found")
    if reason := preview_deletion_block_reason(session, preview):
        raise HTTPException(status_code=409, detail=reason)
    preview_key = preview.preview_key
    paths = delete_preview_records(session, preview)
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"id": str(preview_id), "preview_key": preview_key, "deleted": True}
