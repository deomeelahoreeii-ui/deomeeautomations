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

@router.get("/previews")
def previews(
    search: str = "",
    preview_status: str = "",
    view: Literal["pending", "sending", "history", "all"] = "pending",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    filters: list[Any] = []
    approval_exists = select(WhatsAppDispatchApproval.id).where(
        WhatsAppDispatchApproval.preview_id == WhatsAppDispatchPreview.id
    ).exists()
    if view == "pending":
        filters.append(~approval_exists)
    elif view == "sending":
        filters.append(approval_exists.where(WhatsAppDispatchApproval.status.in_(["queued", "sending"])))
    elif view == "history":
        filters.append(approval_exists.where(WhatsAppDispatchApproval.status.in_(["completed", "failed"])))
    if preview_status and preview_status != "stale":
        filters.append(WhatsAppDispatchPreview.status == preview_status)
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppDispatchPreview.preview_key.ilike(pattern),
                WhatsAppDispatchPreview.profile_name.ilike(pattern),
                WhatsAppDispatchPreview.audience_name.ilike(pattern),
                WhatsAppDispatchPreview.wing_name.ilike(pattern),
            )
        )
    if preview_status == "stale":
        records = session.scalars(
            select(WhatsAppDispatchPreview)
            .where(*filters)
            .order_by(WhatsAppDispatchPreview.created_at.desc())
        ).all()
        all_items = [
            _with_approval(session, preview_dict(session, item, check_files=True))
            for item in records
        ]
        all_items = [item for item in all_items if item["stale"]]
        total = len(all_items)
        start = (page - 1) * page_size
        items = all_items[start : start + page_size]
    else:
        total = session.scalar(
            select(func.count()).select_from(WhatsAppDispatchPreview).where(*filters)
        ) or 0
        records = session.scalars(
            select(WhatsAppDispatchPreview)
            .where(*filters)
            .order_by(WhatsAppDispatchPreview.created_at.desc())
            .limit(page_size)
            .offset((page - 1) * page_size)
        ).all()
        items = [_with_approval(session, preview_dict(session, item)) for item in records]
    return {"items": items, "total": total, "page": page, "page_size": page_size}
@router.get("/previews/selection")
def preview_selection(
    search: str = "",
    preview_status: str = "",
    view: Literal["pending", "sending", "history", "all"] = "pending",
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    filters: list[Any] = []
    approval_exists = select(WhatsAppDispatchApproval.id).where(
        WhatsAppDispatchApproval.preview_id == WhatsAppDispatchPreview.id
    ).exists()
    if view == "pending":
        filters.append(~approval_exists)
    elif view == "sending":
        filters.append(approval_exists.where(WhatsAppDispatchApproval.status.in_(["queued", "sending"])))
    elif view == "history":
        filters.append(approval_exists.where(WhatsAppDispatchApproval.status.in_(["completed", "failed"])))
    if preview_status and preview_status != "stale":
        filters.append(WhatsAppDispatchPreview.status == preview_status)
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(or_(
            WhatsAppDispatchPreview.preview_key.ilike(pattern),
            WhatsAppDispatchPreview.profile_name.ilike(pattern),
            WhatsAppDispatchPreview.audience_name.ilike(pattern),
            WhatsAppDispatchPreview.wing_name.ilike(pattern),
        ))
    records = session.scalars(
        select(WhatsAppDispatchPreview).where(*filters).order_by(WhatsAppDispatchPreview.created_at.desc())
    ).all()
    if preview_status == "stale":
        records = [item for item in records if preview_is_stale(session, item, check_files=True)]
    return {"ids": [str(item.id) for item in records], "count": len(records)}
@router.get("/previews/{preview_id}")
def preview_detail(
    preview_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    preview = session.get(WhatsAppDispatchPreview, preview_id)
    if preview is None:
        raise HTTPException(status_code=404, detail="Dispatch preview not found")
    result = preview_dict(session, preview, check_files=True)
    approval = session.scalar(
        select(WhatsAppDispatchApproval).where(
            WhatsAppDispatchApproval.preview_id == preview.id
        )
    )
    result["approval"] = (
        {
            "id": str(approval.id),
            "job_id": str(approval.job_id),
            "status": approval.status,
            "approved_by": approval.approved_by,
            "warnings_acknowledged": approval.warnings_acknowledged,
            "exclusions_acknowledged": approval.exclusions_acknowledged,
            "delivery_count": approval.delivery_count,
            "excluded_count": approval.excluded_count,
            "partial": approval.partial,
            "approved_content_sha256": approval.approved_content_sha256,
            "approved_delivery_ids": approval.approved_delivery_ids,
            "excluded_deliveries": approval.excluded_deliveries,
            "error": approval.error,
            "approved_at": approval.approved_at,
            "completed_at": approval.completed_at,
        }
        if approval
        else None
    )
    return result
