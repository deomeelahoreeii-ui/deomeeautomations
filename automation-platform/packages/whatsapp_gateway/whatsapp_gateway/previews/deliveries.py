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

@router.get("/previews/{preview_id}/deliveries")
def preview_deliveries(
    preview_id: uuid.UUID,
    delivery_status: str = "",
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if session.get(WhatsAppDispatchPreview, preview_id) is None:
        raise HTTPException(status_code=404, detail="Dispatch preview not found")
    filters: list[Any] = [WhatsAppDispatchPreviewDelivery.preview_id == preview_id]
    if delivery_status:
        filters.append(WhatsAppDispatchPreviewDelivery.status == delivery_status)
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppDispatchPreviewDelivery.target_name.ilike(pattern),
                WhatsAppDispatchPreviewDelivery.target_jid.ilike(pattern),
                WhatsAppDispatchPreviewDelivery.route_scope.ilike(pattern),
            )
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDispatchPreviewDelivery).where(*filters)
    ) or 0
    deliveries = session.scalars(
        select(WhatsAppDispatchPreviewDelivery)
        .where(*filters)
        .order_by(WhatsAppDispatchPreviewDelivery.sequence)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [_delivery_dict(session, item) for item in deliveries],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
