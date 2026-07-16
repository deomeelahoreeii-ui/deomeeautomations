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

@router.post(
    "/previews",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_preview(
    data: PreviewInput,
    session: Session = Depends(get_session),
) -> JobPublic:
    source_job = session.get(Job, data.source_job_id)
    profile = session.get(WhatsAppDispatchProfile, data.dispatch_profile_id)
    if source_job is None or source_job.status != JobStatus.succeeded.value:
        raise HTTPException(status_code=422, detail="Select a successful source dry run")
    if profile is None or not profile.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled dispatch profile")
    job = create_job(
        session,
        job_type=JobType.whatsapp_dispatch_preview.value,
        title="Compile AntiDengue dispatch preview",
        parameters={
            "source_job_id": str(data.source_job_id),
            "dispatch_profile_id": str(data.dispatch_profile_id),
            "created_by": "web",
        },
    )
    try:
        from whatsapp_gateway import preview_api as legacy_preview_api
        task = legacy_preview_api.compile_dispatch_preview_job.apply_async(
            args=[str(job.id)], queue="antidengue"
        )
    except Exception as exc:
        append_log(session, job.id, f"Queue unavailable: {exc}", level="error")
        mark_job_failed(session, job.id, f"Queue unavailable: {exc}")
        raise HTTPException(status_code=503, detail="Queue unavailable") from exc
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, "Queued immutable dispatch preview compilation.")
    queued = get_job(session, job.id)
    assert queued is not None
    return queued
@router.post("/previews/bulk", status_code=status.HTTP_202_ACCEPTED)
def create_previews_bulk(
    data: BulkPreviewInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    profile_ids = list(dict.fromkeys(data.dispatch_profile_ids))
    if not profile_ids or len(profile_ids) > 50:
        raise HTTPException(status_code=422, detail="Select between 1 and 50 dispatch profiles")
    source_job = session.get(Job, data.source_job_id)
    if source_job is None or source_job.status != JobStatus.succeeded.value:
        raise HTTPException(status_code=422, detail="Select a successful source dry run")
    profiles = session.scalars(
        select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.id.in_(profile_ids),
            WhatsAppDispatchProfile.enabled.is_(True),
        )
    ).all()
    if len(profiles) != len(profile_ids):
        raise HTTPException(status_code=422, detail="One or more dispatch profiles are unavailable")
    if any(profile.application_id != profiles[0].application_id for profile in profiles):
        raise HTTPException(status_code=422, detail="Bulk compilation must stay within one module")
    jobs = [
        JobPublic.model_validate(create_preview(
            PreviewInput(source_job_id=data.source_job_id, dispatch_profile_id=profile_id),
            session,
        )).model_dump(mode="json")
        for profile_id in profile_ids
    ]
    return {"jobs": jobs, "count": len(jobs)}
