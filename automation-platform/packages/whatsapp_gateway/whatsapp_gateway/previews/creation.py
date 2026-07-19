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
from automation_core.job_service import add_job, add_job_log, get_job
from automation_core.models import Job, JobPublic, JobStatus, JobType
from automation_core.task_outbox import publish_pending_tasks, stage_task
from automation_core.time import utcnow
from antidengue_automation.deadline_policy import resolve_deadline_policy
from automation_core.worker_runtime import WorkerCapabilityUnavailable, require_compatible_workers
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
from whatsapp_gateway.tasks import compile_dispatch_preview_v2_job, send_approved_preview_job
from whatsapp_gateway.previews.compiler.capabilities import (
    PREVIEW_COMPILER_QUEUE, PREVIEW_COMPILER_TASK, UnsupportedPlannerCapability,
    required_compiler_contract,
)
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
    try:
        compiler_contract = required_compiler_contract(session, [profile])
    except UnsupportedPlannerCapability as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    try:
        require_compatible_workers(session, compiler_contract)
    except WorkerCapabilityUnavailable as exc:
        raise HTTPException(status_code=503, detail=exc.detail()) from exc
    deadline = resolve_deadline_policy(session)
    job = add_job(
        session,
        job_type=JobType.whatsapp_dispatch_preview.value,
        title="Compile AntiDengue dispatch preview",
        parameters={
            "source_job_id": str(data.source_job_id),
            "dispatch_profile_id": str(data.dispatch_profile_id),
            "created_by": "web",
            "compiler_contract": compiler_contract,
            "deadline_snapshot": deadline.model_dump(),
        },
    )
    add_job_log(session, job.id, "Committed immutable dispatch preview request to the durable task outbox.")
    stage_task(
        session,
        job=job,
        task_name=PREVIEW_COMPILER_TASK,
        queue=PREVIEW_COMPILER_QUEUE,
        args=[str(job.id)],
        idempotency_key=f"manual-preview:{job.id}",
    )
    session.commit()
    publish_pending_tasks(session, limit=10)
    queued = get_job(session, job.id)
    assert queued is not None
    return queued

# Compile one atomic preview for the selected profile set. The scheduled
# AntiDengue path already treats a profile set as one frozen review boundary;
# the manual path must preserve the same behavior. Keep this as a source comment
# rather than a route docstring so the established OpenAPI path contract remains
# byte-for-byte stable.
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
    profiles_by_id = {profile.id: profile for profile in profiles}
    if set(profiles_by_id) != set(profile_ids):
        raise HTTPException(status_code=422, detail="One or more dispatch profiles are unavailable")
    ordered_profiles = [profiles_by_id[profile_id] for profile_id in profile_ids]
    if any(profile.application_id != ordered_profiles[0].application_id for profile in ordered_profiles):
        raise HTTPException(status_code=422, detail="Bulk compilation must stay within one module")
    try:
        compiler_contract = required_compiler_contract(session, ordered_profiles)
    except UnsupportedPlannerCapability as exc:
        raise HTTPException(status_code=422, detail=exc.detail) from exc
    try:
        require_compatible_workers(session, compiler_contract)
    except WorkerCapabilityUnavailable as exc:
        raise HTTPException(status_code=503, detail=exc.detail()) from exc

    deadline = resolve_deadline_policy(session)
    job = add_job(
        session,
        job_type=JobType.whatsapp_dispatch_preview.value,
        title=(
            "Compile AntiDengue dispatch preview"
            if len(profile_ids) == 1
            else f"Compile AntiDengue dispatch preview ({len(profile_ids)} profiles)"
        ),
        parameters={
            "source_job_id": str(data.source_job_id),
            # Retained for compatibility with older workers and audit readers.
            "dispatch_profile_id": str(profile_ids[0]),
            "dispatch_profile_ids": [str(profile_id) for profile_id in profile_ids],
            "created_by": "web",
            "compiler_contract": compiler_contract,
            "deadline_snapshot": deadline.model_dump(),
        },
    )
    add_job_log(
        session,
        job.id,
        f"Committed one immutable preview request for {len(profile_ids)} routing profile(s) "
        "to the durable task outbox.",
    )
    stage_task(
        session,
        job=job,
        task_name=PREVIEW_COMPILER_TASK,
        queue=PREVIEW_COMPILER_QUEUE,
        args=[str(job.id)],
        idempotency_key=f"manual-preview:{job.id}",
    )
    session.commit()
    publish_pending_tasks(session, limit=10)
    queued = get_job(session, job.id)
    assert queued is not None
    public_job = JobPublic.model_validate(queued).model_dump(mode="json")
    return {
        "jobs": [public_job],
        "count": 1,
        "profile_count": len(profile_ids),
        "combined": len(profile_ids) > 1,
    }
