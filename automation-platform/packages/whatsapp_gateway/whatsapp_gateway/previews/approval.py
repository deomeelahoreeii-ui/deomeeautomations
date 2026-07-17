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
    "/previews/{preview_id}/approve",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
async def approve_preview(
    preview_id: uuid.UUID,
    data: PreviewApprovalInput,
    session: Session = Depends(get_session),
) -> JobPublic:
    preview = session.get(WhatsAppDispatchPreview, preview_id)
    if preview is None:
        raise HTTPException(status_code=404, detail="Dispatch preview not found")
    if preview.status != "ready" or preview.blocked_count:
        raise HTTPException(status_code=409, detail="Blocked previews cannot be approved")
    if preview_is_stale(session, preview, check_files=True):
        raise HTTPException(status_code=409, detail="This preview is stale; create a new preview")
    if preview.warning_count and not data.acknowledge_warnings:
        raise HTTPException(status_code=422, detail="Acknowledge all preview warnings before approval")
    existing = session.scalar(
        select(WhatsAppDispatchApproval).where(
            WhatsAppDispatchApproval.preview_id == preview.id
        )
    )
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"This preview was already approved ({existing.status})",
        )

    profile = session.get(WhatsAppDispatchProfile, preview.dispatch_profile_id)
    if profile is None:
        raise HTTPException(status_code=409, detail="The frozen dispatch profile no longer exists")
    account = session.get(WhatsAppAccount, profile.account_id)
    gateway_settings = session.scalar(
        select(WhatsAppSettings).where(
            WhatsAppSettings.default_account_id == profile.account_id
        )
    )
    if account is None or not account.enabled:
        raise HTTPException(status_code=409, detail="The WhatsApp account is disabled")
    if gateway_settings is None or not gateway_settings.live_delivery_enabled:
        raise HTTPException(status_code=409, detail="Enable Live delivery in WhatsApp Settings first")
    # Keep the auth/session runtime in the existing worker, but require a live
    # readiness response before an irreversible approval record is created.
    from whatsapp_gateway.api import worker_health

    health = await worker_health(account)
    if not health.get("ready"):
        raise HTTPException(status_code=503, detail="WhatsApp gateway is not ready")

    frozen_deliveries = session.scalars(
        select(WhatsAppDispatchPreviewDelivery)
        .where(
            WhatsAppDispatchPreviewDelivery.preview_id == preview.id,
            WhatsAppDispatchPreviewDelivery.status.in_(["ready", "warning"]),
        )
        .order_by(WhatsAppDispatchPreviewDelivery.sequence)
    ).all()
    if not frozen_deliveries:
        raise HTTPException(status_code=409, detail="This preview has no sendable deliveries")

    artifacts = session.scalars(
        select(WhatsAppDispatchPreviewArtifact).where(
            WhatsAppDispatchPreviewArtifact.preview_id == preview.id
        )
    ).all()
    artifacts_by_id = {str(item.id): item for item in artifacts}
    delivery_attachments: dict[uuid.UUID, list[dict[str, Any]]] = {}
    for frozen in frozen_deliveries:
        attachments: list[dict[str, Any]] = []
        for artifact_id in frozen.attachment_ids:
            artifact = artifacts_by_id.get(artifact_id)
            if artifact is None or artifact.status == "blocked":
                raise HTTPException(status_code=409, detail="A frozen delivery attachment is invalid")
            path = Path(artifact.path_snapshot)
            if not path.is_file() or sha256_file(path) != artifact.checksum_sha256:
                raise HTTPException(status_code=409, detail=f"Frozen attachment changed: {artifact.name}")
            attachments.append({
                "preview_artifact_id": str(artifact.id),
                "path": artifact.path_snapshot,
                "name": artifact.name,
                "mime_type": artifact.mime_type,
                "checksum_sha256": artifact.checksum_sha256,
            })
        delivery_attachments[frozen.id] = attachments

    job = add_job(
        session,
        job_type=JobType.whatsapp_dispatch_send.value,
        title=f"Send approved preview {preview.preview_key}",
        parameters={"preview_id": str(preview.id)},
    )
    approval = WhatsAppDispatchApproval(
        preview_id=preview.id,
        job_id=job.id,
        approved_by=data.approved_by.strip() or "web-operator",
        warnings_acknowledged=data.acknowledge_warnings,
        preview_content_sha256=preview.content_sha256,
        delivery_count=len(frozen_deliveries),
    )
    session.add(approval)
    session.flush()
    for frozen in frozen_deliveries:
        session.add(
            WhatsAppDelivery(
                approval_id=approval.id,
                preview_delivery_id=frozen.id,
                account_id=account.id,
                wing_id=frozen.wing_id,
                recipient_type=frozen.target_type,
                recipient_name=frozen.target_name,
                target=frozen.target_jid,
                message=frozen.message,
                attachments=delivery_attachments[frozen.id],
                status_subject=f"whatsapp.status.platform.{uuid.uuid4().hex}",
            )
        )
    job.parameters = {**job.parameters, "approval_id": str(approval.id)}
    add_job_log(
        session,
        job.id,
        f"Approved {len(frozen_deliveries)} exact frozen deliveries and committed the dispatch outbox.",
    )
    stage_task(
        session,
        job=job,
        task_name="whatsapp_gateway.send_approved_preview",
        queue="antidengue",
        args=[str(job.id)],
        idempotency_key=f"preview-approval:{preview.id}",
    )
    session.add(job)
    session.commit()
    publish_pending_tasks(session, limit=10)
    queued = get_job(session, job.id)
    assert queued is not None
    return queued
@router.post("/preview-bulk/approve", status_code=status.HTTP_202_ACCEPTED)
async def approve_previews_bulk(
    data: BulkPreviewApprovalInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    preview_ids = list(dict.fromkeys(data.preview_ids))
    if not preview_ids or len(preview_ids) > 50:
        raise HTTPException(status_code=422, detail="Select between 1 and 50 previews")
    previews = session.scalars(
        select(WhatsAppDispatchPreview).where(WhatsAppDispatchPreview.id.in_(preview_ids))
    ).all()
    if len(previews) != len(preview_ids):
        raise HTTPException(status_code=404, detail="One or more selected previews no longer exist")
    if any(item.status != "ready" or item.blocked_count for item in previews):
        raise HTTPException(status_code=409, detail="Remove blocked previews from the selection")
    if any(preview_is_stale(session, item, check_files=True) for item in previews):
        raise HTTPException(status_code=409, detail="Remove stale previews and compile them again")
    if any(item.warning_count for item in previews) and not data.acknowledge_warnings:
        raise HTTPException(status_code=422, detail="Acknowledge warnings for the selected previews")
    approved_ids = set(session.scalars(
        select(WhatsAppDispatchApproval.preview_id).where(
            WhatsAppDispatchApproval.preview_id.in_(preview_ids)
        )
    ).all())
    if approved_ids:
        raise HTTPException(status_code=409, detail="Remove already approved previews from the selection")

    jobs: list[JobPublic] = []
    for preview_id in preview_ids:
        jobs.append(await approve_preview(
            preview_id,
            PreviewApprovalInput(
                acknowledge_warnings=data.acknowledge_warnings,
                approved_by=data.approved_by,
            ),
            session,
        ))
    return {"jobs": jobs, "count": len(jobs)}
