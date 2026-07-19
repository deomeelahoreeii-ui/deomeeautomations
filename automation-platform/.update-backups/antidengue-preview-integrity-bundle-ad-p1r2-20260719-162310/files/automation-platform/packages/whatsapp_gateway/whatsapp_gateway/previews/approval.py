from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.job_service import add_job, add_job_log, get_job
from automation_core.models import Job, JobPublic, JobStatus, JobType
from automation_core.task_outbox import publish_pending_tasks, stage_task
from automation_core.time import utcnow
from automation_core.storage_catalog import ensure_stored_path
from whatsapp_gateway.models import (
    WhatsAppAccount, WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact, WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchApproval, WhatsAppDispatchProfile, WhatsAppDelivery,
    WhatsAppSettings,
)
from whatsapp_gateway.preview_service import (
    preview_is_stale,
)
from whatsapp_gateway.previews.schemas import (
    BulkPreviewApprovalInput, PreviewApprovalInput,
)
from whatsapp_gateway.previews.approval_claims import (
    claim_daily_message, complete_noop_approval,
)
from whatsapp_gateway.previews.approval_bulk import approve_previews_bulk as _approve_previews_bulk

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp-previews"])
# Preserve the established private hooks used by integrations during migration.
_claim_daily_message = claim_daily_message
_complete_noop_approval = complete_noop_approval


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

    frozen_deliveries = session.scalars(
        select(WhatsAppDispatchPreviewDelivery)
        .where(
            WhatsAppDispatchPreviewDelivery.preview_id == preview.id,
            WhatsAppDispatchPreviewDelivery.status.in_(["ready", "warning"]),
        )
        .order_by(WhatsAppDispatchPreviewDelivery.sequence)
    ).all()
    if not frozen_deliveries:
        return complete_noop_approval(
            session,
            preview,
            data,
            skipped_count=preview.skipped_count,
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
            try:
                path = ensure_stored_path(
                    session,
                    stored_object_id=artifact.stored_object_id,
                    local_path=Path(artifact.path_snapshot),
                    name=artifact.name,
                    expected_sha256=artifact.checksum_sha256,
                )
            except Exception as exc:
                raise HTTPException(status_code=409, detail=f"Frozen attachment is unavailable: {artifact.name}: {exc}") from exc
            artifact.path_snapshot = str(path)
            session.add(artifact)
            attachments.append({
                "preview_artifact_id": str(artifact.id),
                "path": str(path),
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
    claimed_deliveries: list[WhatsAppDispatchPreviewDelivery] = []
    for frozen in frozen_deliveries:
        if not claim_daily_message(
            session,
            preview=preview,
            frozen=frozen,
            approval=approval,
            account=account,
        ):
            add_job_log(
                session,
                job.id,
                f"Suppressed duplicate once-daily acknowledgement for {frozen.target_name}.",
            )
            continue
        claimed_deliveries.append(frozen)
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
    approval.delivery_count = len(claimed_deliveries)
    if not claimed_deliveries:
        now = utcnow()
        approval.status = "completed"
        approval.completed_at = now
        job.status = JobStatus.succeeded.value
        job.started_at = now
        job.finished_at = now
        job.result = {
            "delivered": 0,
            "failed": 0,
            "skipped": len(frozen_deliveries),
            "no_op": True,
            "message": "Nothing new to send; acknowledgements were claimed by another approval.",
        }
        session.add(approval)
        session.add(job)
        session.commit()
        session.refresh(job)
        return job
    job.parameters = {**job.parameters, "approval_id": str(approval.id)}
    add_job_log(
        session,
        job.id,
        f"Approved {len(claimed_deliveries)} exact frozen deliveries and committed the dispatch outbox.",
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
    return await _approve_previews_bulk(data, session)
