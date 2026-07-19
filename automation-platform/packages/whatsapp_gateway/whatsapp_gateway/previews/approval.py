from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.job_service import add_job, add_job_log, get_job
from automation_core.models import Job, JobPublic, JobStatus, JobType
from automation_core.task_outbox import publish_pending_tasks, stage_task
from automation_core.time import utcnow
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
from whatsapp_gateway.previews.approval_payloads import (
    approved_subset_sha256, excluded_delivery_snapshot, resolve_delivery_attachments,
)
from whatsapp_gateway.previews.approval_bulk import approve_previews_bulk as _approve_previews_bulk
from whatsapp_gateway.previews.state import summarize_preview_state

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
    all_deliveries = list(session.scalars(
        select(WhatsAppDispatchPreviewDelivery)
        .where(WhatsAppDispatchPreviewDelivery.preview_id == preview.id)
        .order_by(WhatsAppDispatchPreviewDelivery.sequence)
    ).all())
    artifacts = list(session.scalars(
        select(WhatsAppDispatchPreviewArtifact).where(
            WhatsAppDispatchPreviewArtifact.preview_id == preview.id
        )
    ).all())
    summary = summarize_preview_state(all_deliveries, preview.issues or [], artifacts)
    if summary.status != "ready":
        raise HTTPException(status_code=409, detail="Batch-blocked previews cannot be approved")
    if summary.blocked_delivery_count and not data.acknowledge_exclusions:
        raise HTTPException(
            status_code=422,
            detail="Acknowledge that blocked deliveries will be excluded from approval",
        )
    if preview_is_stale(session, preview, check_files=True):
        raise HTTPException(status_code=409, detail="This preview is stale; create a new preview")
    if summary.warning_issue_count and not data.acknowledge_warnings:
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

    frozen_deliveries = [
        item for item in all_deliveries if item.status in {"ready", "warning"}
    ]
    excluded_deliveries = [
        item for item in all_deliveries if item.status in {"blocked", "skipped"}
    ]
    excluded_snapshot = excluded_delivery_snapshot(excluded_deliveries)
    if not frozen_deliveries:
        return complete_noop_approval(
            session,
            preview,
            data,
            skipped_count=summary.skipped_delivery_count,
            excluded_deliveries=excluded_snapshot,
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

    delivery_attachments = resolve_delivery_attachments(
        session, frozen_deliveries, artifacts
    )
    approved_content_sha256 = approved_subset_sha256(
        frozen_deliveries, delivery_attachments
    )
    job = add_job(
        session,
        job_type=JobType.whatsapp_dispatch_send.value,
        title=f"Send approved eligible deliveries from {preview.preview_key}",
        parameters={
            "preview_id": str(preview.id),
            "approved_delivery_ids": [str(item.id) for item in frozen_deliveries],
            "excluded_delivery_ids": [item["id"] for item in excluded_snapshot],
        },
    )
    approval = WhatsAppDispatchApproval(
        preview_id=preview.id,
        job_id=job.id,
        approved_by=data.approved_by.strip() or "web-operator",
        warnings_acknowledged=data.acknowledge_warnings,
        exclusions_acknowledged=data.acknowledge_exclusions,
        preview_content_sha256=preview.content_sha256,
        approved_content_sha256=approved_content_sha256,
        approved_delivery_ids=[str(item.id) for item in frozen_deliveries],
        excluded_deliveries=excluded_snapshot,
        delivery_count=len(frozen_deliveries),
        excluded_count=len(excluded_snapshot),
        partial=any(item["status"] == "blocked" for item in excluded_snapshot),
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
        (
            f"Approved {len(claimed_deliveries)} exact frozen deliveries; "
            f"excluded {len(excluded_snapshot)} blocked/skipped deliveries and committed the dispatch outbox."
        ),
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
