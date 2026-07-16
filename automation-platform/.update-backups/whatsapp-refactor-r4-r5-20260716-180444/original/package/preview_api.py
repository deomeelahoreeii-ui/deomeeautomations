from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy import func, or_, select
from sqlmodel import Session

from automation_core.database import get_session
from automation_core.job_service import (
    append_log,
    create_job,
    get_job,
    mark_job_failed,
    set_task_id,
)
from automation_core.models import Job, JobPublic, JobStatus, JobType
from automation_core.time import utcnow
from master_data.models import Officer, School, SchoolHead, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppContactLink,
    WhatsAppDirectoryContact,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
    WhatsAppDispatchApproval,
    WhatsAppDispatchProfile,
    WhatsAppDelivery,
    WhatsAppRecipientScope,
    WhatsAppReportType,
    WhatsAppSettings,
)
from whatsapp_gateway.preview_service import (
    cleanup_unreferenced_preview_files,
    delete_preview_records,
    entity_link_details,
    is_managed_preview_artifact,
    preview_dict,
    preview_is_stale,
    sha256_file,
)
from whatsapp_gateway.tasks import compile_dispatch_preview_job, send_approved_preview_job


router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp-previews"])


class PreviewInput(BaseModel):
    source_job_id: uuid.UUID
    dispatch_profile_id: uuid.UUID


class BulkPreviewInput(BaseModel):
    source_job_id: uuid.UUID
    dispatch_profile_ids: list[uuid.UUID]


class PreviewIdsInput(BaseModel):
    preview_ids: list[uuid.UUID]


class BulkPreviewApprovalInput(PreviewIdsInput):
    acknowledge_warnings: bool = False
    approved_by: str = "web-operator"


class ContactLinkInput(BaseModel):
    entity_type: Literal["officer", "school_head"]
    entity_id: uuid.UUID


class PreviewApprovalInput(BaseModel):
    acknowledge_warnings: bool = False
    approved_by: str = "web-operator"


def _digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def _delivery_dict(
    session: Session,
    delivery: WhatsAppDispatchPreviewDelivery,
) -> dict[str, Any]:
    artifact_ids = [uuid.UUID(value) for value in delivery.attachment_ids]
    artifacts = (
        session.scalars(
            select(WhatsAppDispatchPreviewArtifact).where(
                WhatsAppDispatchPreviewArtifact.id.in_(artifact_ids)
            )
        ).all()
        if artifact_ids
        else []
    )
    artifacts_by_id = {str(item.id): item for item in artifacts}
    attachments = []
    for artifact_id in delivery.attachment_ids:
        artifact = artifacts_by_id.get(artifact_id)
        if artifact:
            attachments.append(
                {
                    "id": str(artifact.id),
                    "name": artifact.name,
                    "mime_type": artifact.mime_type,
                    "size_bytes": artifact.size_bytes,
                    "checksum_sha256": artifact.checksum_sha256,
                    "status": artifact.status,
                }
            )
    return {
        "id": str(delivery.id),
        "preview_id": str(delivery.preview_id),
        "sequence": delivery.sequence,
        "source_route_key": delivery.source_route_key,
        "target_type": delivery.target_type,
        "target_name": delivery.target_name,
        "target_jid": delivery.target_jid,
        "wing_name": delivery.wing_name,
        "route_kind": delivery.route_kind,
        "route_scope": delivery.route_scope,
        "message": delivery.message,
        "attachments": attachments,
        "routing_snapshot": delivery.routing_snapshot,
        "issues": delivery.issues,
        "status": delivery.status,
        "idempotency_key": delivery.idempotency_key,
        "created_at": delivery.created_at,
    }


def _artifact_dict(artifact: WhatsAppDispatchPreviewArtifact) -> dict[str, Any]:
    return {
        "id": str(artifact.id),
        "preview_id": str(artifact.preview_id),
        "artifact_id": artifact.artifact_id,
        "role": artifact.role,
        "name": artifact.name,
        "mime_type": artifact.mime_type,
        "size_bytes": artifact.size_bytes,
        "checksum_sha256": artifact.checksum_sha256,
        "status": artifact.status,
        "issues": artifact.issues,
        "created_at": artifact.created_at,
    }


def _with_approval(session: Session, item: dict[str, Any]) -> dict[str, Any]:
    approval = session.scalar(
        select(WhatsAppDispatchApproval).where(
            WhatsAppDispatchApproval.preview_id == uuid.UUID(item["id"])
        )
    )
    return {
        **item,
        "approval_status": approval.status if approval else None,
        "approved": approval is not None,
    }


@router.get("/previews/options")
def preview_options(session: Session = Depends(get_session)) -> dict[str, Any]:
    jobs = session.scalars(
        select(Job)
        .where(
            Job.type == JobType.antidengue_report.value,
            Job.status == JobStatus.succeeded.value,
        )
        .order_by(Job.created_at.desc())
        .limit(50)
    ).all()
    run_options = []
    for job in jobs:
        if not bool(job.parameters.get("dry_run", False)):
            continue
        summary = (job.result or {}).get("summary") or {}
        dispatch_plan = ((summary.get("whatsapp") or {}).get("dispatch_plan") or []) if isinstance(summary, dict) else []
        quality = summary.get("quality_gate") or {} if isinstance(summary, dict) else {}
        run_options.append(
            {
                "id": str(job.id),
                "title": job.title,
                "created_at": job.created_at,
                "finished_at": job.finished_at,
                "planned_delivery_count": len(dispatch_plan) if isinstance(dispatch_plan, list) else 0,
                "report_count": int((job.result or {}).get("artifact_count") or 0),
                "quality_warning_count": len(quality.get("warnings") or []) if isinstance(quality, dict) else 0,
            }
        )

    application = session.scalar(
        select(WhatsAppApplication).where(WhatsAppApplication.key == "antidengue")
    )
    profiles = []
    if application:
        profile_rows = session.scalars(
            select(WhatsAppDispatchProfile)
            .where(
                WhatsAppDispatchProfile.application_id == application.id,
                WhatsAppDispatchProfile.enabled.is_(True),
            )
            .order_by(WhatsAppDispatchProfile.name)
        ).all()
        for profile in profile_rows:
            audience = session.get(WhatsAppAudience, profile.audience_id)
            report_type = session.get(WhatsAppReportType, profile.report_type_id)
            wing = session.get(Wing, profile.wing_id) if profile.wing_id else None
            recipient_scope = (
                session.get(WhatsAppRecipientScope, profile.recipient_scope_id)
                if profile.recipient_scope_id
                else None
            )
            target_count = session.scalar(
                select(func.count())
                .select_from(WhatsAppAudienceMember)
                .where(
                    WhatsAppAudienceMember.audience_id == profile.audience_id,
                    WhatsAppAudienceMember.enabled.is_(True),
                )
            ) or 0
            profiles.append(
                {
                    "id": str(profile.id),
                    "name": profile.name,
                    "key": profile.key,
                    "application_id": str(application.id),
                    "application_key": application.key,
                    "application_name": application.name,
                    "version": profile.version,
                    "audience_name": audience.name if audience else "Missing audience",
                    "report_type_name": report_type.name if report_type else "Missing report type",
                    "wing_name": wing.name if wing else "Missing wing",
                    "recipient_channel": profile.recipient_channel,
                    "recipient_scope_name": recipient_scope.name if recipient_scope else "Legacy / unscoped",
                    "target_count": target_count,
                }
            )
    return {"runs": run_options, "profiles": profiles}


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
        task = compile_dispatch_preview_job.apply_async(
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
            "delivery_count": approval.delivery_count,
            "error": approval.error,
            "approved_at": approval.approved_at,
            "completed_at": approval.completed_at,
        }
        if approval
        else None
    )
    return result


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

    job = create_job(
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
    session.add(job)
    session.commit()

    try:
        task = send_approved_preview_job.apply_async(args=[str(job.id)], queue="antidengue")
    except Exception as exc:
        approval.status = "failed"
        approval.error = f"Queue unavailable: {exc}"
        approval.completed_at = utcnow()
        session.add(approval)
        for delivery in session.scalars(
            select(WhatsAppDelivery).where(
                WhatsAppDelivery.approval_id == approval.id,
                WhatsAppDelivery.status == "queued",
            )
        ):
            delivery.status = "failed"
            delivery.error = approval.error
            delivery.completed_at = utcnow()
            session.add(delivery)
        mark_job_failed(session, job.id, approval.error)
        raise HTTPException(status_code=503, detail="Dispatch queue unavailable") from exc
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, f"Approved {len(frozen_deliveries)} exact frozen deliveries for queueing.")
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
    if session.scalar(
        select(WhatsAppDispatchApproval.id).where(
            WhatsAppDispatchApproval.preview_id == preview.id
        )
    ):
        raise HTTPException(
            status_code=409,
            detail="Approved previews are retained as delivery audit records",
        )
    preview_key = preview.preview_key
    paths = delete_preview_records(session, preview)
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"id": str(preview_id), "preview_key": preview_key, "deleted": True}


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


@router.get("/directory/contacts/{contact_id}/links")
def contact_links(
    contact_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None:
        raise HTTPException(status_code=404, detail="WhatsApp contact not found")
    links = session.scalars(
        select(WhatsAppContactLink)
        .where(
            WhatsAppContactLink.directory_contact_id == contact_id,
            WhatsAppContactLink.active.is_(True),
        )
        .order_by(WhatsAppContactLink.entity_type, WhatsAppContactLink.created_at)
    ).all()
    return {
        "contact": {
            "id": str(contact.id),
            "display_name": contact.display_name,
            "phone_jid": contact.phone_jid,
            "primary_lid_jid": contact.primary_lid_jid,
        },
        "items": [entity_link_details(session, item) for item in links],
    }


@router.get("/directory/contact-link-targets")
def contact_link_targets(
    contact_id: uuid.UUID,
    entity_type: Literal["officer", "school_head"],
    search: str = "",
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None:
        raise HTTPException(status_code=404, detail="WhatsApp contact not found")
    contact_digits = _digits(contact.phone_jid)
    if entity_type == "officer":
        filters: list[Any] = [Officer.active.is_(True)]
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(Officer.name.ilike(pattern), Officer.mobile.ilike(pattern), Officer.normalized_mobile.ilike(pattern))
            )
        records = session.scalars(
            select(Officer).where(*filters).order_by(Officer.name).limit(50)
        ).all()
        items = []
        for officer in records:
            wing = session.get(Wing, officer.wing_id)
            exact = bool(contact_digits and contact_digits.endswith(_digits(officer.normalized_mobile)))
            items.append(
                {
                    "id": str(officer.id),
                    "entity_type": "officer",
                    "name": officer.name,
                    "detail": f"{officer.role.upper()} · {wing.name if wing else 'Unknown wing'}",
                    "mobile": officer.mobile,
                    "wing_name": wing.name if wing else "Unknown wing",
                    "suggested": exact,
                }
            )
    else:
        filters = [SchoolHead.active.is_(True), School.active.is_(True)]
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(
                    SchoolHead.name.ilike(pattern),
                    SchoolHead.mobile.ilike(pattern),
                    SchoolHead.normalized_mobile.ilike(pattern),
                    School.name.ilike(pattern),
                    School.emis.ilike(pattern),
                )
            )
        records = session.execute(
            select(SchoolHead, School)
            .join(School, School.id == SchoolHead.school_id)
            .where(*filters)
            .order_by(SchoolHead.name)
            .limit(50)
        ).all()
        items = []
        for head, school in records:
            wing = session.get(Wing, school.wing_id)
            exact = bool(contact_digits and contact_digits.endswith(_digits(head.normalized_mobile)))
            items.append(
                {
                    "id": str(head.id),
                    "entity_type": "school_head",
                    "name": head.name,
                    "detail": f"{school.emis} · {school.name}",
                    "mobile": head.mobile,
                    "wing_name": wing.name if wing else "Unknown wing",
                    "suggested": exact,
                }
            )
    items.sort(key=lambda item: (not item["suggested"], item["name"].lower()))
    if not search.strip():
        items = [item for item in items if item["suggested"]]
    return {"items": items[:25]}


@router.post("/directory/contacts/{contact_id}/links", status_code=status.HTTP_201_CREATED)
def save_contact_link(
    contact_id: uuid.UUID,
    data: ContactLinkInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or not contact.active:
        raise HTTPException(status_code=404, detail="Active WhatsApp contact not found")
    officer = session.get(Officer, data.entity_id) if data.entity_type == "officer" else None
    head = session.get(SchoolHead, data.entity_id) if data.entity_type == "school_head" else None
    school = session.get(School, head.school_id) if head else None
    if data.entity_type == "officer" and (officer is None or not officer.active):
        raise HTTPException(status_code=422, detail="Select an active officer")
    if data.entity_type == "school_head" and (head is None or not head.active or school is None or not school.active):
        raise HTTPException(status_code=422, detail="Select an active school head")
    wing_id = officer.wing_id if officer else school.wing_id
    entity_key = f"{data.entity_type}:{data.entity_id}"
    link = session.scalar(
        select(WhatsAppContactLink).where(
            WhatsAppContactLink.directory_contact_id == contact_id,
            WhatsAppContactLink.entity_type == data.entity_type,
            WhatsAppContactLink.entity_key == entity_key,
        )
    )
    if link is None:
        link = WhatsAppContactLink(
            directory_contact_id=contact_id,
            entity_type=data.entity_type,
            entity_key=entity_key,
            officer_id=officer.id if officer else None,
            school_head_id=head.id if head else None,
            wing_id=wing_id,
        )
    link.status = "verified"
    link.source = "manual"
    link.confidence = 1.0
    link.active = True
    link.updated_at = utcnow()
    session.add(link)
    session.commit()
    session.refresh(link)
    return entity_link_details(session, link)


@router.delete("/directory/contacts/{contact_id}/links/{link_id}")
def remove_contact_link(
    contact_id: uuid.UUID,
    link_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    link = session.get(WhatsAppContactLink, link_id)
    if link is None or link.directory_contact_id != contact_id:
        raise HTTPException(status_code=404, detail="Contact link not found")
    link.active = False
    link.updated_at = utcnow()
    session.add(link)
    session.commit()
    return {"id": str(link.id), "active": False}
