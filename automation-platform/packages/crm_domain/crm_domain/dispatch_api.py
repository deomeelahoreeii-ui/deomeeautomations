from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.dispatch import (
    CrmDispatchError,
    CrmDispatchConflict,
    CrmDispatchNotFound,
    CrmDispatchService,
    CrmDispatchValidationError,
)

router = APIRouter(prefix="/api/v1/crm/dispatch", tags=["crm-dispatch"])


class DestinationProfileInput(BaseModel):
    id: uuid.UUID | None = None
    name: str = Field(min_length=2, max_length=200)
    target_type: str
    target_ids: list[uuid.UUID]
    template_body: str = Field(default="", max_length=10_000)
    packet_policy: str = "complete_pdf"
    privacy_policy: str = "full"
    office_level: str = "both"
    allowed_directions: list[str] = Field(default_factory=lambda: ["downward", "upward"])
    max_packet_bytes: int = 15 * 1024 * 1024
    require_approval: bool = True
    messages_per_minute: int = 20
    max_retries: int = 5
    enabled: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class EnabledInput(BaseModel):
    enabled: bool


class RoutingRuleInput(BaseModel):
    id: uuid.UUID | None = None
    name: str = Field(min_length=2, max_length=200)
    description: str | None = Field(default=None, max_length=5_000)
    priority: int = Field(default=100, ge=-10_000, le=10_000)
    selection_mode: str = "suggested"
    conditions: dict[str, Any] = Field(default_factory=dict)
    profile_ids: list[uuid.UUID]
    stop_after_match: bool = True
    enabled: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class BatchInput(BaseModel):
    direction: str = Field(default="downward", max_length=20)
    case_ids: list[uuid.UUID] = Field(default_factory=list)
    official_letter_ids: list[uuid.UUID] = Field(default_factory=list)
    purpose: str | None = Field(default=None, max_length=80)
    response_due_at: datetime | None = None
    actor: str = Field(default="web-operator", max_length=120)


class ActorInput(BaseModel):
    actor: str = Field(default="web-operator", max_length=120)


class BatchApprovalInput(BaseModel):
    acknowledge_warnings: bool = False
    acknowledge_exclusions: bool = False
    approved_by: str = Field(default="web-operator", max_length=120)


class ManualRouteInput(BaseModel):
    profile_ids: list[uuid.UUID]
    reason: str = Field(default="", max_length=2_000)
    actor: str = Field(default="web-operator", max_length=120)


class ExclusionInput(BaseModel):
    excluded: bool
    reason: str = Field(default="", max_length=2_000)
    actor: str = Field(default="web-operator", max_length=120)


def service(session: Session, settings: Settings | None = None) -> CrmDispatchService:
    return CrmDispatchService(session, settings)


def fail(exc: CrmDispatchError) -> None:
    if isinstance(exc, CrmDispatchNotFound):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, CrmDispatchConflict):
        raise HTTPException(status_code=409, detail=exc.detail) from exc
    if isinstance(exc, CrmDispatchValidationError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/bootstrap")
def bootstrap(session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        result = service(session).ensure_defaults()
        return {
            "application_id": str(result["application"].id),
            "application_name": result["application"].name,
            "report_type_id": str(result["report_type"].id),
            "report_type_name": result["report_type"].name,
            "account_id": str(result["account"].id),
            "account_name": result["account"].name,
            "template_id": str(result["template"].id),
        }
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/statistics")
def statistics(session: Session = Depends(get_session)) -> dict[str, int]:
    return service(session).statistics()


@router.get("/profiles")
def profiles(
    search: str = Query(default="", max_length=300),
    include_inactive: bool = False,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        items = service(session).profiles(search=search, include_inactive=include_inactive)
        return {"items": items, "total": len(items)}
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/profiles", status_code=status.HTTP_201_CREATED)
def save_profile(
    payload: DestinationProfileInput, session: Session = Depends(get_session)
) -> dict[str, Any]:
    try:
        return service(session).save_profile(
            profile_id=payload.id,
            name=payload.name,
            target_type=payload.target_type,
            target_ids=payload.target_ids,
            template_body=payload.template_body,
            packet_policy=payload.packet_policy,
            privacy_policy=payload.privacy_policy,
            office_level=payload.office_level,
            allowed_directions=payload.allowed_directions,
            max_packet_bytes=payload.max_packet_bytes,
            require_approval=payload.require_approval,
            messages_per_minute=payload.messages_per_minute,
            max_retries=payload.max_retries,
            enabled=payload.enabled,
            actor=payload.actor,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.patch("/profiles/{profile_id}/enabled")
def set_profile_enabled(
    profile_id: uuid.UUID,
    payload: EnabledInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).set_profile_enabled(profile_id, payload.enabled)
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/rules")
def rules(include_inactive: bool = True, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        items = service(session).rules(include_inactive=include_inactive)
        return {"items": items, "total": len(items)}
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/rules", status_code=status.HTTP_201_CREATED)
def save_rule(payload: RoutingRuleInput, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).save_rule(
            rule_id=payload.id,
            name=payload.name,
            description=payload.description,
            priority=payload.priority,
            selection_mode=payload.selection_mode,
            conditions=payload.conditions,
            profile_ids=payload.profile_ids,
            stop_after_match=payload.stop_after_match,
            enabled=payload.enabled,
            actor=payload.actor,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        service(session).delete_rule(rule_id)
        return {"id": str(rule_id), "enabled": False}
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/routing-test/{case_id}")
def routing_test(
    case_id: uuid.UUID,
    direction: str = Query(default="downward", max_length=20),
    purpose: str = Query(default="compliance_request", max_length=80),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).test_routing(case_id, direction=direction, purpose=purpose)
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/eligible-sources")
def eligible_sources(
    direction: str = Query(default="downward", max_length=20),
    search: str = Query(default="", max_length=300),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=1, le=500),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return service(session, settings).eligible_sources(
            direction=direction, search=search, page=page, page_size=page_size
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches", status_code=status.HTTP_201_CREATED)
def create_batch(
    payload: BatchInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return service(session, settings).create_batch(
            direction=payload.direction,
            case_ids=payload.case_ids,
            official_letter_ids=payload.official_letter_ids,
            actor=payload.actor,
            purpose=payload.purpose,
            response_due_at=payload.response_due_at,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/artifacts/{artifact_id}/download")
def download_dispatch_artifact(
    artifact_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    from crm_domain.models import CrmDispatchArtifact
    from pathlib import Path

    artifact = session.get(CrmDispatchArtifact, artifact_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail="Dispatch packet was not found")
    path = Path(artifact.path).expanduser().resolve()
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Dispatch packet file is unavailable")
    return FileResponse(path, media_type=artifact.content_type, filename=artifact.name)


@router.get("/batches")
def list_batches(
    search: str = Query(default="", max_length=300),
    status_filter: str = Query(default="", alias="status", max_length=40),
    direction: str = Query(default="", max_length=20),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    return service(session).list_batches(
        search=search, status=status_filter, direction=direction, page=page, page_size=page_size
    )


@router.get("/deliveries")
def delivery_history(
    search: str = Query(default="", max_length=300),
    status_filter: str = Query(default="", alias="status", max_length=40),
    direction: str = Query(default="", max_length=20),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    return service(session).list_delivery_history(
        search=search, status=status_filter, direction=direction, page=page, page_size=page_size
    )


@router.get("/submissions")
def upward_submissions(
    search: str = Query(default="", max_length=300),
    phase: str = Query(default="sent", pattern="^(sent|in_progress|attention|all)$"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).list_upward_submissions(
            search=search, phase=phase, page=page, page_size=page_size
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/batches/{batch_id}")
def detail(batch_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).detail(batch_id)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/resolve")
def resolve(batch_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).resolve_batch(batch_id)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/compile")
def compile_batch(
    batch_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).compile_previews(batch_id, actor=payload.actor)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/approve")
async def approve_batch(
    batch_id: uuid.UUID,
    payload: BatchApprovalInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    from whatsapp_gateway.models import (
        WhatsAppDispatchApproval,
        WhatsAppDispatchPreview,
        WhatsAppDispatchPreviewArtifact,
        WhatsAppDispatchPreviewDelivery,
    )
    from whatsapp_gateway.previews.approval import approve_preview
    from whatsapp_gateway.previews.schemas import PreviewApprovalInput
    from whatsapp_gateway.previews.state import summarize_preview_state

    try:
        current = service(session).detail(batch_id)
    except CrmDispatchError as exc:
        fail(exc)
    pending = [item for item in current.get("previews", []) if not item.get("approval")]
    if not pending:
        raise HTTPException(
            status_code=409, detail="This CRM dispatch batch has no unapproved frozen previews"
        )

    eligible: list[tuple[uuid.UUID, int, int]] = []
    blocked: list[dict[str, Any]] = []
    for item in pending:
        preview_id = uuid.UUID(str(item["id"]))
        preview = session.get(WhatsAppDispatchPreview, preview_id)
        if preview is None:
            blocked.append({"preview_id": str(preview_id), "reason": "Preview no longer exists"})
            continue
        deliveries = list(
            session.scalars(
                select(WhatsAppDispatchPreviewDelivery).where(
                    WhatsAppDispatchPreviewDelivery.preview_id == preview_id
                )
            ).all()
        )
        artifacts = list(
            session.scalars(
                select(WhatsAppDispatchPreviewArtifact).where(
                    WhatsAppDispatchPreviewArtifact.preview_id == preview_id
                )
            ).all()
        )
        summary = summarize_preview_state(deliveries, preview.issues or [], artifacts)
        if summary.eligible_delivery_count <= 0:
            blocked.append(
                {
                    "preview_id": str(preview_id),
                    "preview_key": preview.preview_key,
                    "reason": "No new eligible recipient remains; the delivery is blocked, excluded, or already sent",
                    "excluded_count": summary.excluded_delivery_count,
                }
            )
            continue
        eligible.append((preview_id, summary.warning_issue_count, summary.excluded_delivery_count))

    if not eligible:
        service(session).refresh(batch_id)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "No new WhatsApp delivery is eligible. Resolve the route or review the existing delivery history.",
                "queued": 0,
                "blocked": blocked,
            },
        )

    jobs: list[dict[str, Any]] = []
    queued = 0
    excluded = 0
    for preview_id, warning_count, excluded_count in eligible:
        job = await approve_preview(
            preview_id,
            PreviewApprovalInput(
                acknowledge_warnings=payload.acknowledge_warnings or warning_count == 0,
                acknowledge_exclusions=payload.acknowledge_exclusions or excluded_count == 0,
                approved_by=payload.approved_by,
            ),
            session,
        )
        approval = session.scalar(
            select(WhatsAppDispatchApproval).where(
                WhatsAppDispatchApproval.preview_id == preview_id
            )
        )
        actual = int(approval.delivery_count if approval else 0)
        queued += actual
        excluded += int(approval.excluded_count if approval else excluded_count)
        jobs.append(
            {
                "preview_id": str(preview_id),
                "job_id": str(job.id),
                "job_status": job.status,
                "queued": actual,
                "no_op": bool((job.result or {}).get("no_op"))
                if isinstance(job.result, dict)
                else False,
            }
        )

    refreshed = service(session).refresh(batch_id)
    if queued <= 0:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Approval completed, but no new WhatsApp delivery was queued.",
                "queued": 0,
                "excluded": excluded,
                "blocked": blocked,
                "jobs": jobs,
            },
        )
    return {
        "message": f"Queued {queued} real WhatsApp delivery{'ies' if queued != 1 else ''}.",
        "queued": queued,
        "excluded": excluded,
        "blocked": blocked,
        "jobs": jobs,
        "batch": refreshed,
    }


@router.post("/batches/{batch_id}/refresh")
def refresh(batch_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).refresh(batch_id)
    except CrmDispatchError as exc:
        fail(exc)


@router.put("/items/{item_id}/route")
def manual_route(
    item_id: uuid.UUID,
    payload: ManualRouteInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).set_manual_route(
            item_id=item_id,
            profile_ids=payload.profile_ids,
            reason=payload.reason,
            actor=payload.actor,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.put("/items/{item_id}/excluded")
def exclude_item(
    item_id: uuid.UUID,
    payload: ExclusionInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).set_item_excluded(
            item_id,
            excluded=payload.excluded,
            reason=payload.reason,
            actor=payload.actor,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/discard")
def discard_upward_batch(
    batch_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).discard_upward_batch(batch_id, actor=payload.actor)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/retry-failed", status_code=status.HTTP_202_ACCEPTED)
def retry_failed_upward_deliveries(
    batch_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    from automation_core.job_service import add_job, add_job_log
    from automation_core.models import Job, JobStatus, JobType
    from automation_core.task_outbox import publish_pending_tasks, stage_task
    from crm_domain.models import CrmDispatchBatch, CrmDispatchItem, CrmDispatchTarget
    from whatsapp_gateway.models import (
        WhatsAppActivity,
        WhatsAppDelivery,
        WhatsAppDispatchApproval,
    )

    batch = session.get(CrmDispatchBatch, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Dispatch batch not found")
    if batch.direction != "upward":
        raise HTTPException(
            status_code=409,
            detail="Only failed upward compliance deliveries can be retried here",
        )
    preview_ids = list(
        session.scalars(
            select(CrmDispatchTarget.preview_id)
            .join(
                CrmDispatchItem,
                CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id,
            )
            .where(
                CrmDispatchItem.batch_id == batch.id,
                CrmDispatchTarget.preview_id.is_not(None),
            )
        ).all()
    )
    approvals = list(
        session.scalars(
            select(WhatsAppDispatchApproval).where(
                WhatsAppDispatchApproval.preview_id.in_(preview_ids)
            ).with_for_update()
        ).all()
    )
    staged: list[dict[str, Any]] = []
    total = 0
    for approval in approvals:
        if approval.status in {"queued", "sending"}:
            raise HTTPException(
                status_code=409,
                detail="A delivery attempt for this approval is already active",
            )
        failed = list(
            session.scalars(
                select(WhatsAppDelivery).where(
                    WhatsAppDelivery.approval_id == approval.id,
                    WhatsAppDelivery.status == "failed",
                ).with_for_update()
            ).all()
        )
        if not failed:
            continue
        original_job = session.get(Job, approval.job_id)
        if original_job is None or original_job.status in {
            JobStatus.queued.value,
            JobStatus.running.value,
        }:
            raise HTTPException(
                status_code=409,
                detail="A failed delivery job is unavailable or already active",
            )
        retry_ids = [str(delivery.id) for delivery in failed]
        retry_job = add_job(
            session,
            job_type=JobType.whatsapp_dispatch_send.value,
            title=f"Retry failed deliveries from approval {approval.id}",
            parameters={
                "approval_id": str(approval.id),
                "retry_delivery_ids": retry_ids,
                "retry_of_job_id": str(original_job.id),
                "retry_requested_by": payload.actor,
            },
        )
        for delivery in failed:
            session.add(
                WhatsAppActivity(
                    account_id=delivery.account_id,
                    level="info",
                    event_type="approved_delivery_retry_requested",
                    message=(
                        f"Retry requested for failed approved delivery to "
                        f"{delivery.recipient_name}"
                    ),
                    details={
                        "delivery_id": str(delivery.id),
                        "approval_id": str(approval.id),
                        "retry_job_id": str(retry_job.id),
                        "previous_status": delivery.status,
                        "previous_error": delivery.error,
                        "previous_provider_result": delivery.provider_result,
                        "requested_by": payload.actor,
                    },
                )
            )
            delivery.status = "queued"
            delivery.error = None
            delivery.provider_result = None
            delivery.queue_stream = None
            delivery.queue_sequence = None
            delivery.completed_at = None
            session.add(delivery)
        approval.status = "queued"
        approval.error = None
        approval.completed_at = None
        session.add(approval)
        add_job_log(
            session,
            retry_job.id,
            (
                f"Staged CRM retry for {len(failed)} explicitly failed "
                f"delivery(ies) by {payload.actor}."
            ),
        )
        stage_task(
            session,
            job=retry_job,
            task_name="whatsapp_gateway.send_approved_preview",
            queue="antidengue",
            args=[str(retry_job.id)],
            idempotency_key=(
                f"crm-dispatch:{batch.id}:approval:{approval.id}:"
                f"retry-job:{retry_job.id}"
            ),
        )
        total += len(failed)
        staged.append(
            {
                "approval_id": str(approval.id),
                "job_id": str(retry_job.id),
                "delivery_ids": retry_ids,
            }
        )
    if not total:
        ambiguous = session.scalar(
            select(WhatsAppDelivery.id)
            .join(
                WhatsAppDispatchApproval,
                WhatsAppDispatchApproval.id == WhatsAppDelivery.approval_id,
            )
            .where(
                WhatsAppDispatchApproval.preview_id.in_(preview_ids),
                WhatsAppDelivery.status == "timed_out",
            )
            .limit(1)
        )
        raise HTTPException(
            status_code=409,
            detail=(
                "Timed-out deliveries have an ambiguous send outcome and cannot be "
                "automatically retried; reconcile them before resending."
                if ambiguous
                else "This batch has no explicitly failed destinations to retry"
            ),
        )
    session.commit()
    refreshed = service(session).refresh(batch.id)
    publish_pending_tasks(session, limit=10)
    return {"retried": total, "staged": staged, "batch": refreshed}
