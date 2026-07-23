from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from sqlalchemy import select
from sqlmodel import Session

from automation_core.celery_app import celery_app
from automation_core.database import engine
from automation_core.job_service import (
    append_log, claim_job_running, get_job, mark_job_failed, mark_job_succeeded,
)
from automation_core.time import utcnow
from automation_core.task_delivery import discarded_missing_job_delivery
from whatsapp_gateway.dispatch.approved_delivery import _publish_approved_deliveries
from whatsapp_gateway.dispatch.retry_delivery import _publish_selected_approved_deliveries
from whatsapp_gateway.dispatch.source_reconciliation import reconcile_approval_source
from whatsapp_gateway.models import (
    WhatsAppActivity, WhatsAppDelivery, WhatsAppDispatchApproval,
)
from whatsapp_gateway.preview_service import compile_antidengue_preview
from whatsapp_gateway.previews.compiler.capabilities import (
    PREVIEW_COMPILER_TASK, contract_mismatches, local_compiler_runtime,
)


logger = logging.getLogger(__name__)


@celery_app.task(
    name="whatsapp_gateway.compile_dispatch_preview",
    soft_time_limit=60 * 10,
    time_limit=60 * 12,
)
def compile_dispatch_preview_job(job_id: str) -> dict[str, str]:
    """Compatibility consumer for preview jobs queued before protocol v2."""
    return _compile_dispatch_preview(job_id, enforce_contract=False, worker_name="legacy")


@celery_app.task(
    bind=True,
    name=PREVIEW_COMPILER_TASK,
    soft_time_limit=60 * 10,
    time_limit=60 * 12,
)
def compile_dispatch_preview_v2_job(self, job_id: str) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return _compile_dispatch_preview(
        job_id,
        enforce_contract=True,
        worker_name=str(getattr(self.request, "hostname", "") or "automation-worker"),
    )


def _compile_dispatch_preview(
    job_id: str, *, enforce_contract: bool, worker_name: str
) -> dict[str, Any]:
    uuid.UUID(job_id)
    with Session(engine) as session:
        job = claim_job_running(session, job_id)
        if job is None:
            existing = get_job(session, job_id)
            if existing is None:
                return discarded_missing_job_delivery(
                    job_id,
                    task_name=(
                        PREVIEW_COMPILER_TASK
                        if enforce_contract
                        else "whatsapp_gateway.compile_dispatch_preview"
                    ),
                    logger=logger,
                )
            return {**dict(existing.result or {}), "deduplicated": True, "job_status": existing.status}
        parameters = dict(job.parameters)
        append_log(session, job_id, "Compiling immutable AntiDengue dispatch preview.")

    try:
        runtime = local_compiler_runtime(worker_name=worker_name)
        if enforce_contract:
            required = dict(parameters.get("compiler_contract") or {})
            problems = contract_mismatches(required, runtime)
            if not required or problems:
                explanation = "; ".join(problems) or "the job has no compiler capability contract"
                raise RuntimeError(
                    f"Preview worker capability mismatch: {explanation}. "
                    "No preview was created; restart or deploy a compatible AntiDengue worker."
                )
            from automation_core.worker_runtime import touch_worker_runtime

            with Session(engine) as session:
                touch_worker_runtime(session, worker_name)
        with Session(engine) as session:
            preview = compile_antidengue_preview(
                session,
                source_job_id=uuid.UUID(parameters["source_job_id"]),
                dispatch_profile_id=uuid.UUID(parameters["dispatch_profile_id"]),
                dispatch_profile_ids=[
                    uuid.UUID(value) for value in parameters.get("dispatch_profile_ids", [])
                ] or None,
                created_by=str(parameters.get("created_by") or "web"),
                compiler_runtime=runtime,
                deadline_snapshot=dict(parameters.get("deadline_snapshot") or {}) or None,
            )
            result = {"preview_id": str(preview.id), "preview_key": preview.preview_key}
        with Session(engine) as session:
            append_log(session, job_id, f"Frozen preview {result['preview_key']} is ready for review.")
            mark_job_succeeded(session, job_id, result)
        return result
    except Exception as exc:
        with Session(engine) as session:
            append_log(session, job_id, str(exc), level="error")
            mark_job_failed(session, job_id, str(exc))
        raise


@celery_app.task(
    name="whatsapp_gateway.send_approved_preview",
    soft_time_limit=60 * 15,
    time_limit=60 * 20,
)
def send_approved_preview_job(job_id: str) -> dict[str, Any]:
    with Session(engine) as session:
        job = claim_job_running(session, job_id)
        if job is None:
            existing = get_job(session, job_id)
            if existing is None:
                return discarded_missing_job_delivery(
                    job_id,
                    task_name="whatsapp_gateway.send_approved_preview",
                    logger=logger,
                )
            return {**dict(existing.result or {}), "deduplicated": True, "job_status": existing.status}
        # ORM instances must not cross this committing log call or the Session
        # boundary below. Snapshot every primitive task input while attached.
        parameters = dict(job.parameters)
        approval_id = uuid.UUID(str(parameters["approval_id"]))
        delivery_ids = [
            uuid.UUID(str(value))
            for value in parameters.get("retry_delivery_ids", [])
        ]
        append_log(session, job_id, "Revalidating and queueing the exact approved frozen payloads.")
    try:
        result = asyncio.run(
            _publish_selected_approved_deliveries(approval_id, job_id, delivery_ids)
            if delivery_ids
            else _publish_approved_deliveries(approval_id, job_id)
        )
        with Session(engine) as session:
            append_log(session, job_id, f"Dispatch completed: {result['delivered']} successful, {result['failed']} failed.")
            mark_job_succeeded(session, job_id, result)
        return result
    except Exception as exc:
        with Session(engine) as session:
            approval = session.get(WhatsAppDispatchApproval, approval_id)
            if approval:
                approval.status = "failed"
                approval.error = str(exc)
                approval.completed_at = utcnow()
                session.add(approval)
                failed_statement = select(WhatsAppDelivery).where(
                    WhatsAppDelivery.approval_id == approval.id,
                    WhatsAppDelivery.status == "queued",
                )
                if delivery_ids:
                    failed_statement = failed_statement.where(
                        WhatsAppDelivery.id.in_(delivery_ids)
                    )
                for delivery in session.scalars(failed_statement):
                    queue_was_accepted = bool(
                        (delivery.provider_result or {}).get("queueAccepted")
                    )
                    delivery.status = (
                        "timed_out" if queue_was_accepted else "failed"
                    )
                    delivery.error = (
                        "Dispatch execution was interrupted after the broker accepted "
                        "this attempt; outcome is ambiguous and requires reconciliation."
                        if queue_was_accepted
                        else str(exc)
                    )
                    delivery.completed_at = utcnow()
                    session.add(delivery)
                    session.add(
                        WhatsAppActivity(
                            account_id=delivery.account_id,
                            level="error",
                            event_type="approved_delivery_dispatch_interrupted",
                            message=(
                                f"Approved delivery {delivery.status} after dispatch "
                                f"interruption for {delivery.recipient_name}"
                            ),
                            details={
                                "delivery_id": str(delivery.id),
                                "approval_id": str(approval.id),
                                "dispatch_job_id": job_id,
                                "queue_was_accepted": queue_was_accepted,
                                "error": delivery.error,
                            },
                        )
                    )
                session.commit()
            append_log(session, job_id, str(exc), level="error")
            mark_job_failed(session, job_id, str(exc))
        try:
            reconcile_approval_source(approval_id)
        except Exception:
            logger.exception(
                "dispatch.source_reconciliation.failed_after_dispatch_error",
                extra={"context": {"approval_id": str(approval_id)}},
            )
        raise
