from __future__ import annotations

import asyncio
import uuid

from sqlalchemy import select
from sqlmodel import Session

from automation_core.celery_app import celery_app
from automation_core.database import engine
from automation_core.database_identity import database_identity
from automation_core.job_service import (
    append_log, claim_job_running, get_job, mark_job_failed, mark_job_succeeded,
)
from automation_core.time import utcnow
from whatsapp_gateway.dispatch.approved_delivery import _publish_approved_deliveries
from whatsapp_gateway.dispatch.retry_delivery import _publish_selected_approved_deliveries
from whatsapp_gateway.models import (
    WhatsAppDelivery, WhatsAppDispatchApproval,
)
from whatsapp_gateway.preview_service import compile_antidengue_preview
from whatsapp_gateway.previews.compiler.capabilities import (
    PREVIEW_COMPILER_TASK, contract_mismatches, local_compiler_runtime,
)


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
def compile_dispatch_preview_v2_job(self, job_id: str) -> dict[str, str]:  # type: ignore[no-untyped-def]
    return _compile_dispatch_preview(
        job_id,
        enforce_contract=True,
        worker_name=str(getattr(self.request, "hostname", "") or "automation-worker"),
    )


def _compile_dispatch_preview(
    job_id: str, *, enforce_contract: bool, worker_name: str
) -> dict[str, str]:
    uuid.UUID(job_id)
    with Session(engine) as session:
        job = claim_job_running(session, job_id)
        if job is None:
            existing = get_job(session, job_id)
            if existing is None:
                identity = database_identity()
                raise ValueError(
                    f"Job not found after durable outbox publication: {job_id}; "
                    f"worker_database={identity['fingerprint']} ({identity['display']})"
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
def send_approved_preview_job(job_id: str) -> dict[str, int]:
    with Session(engine) as session:
        job = claim_job_running(session, job_id)
        if job is None:
            existing = get_job(session, job_id)
            if existing is None:
                identity = database_identity()
                raise ValueError(
                    f"Job not found after durable outbox publication: {job_id}; "
                    f"worker_database={identity['fingerprint']} ({identity['display']})"
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
                for delivery in session.scalars(
                    select(WhatsAppDelivery).where(
                        WhatsAppDelivery.approval_id == approval.id,
                        WhatsAppDelivery.status == "queued",
                    )
                ):
                    delivery.status = "failed"
                    delivery.error = str(exc)
                    delivery.completed_at = utcnow()
                    session.add(delivery)
                session.commit()
            append_log(session, job_id, str(exc), level="error")
            mark_job_failed(session, job_id, str(exc))
        raise
