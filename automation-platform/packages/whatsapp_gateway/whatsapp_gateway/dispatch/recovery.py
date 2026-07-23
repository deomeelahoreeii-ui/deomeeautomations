from __future__ import annotations

import uuid
from datetime import timedelta

from sqlmodel import Session, select

from automation_core.models import Job, JobLog, JobStatus, JobType
from automation_core.task_outbox import cancel_outbox_for_job
from automation_core.time import utcnow
from whatsapp_gateway.dispatch.source_reconciliation import reconcile_approval_source
from whatsapp_gateway.models import (
    WhatsAppActivity,
    WhatsAppDelivery,
    WhatsAppDispatchApproval,
)


ACTIVE_JOB_STATUSES = frozenset({JobStatus.queued.value, JobStatus.running.value})


def recover_active_dispatch_jobs(
    session: Session,
    *,
    all_active: bool = False,
    older_than_minutes: int = 20,
    reason: str | None = None,
) -> dict[str, int]:
    """Close orphaned dispatch attempts without creating a duplicate send."""

    statement = select(Job).where(
        Job.type == JobType.whatsapp_dispatch_send.value,
        Job.status.in_(sorted(ACTIVE_JOB_STATUSES)),
    )
    if not all_active:
        cutoff = utcnow() - timedelta(minutes=max(1, older_than_minutes))
        statement = statement.where(Job.updated_at < cutoff)
    jobs = list(session.exec(statement).all())
    stats = {
        "jobs_failed": 0,
        "deliveries_failed": 0,
        "deliveries_ambiguous": 0,
        "outbox_cancelled": 0,
        "sources_reconciled": 0,
    }
    approval_ids: set[uuid.UUID] = set()
    now = utcnow()
    recovery_reason = reason or (
        "The dispatch process stopped before this attempt reached a terminal state."
    )

    for job in jobs:
        try:
            approval_id = uuid.UUID(str(job.parameters.get("approval_id")))
        except (TypeError, ValueError, AttributeError):
            approval_id = None
        retry_ids: set[uuid.UUID] = set()
        for value in job.parameters.get("retry_delivery_ids", []):
            try:
                retry_ids.add(uuid.UUID(str(value)))
            except (TypeError, ValueError, AttributeError):
                continue
        if approval_id is not None:
            approval = session.get(WhatsAppDispatchApproval, approval_id)
            if approval is not None:
                approval_ids.add(approval.id)
                approval.status = "failed"
                approval.error = recovery_reason
                approval.completed_at = now
                session.add(approval)
                delivery_statement = select(WhatsAppDelivery).where(
                    WhatsAppDelivery.approval_id == approval.id,
                    WhatsAppDelivery.status == "queued",
                )
                if retry_ids:
                    delivery_statement = delivery_statement.where(
                        WhatsAppDelivery.id.in_(retry_ids)
                    )
                for delivery in session.exec(delivery_statement).all():
                    queue_was_accepted = bool(
                        (delivery.provider_result or {}).get("queueAccepted")
                    )
                    if queue_was_accepted:
                        delivery.status = "timed_out"
                        delivery.error = (
                            "Dispatch process stopped after the broker accepted this "
                            "attempt; outcome is ambiguous and requires reconciliation."
                        )
                        stats["deliveries_ambiguous"] += 1
                    else:
                        delivery.status = "failed"
                        delivery.error = recovery_reason
                        stats["deliveries_failed"] += 1
                    delivery.completed_at = now
                    session.add(delivery)
                    session.add(
                        WhatsAppActivity(
                            account_id=delivery.account_id,
                            level="error",
                            event_type="approved_delivery_recovered_after_restart",
                            message=(
                                f"Recovered {delivery.status} delivery attempt for "
                                f"{delivery.recipient_name}"
                            ),
                            details={
                                "delivery_id": str(delivery.id),
                                "approval_id": str(approval.id),
                                "dispatch_job_id": str(job.id),
                                "queue_was_accepted": queue_was_accepted,
                                "error": delivery.error,
                            },
                        )
                    )

        job.status = JobStatus.failed.value
        job.error = recovery_reason
        job.finished_at = now
        job.updated_at = now
        session.add(job)
        session.add(JobLog(job_id=job.id, level="error", message=recovery_reason))
        stats["outbox_cancelled"] += cancel_outbox_for_job(session, job.id)
        stats["jobs_failed"] += 1

    if jobs:
        session.commit()

    for approval_id in approval_ids:
        try:
            result = reconcile_approval_source(approval_id)
            stats["sources_reconciled"] += int(bool(result.get("reconciled")))
        except Exception:
            # The persisted terminal delivery state remains authoritative. The
            # source workflow's manual reconciliation action can retry projection.
            continue
    return stats


__all__ = ["recover_active_dispatch_jobs"]
