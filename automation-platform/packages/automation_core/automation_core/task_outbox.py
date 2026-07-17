from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import or_, select, update
from sqlmodel import Session

from automation_core.celery_app import celery_app
from automation_core.models import Job, JobStatus, TaskOutbox
from automation_core.time import utcnow

PUBLISHABLE_STATUSES = {"pending", "failed"}


def stage_task(
    session: Session,
    *,
    job: Job,
    task_name: str,
    queue: str,
    args: list[Any] | None = None,
    kwargs: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> TaskOutbox:
    """Add a broker task to the current transaction without publishing it."""
    key = idempotency_key or f"job:{job.id}:{task_name}"
    existing = session.scalar(select(TaskOutbox).where(TaskOutbox.idempotency_key == key))
    if existing is not None:
        return existing
    task_id = str(uuid.uuid4())
    item = TaskOutbox(
        job_id=job.id,
        task_name=task_name,
        queue=queue,
        args=list(args or []),
        kwargs=dict(kwargs or {}),
        task_id=task_id,
        idempotency_key=key,
    )
    job.task_id = task_id
    session.add(job)
    session.add(item)
    session.flush()
    return item


def cancel_outbox_for_job(session: Session, job_id: uuid.UUID) -> int:
    rows = list(
        session.scalars(
            select(TaskOutbox).where(
                TaskOutbox.job_id == job_id,
                TaskOutbox.status.in_(["pending", "publishing", "failed"]),
            )
        )
    )
    now = utcnow()
    for item in rows:
        item.status = "cancelled"
        item.locked_at = None
        item.updated_at = now
        session.add(item)
    return len(rows)


def recover_stale_publications(session: Session, *, stale_after_seconds: int = 90) -> int:
    cutoff = utcnow() - timedelta(seconds=stale_after_seconds)
    rows = list(
        session.scalars(
            select(TaskOutbox).where(
                TaskOutbox.status == "publishing",
                TaskOutbox.locked_at.is_not(None),
                TaskOutbox.locked_at < cutoff,
            )
        )
    )
    now = utcnow()
    for item in rows:
        item.status = "pending"
        item.locked_at = None
        item.last_error = "Recovered publication interrupted before broker acknowledgement."
        item.updated_at = now
        session.add(item)
    if rows:
        session.commit()
    return len(rows)


def _claim_one(session: Session, outbox_id: uuid.UUID) -> TaskOutbox | None:
    now = utcnow()
    result = session.exec(
        update(TaskOutbox)
        .where(
            TaskOutbox.id == outbox_id,
            TaskOutbox.status.in_(sorted(PUBLISHABLE_STATUSES)),
            TaskOutbox.available_at <= now,
        )
        .values(status="publishing", locked_at=now, updated_at=now)
    )
    session.commit()
    if not result.rowcount:
        return None
    return session.get(TaskOutbox, outbox_id)


def publish_pending_tasks(session: Session, *, limit: int = 50) -> dict[str, int]:
    """Publish committed outbox rows with at-least-once delivery and stable task IDs."""
    recover_stale_publications(session)
    now = utcnow()
    statement = (
        select(TaskOutbox.id)
        .where(
            TaskOutbox.status.in_(sorted(PUBLISHABLE_STATUSES)),
            TaskOutbox.available_at <= now,
        )
        .order_by(TaskOutbox.created_at)
        .limit(limit)
    )
    ids = list(session.scalars(statement))
    stats = {"published": 0, "failed": 0, "cancelled": 0}
    for outbox_id in ids:
        item = _claim_one(session, outbox_id)
        if item is None:
            continue
        job = session.get(Job, item.job_id)
        if job is None or job.status == JobStatus.cancelled.value:
            item.status = "cancelled"
            item.locked_at = None
            item.last_error = "Owning job is missing or cancelled."
            item.updated_at = utcnow()
            session.add(item)
            session.commit()
            stats["cancelled"] += 1
            continue
        try:
            celery_app.send_task(
                item.task_name,
                args=list(item.args),
                kwargs=dict(item.kwargs),
                queue=item.queue,
                task_id=item.task_id,
            )
        except Exception as exc:
            item.attempts += 1
            item.status = "failed"
            item.locked_at = None
            item.last_error = f"{type(exc).__name__}: {exc}"
            item.available_at = utcnow() + timedelta(seconds=min(60, 2 ** min(item.attempts, 6)))
            item.updated_at = utcnow()
            session.add(item)
            session.commit()
            stats["failed"] += 1
            continue
        item.attempts += 1
        item.status = "published"
        item.locked_at = None
        item.published_at = utcnow()
        item.last_error = None
        item.updated_at = item.published_at
        session.add(item)
        session.commit()
        stats["published"] += 1
    return stats


def requeue_job_task(session: Session, job: Job, *, reason: str) -> TaskOutbox | None:
    existing = session.scalar(
        select(TaskOutbox).where(TaskOutbox.job_id == job.id).order_by(TaskOutbox.created_at.desc())
    )
    if existing and existing.status in {"pending", "publishing", "failed"}:
        return existing
    task_map = {
        "antidengue.report": ("antidengue_automation.run_report", "antidengue"),
        "whatsapp.dispatch_preview": ("whatsapp_gateway.compile_dispatch_preview", "antidengue"),
        "whatsapp.dispatch_send": ("whatsapp_gateway.send_approved_preview", "antidengue"),
    }
    mapped = task_map.get(job.type)
    if mapped is None:
        return None
    job.status = JobStatus.queued.value
    job.error = None
    job.started_at = None
    job.finished_at = None
    job.updated_at = utcnow()
    item = stage_task(
        session,
        job=job,
        task_name=mapped[0],
        queue=mapped[1],
        args=[str(job.id)],
        idempotency_key=f"recovery:{job.id}:{uuid.uuid4()}",
    )
    item.last_error = reason
    session.add(item)
    return item
