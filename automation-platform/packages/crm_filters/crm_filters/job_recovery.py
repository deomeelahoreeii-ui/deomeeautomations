from __future__ import annotations

from datetime import timedelta

from sqlmodel import Session, select

from automation_core.models import Job, JobLog, JobStatus, JobType
from automation_core.time import utcnow

CRM_JOB_TYPES = (
    JobType.crm_sheet_filter.value,
    JobType.crm_pdf_filter.value,
    JobType.crm_sheet_to_pdf.value,
)
ACTIVE_JOB_STATUSES = (JobStatus.queued.value, JobStatus.running.value)


def fail_stale_crm_jobs(
    session: Session,
    *,
    older_than_minutes: int,
    reason: str | None = None,
) -> list[Job]:
    """Mark CRM jobs with no recent heartbeat as failed.

    Job logs update ``Job.updated_at``. A CRM worker therefore keeps a long-running
    job alive simply by reporting progress. If the worker exits, the timestamp stops
    changing and this reconciler prevents the UI from showing ``running`` forever.
    """

    minutes = max(1, int(older_than_minutes))
    cutoff = utcnow() - timedelta(minutes=minutes)
    jobs = list(
        session.exec(
            select(Job)
            .where(Job.type.in_(CRM_JOB_TYPES))
            .where(Job.status.in_(ACTIVE_JOB_STATUSES))
            .where(Job.updated_at < cutoff)
        )
    )
    if not jobs:
        return []

    now = utcnow()
    message = reason or (
        f"The CRM worker stopped reporting progress for more than {minutes} minute(s). "
        "The stale run was closed automatically; start a new run after confirming the worker is ready."
    )
    for job in jobs:
        job.status = JobStatus.failed.value
        job.error = message
        job.finished_at = now
        job.updated_at = now
        session.add(job)
        session.add(JobLog(job_id=job.id, level="error", message=message))
    session.commit()
    for job in jobs:
        session.refresh(job)
    return jobs


def fail_all_active_crm_jobs(session: Session, *, reason: str) -> list[Job]:
    """Close active CRM jobs during a confirmed local development restart."""

    jobs = list(
        session.exec(
            select(Job)
            .where(Job.type.in_(CRM_JOB_TYPES))
            .where(Job.status.in_(ACTIVE_JOB_STATUSES))
        )
    )
    if not jobs:
        return []

    now = utcnow()
    for job in jobs:
        job.status = JobStatus.failed.value
        job.error = reason
        job.finished_at = now
        job.updated_at = now
        session.add(job)
        session.add(JobLog(job_id=job.id, level="error", message=reason))
    session.commit()
    for job in jobs:
        session.refresh(job)
    return jobs
