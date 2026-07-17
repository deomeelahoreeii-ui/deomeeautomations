from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from sqlmodel import Session, col, select

from automation_core.models import Artifact, Job, JobLog, JobStatus
from automation_core.time import utcnow


def create_job(
    session: Session,
    *,
    job_type: str,
    title: str,
    parameters: dict[str, Any],
) -> Job:
    job = Job(type=job_type, title=title, parameters=parameters)
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def get_job(session: Session, job_id: uuid.UUID | str) -> Job | None:
    return session.get(Job, uuid.UUID(str(job_id)))


def require_job(session: Session, job_id: uuid.UUID | str) -> Job:
    job = get_job(session, job_id)
    if job is None:
        raise ValueError(f"Job not found: {job_id}")
    return job


def list_jobs(
    session: Session,
    limit: int = 50,
    *,
    job_type: str | None = None,
) -> list[Job]:
    statement = select(Job)
    if job_type:
        statement = statement.where(Job.type == job_type)
    statement = statement.order_by(col(Job.created_at).desc()).limit(limit)
    return list(session.exec(statement))


def get_active_job(session: Session, job_type: str) -> Job | None:
    statement = (
        select(Job)
        .where(Job.type == job_type)
        .where(Job.status.in_([JobStatus.queued.value, JobStatus.running.value]))
        .order_by(col(Job.created_at).desc())
        .limit(1)
    )
    return session.exec(statement).first()


def set_task_id(session: Session, job_id: uuid.UUID | str, task_id: str) -> Job:
    job = require_job(session, job_id)
    job.task_id = task_id
    job.updated_at = utcnow()
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def mark_job_running(session: Session, job_id: uuid.UUID | str) -> Job:
    job = require_job(session, job_id)
    job.status = JobStatus.running.value
    job.started_at = utcnow()
    job.updated_at = job.started_at
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def mark_job_succeeded(
    session: Session, job_id: uuid.UUID | str, result: dict[str, Any] | None = None
) -> Job:
    job = require_job(session, job_id)
    job.status = JobStatus.succeeded.value
    job.result = result or {}
    job.error = None
    job.finished_at = utcnow()
    job.updated_at = job.finished_at
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def mark_job_failed(session: Session, job_id: uuid.UUID | str, error: str) -> Job:
    job = require_job(session, job_id)
    job.status = JobStatus.failed.value
    job.error = error
    job.finished_at = utcnow()
    job.updated_at = job.finished_at
    session.add(job)
    session.commit()
    session.refresh(job)
    return job


def append_log(
    session: Session,
    job_id: uuid.UUID | str,
    message: str,
    *,
    level: str = "info",
) -> JobLog:
    log = JobLog(job_id=uuid.UUID(str(job_id)), level=level, message=message)
    job = require_job(session, job_id)
    job.updated_at = utcnow()
    session.add(log)
    session.add(job)
    session.commit()
    session.refresh(log)
    return log


def list_logs(session: Session, job_id: uuid.UUID | str, limit: int = 500) -> list[JobLog]:
    statement = (
        select(JobLog)
        .where(JobLog.job_id == uuid.UUID(str(job_id)))
        .order_by(JobLog.id)
        .limit(limit)
    )
    return list(session.exec(statement))


def record_artifact(
    session: Session,
    job_id: uuid.UUID | str,
    path: Path,
    *,
    kind: str = "file",
    name: str | None = None,
) -> Artifact:
    artifact = Artifact(
        job_id=uuid.UUID(str(job_id)),
        kind=kind,
        name=name or path.name,
        path=str(path),
        size_bytes=path.stat().st_size if path.exists() else 0,
    )
    session.add(artifact)
    session.commit()
    session.refresh(artifact)
    return artifact


def list_artifacts(session: Session, job_id: uuid.UUID | str) -> list[Artifact]:
    statement = (
        select(Artifact)
        .where(Artifact.job_id == uuid.UUID(str(job_id)))
        .order_by(Artifact.created_at)
    )
    return list(session.exec(statement))
