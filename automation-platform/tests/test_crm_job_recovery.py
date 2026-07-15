from __future__ import annotations

from datetime import timedelta
from unittest.mock import patch

import pytest
from celery.exceptions import Ignore
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

from automation_core.models import Job, JobLog, JobStatus, JobType
from automation_core.time import utcnow
from crm_filters.job_recovery import fail_stale_crm_jobs
from crm_filters import tasks


def memory_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def test_stale_crm_job_is_failed_with_explanatory_log() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        job = Job(
            type=JobType.crm_sheet_filter.value,
            title="Stale CRM run",
            status=JobStatus.running.value,
            parameters={},
            updated_at=utcnow() - timedelta(minutes=30),
        )
        session.add(job)
        session.commit()
        session.refresh(job)

        recovered = fail_stale_crm_jobs(session, older_than_minutes=15)

        assert [item.id for item in recovered] == [job.id]
        refreshed = session.get(Job, job.id)
        assert refreshed is not None
        assert refreshed.status == JobStatus.failed.value
        assert "stopped reporting progress" in (refreshed.error or "")
        log = session.exec(select(JobLog).where(JobLog.job_id == job.id)).one()
        assert log.level == "error"


def test_worker_start_is_atomic_and_stale_messages_cannot_revive_job() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        job = Job(
            type=JobType.crm_sheet_filter.value,
            title="Queued CRM run",
            parameters={"source_file_id": "abc"},
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        job_id = str(job.id)

    with patch.object(tasks, "engine", engine):
        parameters = tasks.start_job(job_id)
        assert parameters == {"source_file_id": "abc"}

        with Session(engine) as session:
            refreshed = session.get(Job, job.id)
            assert refreshed is not None
            assert refreshed.status == JobStatus.running.value
            logs = list(session.exec(select(JobLog).where(JobLog.job_id == job.id)))
            assert [log.message for log in logs] == [
                "CRM worker accepted the job and started execution."
            ]

        with pytest.raises(Ignore):
            tasks.start_job(job_id)
