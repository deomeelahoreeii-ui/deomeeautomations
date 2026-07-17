from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.models import AntiDengueScheduleExecution
from antidengue_automation.scheduling import (
    cancel_execution,
    create_execution,
    recover_orphaned_executions,
)
from automation_api.main import app
from automation_core.database import get_session
from automation_core.job_service import add_job, claim_job_running
from automation_core.models import Job, JobStatus, JobType, TaskOutbox
from automation_core.task_outbox import publish_pending_tasks, stage_task
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
)


def seed_profile(session: Session) -> WhatsAppDispatchProfile:
    """Create the minimal canonical AntiDengue routing graph for this module."""
    account = WhatsAppAccount(name="Primary", worker_key=f"worker-{uuid.uuid4().hex}")
    application = WhatsAppApplication(key="antidengue", name="AntiDengue")
    session.add(account)
    session.add(application)
    session.flush()
    report = WhatsAppReportType(
        application_id=application.id,
        key="school_activity",
        name="School activity",
    )
    audience = WhatsAppAudience(
        application_id=application.id,
        key="all",
        name="All routes",
    )
    session.add(report)
    session.add(audience)
    session.flush()
    profile = WhatsAppDispatchProfile(
        application_id=application.id,
        key="default",
        name="Default AntiDengue",
        report_type_id=report.id,
        audience_id=audience.id,
        account_id=account.id,
        recipient_channel="group",
        delivery_mode="groups",
    )
    session.add(profile)
    session.commit()
    session.refresh(profile)
    return profile


def memory_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return engine


def test_outbox_publishes_only_after_job_commit(monkeypatch) -> None:
    engine = memory_engine()
    with Session(engine) as session:
        job = add_job(
            session,
            job_type=JobType.antidengue_report.value,
            title="Durable report",
            parameters={"dry_run": True},
        )
        stage_task(
            session,
            job=job,
            task_name="antidengue_automation.run_report",
            queue="antidengue",
            args=[str(job.id)],
        )
        session.commit()
        job_id = job.id

    observed: dict[str, object] = {}

    def fake_send_task(name, *, args, kwargs, queue, task_id):
        with Session(engine) as observer:
            observed["job"] = observer.get(Job, job_id)
            observed["outbox"] = observer.exec(select(TaskOutbox)).one()
        observed["name"] = name
        observed["task_id"] = task_id

    monkeypatch.setattr("automation_core.task_outbox.celery_app.send_task", fake_send_task)
    with Session(engine) as session:
        stats = publish_pending_tasks(session)
        row = session.exec(select(TaskOutbox)).one()
        assert stats["published"] == 1
        assert row.status == "published"
    assert observed["job"] is not None
    assert observed["outbox"] is not None
    assert observed["name"] == "antidengue_automation.run_report"


def test_duplicate_broker_delivery_claims_job_once() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        job = add_job(
            session,
            job_type=JobType.antidengue_report.value,
            title="One claim",
            parameters={},
        )
        session.commit()
        job_id = job.id
    with Session(engine) as first:
        assert claim_job_running(first, job_id) is not None
    with Session(engine) as second:
        assert claim_job_running(second, job_id) is None
        assert second.get(Job, job_id).status == JobStatus.running.value


def test_manual_api_reuses_equivalent_active_execution(monkeypatch) -> None:
    engine = memory_engine()
    with Session(engine) as session:
        profile_id = seed_profile(session).id

    def session_override():
        with Session(engine) as session:
            yield session

    monkeypatch.setattr(
        "antidengue_automation.api.publish_pending_tasks",
        lambda session, limit=10: {"published": 0, "failed": 0, "cancelled": 0},
    )
    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            payload = {
                "login_mode": "auto",
                "dispatch_policy": "preview_only",
                "dispatch_profile_id": str(profile_id),
                "created_by": "test",
            }
            first = client.post(
                "/api/v1/antidengue/executions",
                json=payload,
                headers={"Idempotency-Key": "same-click"},
            )
            second = client.post(
                "/api/v1/antidengue/executions",
                json=payload,
                headers={"Idempotency-Key": "second-click"},
            )
            assert first.status_code == 202
            assert second.status_code == 202
            assert first.json()["id"] == second.json()["id"]
            assert second.json()["reused_active_execution"] is True
        with Session(engine) as session:
            assert len(session.exec(select(AntiDengueScheduleExecution)).all()) == 1
    finally:
        app.dependency_overrides.clear()


def test_different_profile_selections_create_different_executions(monkeypatch) -> None:
    engine = memory_engine()
    with Session(engine) as session:
        first = seed_profile(session)
        second = WhatsAppDispatchProfile(
            application_id=first.application_id,
            key="secondary",
            name="Secondary AntiDengue",
            report_type_id=first.report_type_id,
            audience_id=first.audience_id,
            account_id=first.account_id,
            recipient_channel="group",
            delivery_mode="groups",
        )
        session.add(second)
        session.commit()
        first_id, second_id = first.id, second.id

    def session_override():
        with Session(engine) as session:
            yield session

    monkeypatch.setattr(
        "antidengue_automation.api.publish_pending_tasks",
        lambda session, limit=10: {"published": 0, "failed": 0, "cancelled": 0},
    )
    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            common = {"login_mode": "auto", "dispatch_policy": "preview_only", "created_by": "test"}
            first_response = client.post(
                "/api/v1/antidengue/executions",
                json={**common, "dispatch_profile_ids": [str(first_id)]},
            )
            second_response = client.post(
                "/api/v1/antidengue/executions",
                json={**common, "dispatch_profile_ids": [str(first_id), str(second_id)]},
            )
            assert first_response.status_code == 202
            assert second_response.status_code == 202
            assert first_response.json()["id"] != second_response.json()["id"]
            assert second_response.json()["dispatch_profile_ids"] == sorted([str(first_id), str(second_id)])
    finally:
        app.dependency_overrides.clear()


def test_outbox_retry_uses_one_stable_broker_task_id(monkeypatch) -> None:
    engine = memory_engine()
    with Session(engine) as session:
        job = add_job(session, job_type=JobType.antidengue_report.value, title="Retry", parameters={})
        row = stage_task(
            session, job=job, task_name="antidengue_automation.run_report",
            queue="antidengue", args=[str(job.id)],
        )
        session.commit()
        stable_task_id = row.task_id

    calls: list[str] = []
    def fake_send_task(name, *, args, kwargs, queue, task_id):
        calls.append(task_id)
        if len(calls) == 1:
            raise RuntimeError("broker unavailable")

    monkeypatch.setattr("automation_core.task_outbox.celery_app.send_task", fake_send_task)
    with Session(engine) as session:
        assert publish_pending_tasks(session)["failed"] == 1
        row = session.exec(select(TaskOutbox)).one()
        row.available_at = datetime.now(UTC)
        session.add(row); session.commit()
        assert publish_pending_tasks(session)["published"] == 1
        session.refresh(row)
        assert row.attempts == 2
        assert row.task_id == stable_task_id
    assert calls == [stable_task_id, stable_task_id]


def test_cancel_execution_cancels_job_and_outbox(monkeypatch) -> None:
    engine = memory_engine()
    monkeypatch.setattr("automation_core.celery_app.celery_app.control.revoke", lambda *args, **kwargs: None)
    with Session(engine) as session:
        profile = seed_profile(session)
        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime.now(UTC),
            trigger_type="manual_preview",
            dispatch_policy="preview_only",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        job = add_job(
            session,
            job_type=JobType.antidengue_report.value,
            title="Queued",
            parameters={},
        )
        stage_task(
            session,
            job=job,
            task_name="antidengue_automation.run_report",
            queue="antidengue",
            args=[str(job.id)],
        )
        execution.source_job_id = job.id
        execution.status = "dry_run_queued"
        session.add(execution)
        session.commit()
        cancel_execution(session, execution, reason="Test cancellation")
        assert execution.status == "cancelled"
        assert session.get(Job, job.id).status == JobStatus.cancelled.value
        assert session.exec(select(TaskOutbox)).one().status == "cancelled"


def test_recovery_fails_execution_with_missing_job() -> None:
    engine = memory_engine()
    with Session(engine) as session:
        profile = seed_profile(session)
        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime.now(UTC),
            trigger_type="manual_preview",
            dispatch_policy="preview_only",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        execution.source_job_id = uuid.uuid4()
        execution.status = "dry_run_queued"
        session.add(execution)
        session.commit()
        stats = recover_orphaned_executions(session)
        session.refresh(execution)
        assert stats["failed"] == 1
        assert execution.status == "failed"
        assert "Missing persisted job" in (execution.error or "")


def test_dashboard_uses_sse_and_bounded_idle_refresh() -> None:
    source = Path("apps/web/src/pages/index.astro").read_text(encoding="utf-8")
    history = Path("apps/web/src/pages/antidengue/run-history.astro").read_text(encoding="utf-8")
    for text in (source, history):
        assert "new EventSource" in text
        assert "/events`" in text
        assert "2500" not in text
        assert "60000" in text
    assert '"Idempotency-Key": requestKey' in source
    assert 'id="cancel-execution"' in source
