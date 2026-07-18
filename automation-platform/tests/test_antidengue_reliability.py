from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import event
from sqlalchemy.dialects import postgresql
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
    equivalent_profile_ids_clause,
    recover_orphaned_executions,
)
from automation_api.main import app
from automation_core.database import get_session
from automation_core.job_service import add_job, claim_job_running
from automation_core.models import Job, JobStatus, JobType, TaskOutbox
from automation_core.worker_runtime import register_worker_runtime
from automation_core.task_outbox import publish_pending_tasks, stage_task
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDispatchPreview,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
)
from whatsapp_gateway.previews.maintenance import delete_preview_records
from whatsapp_gateway.dispatch.task_entrypoints import send_approved_preview_job
from whatsapp_gateway.previews.compiler.capabilities import (
    PREVIEW_COMPILER_PROTOCOL, PREVIEW_COMPILER_QUEUE,
    compiler_build_id, compiler_capabilities, compiler_fingerprint,
)


def seed_profile(session: Session) -> WhatsAppDispatchProfile:
    """Create the minimal canonical AntiDengue routing graph for this module."""
    register_worker_runtime(
        session, worker_name="test-preview-worker",
        queues=[PREVIEW_COMPILER_QUEUE],
        protocols={"antidengue_preview": PREVIEW_COMPILER_PROTOCOL},
        capabilities=compiler_capabilities(),
        capability_fingerprint=compiler_fingerprint(),
        build_id=compiler_build_id(), database_fingerprint="test",
    )
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


def foreign_key_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    SQLModel.metadata.create_all(engine)
    return engine


def test_postgresql_profile_selection_comparison_uses_jsonb_equality() -> None:
    clause = equivalent_profile_ids_clause("postgresql", [str(uuid.uuid4()), str(uuid.uuid4())])
    sql = str(clause.compile(dialect=postgresql.dialect()))
    assert " AS JSONB) = CAST(" in sql
    assert "dispatch_profile_ids" in sql


def test_deleting_unapproved_preview_preserves_execution_and_clears_link() -> None:
    engine = foreign_key_engine()
    with Session(engine) as session:
        profile = seed_profile(session)
        source_job = Job(
            type=JobType.antidengue_report.value,
            title="Preview source",
            status=JobStatus.succeeded.value,
            parameters={},
        )
        session.add(source_job)
        session.flush()
        preview = WhatsAppDispatchPreview(
            preview_key=f"AD-{uuid.uuid4()}",
            application_id=profile.application_id,
            source_job_id=source_job.id,
            dispatch_profile_id=profile.id,
            profile_version=profile.version,
            application_name="AntiDengue",
            report_type_name="School activity",
            audience_name="All routes",
            profile_name=profile.name,
            account_name="Primary",
        )
        session.add(preview)
        session.flush()
        execution = AntiDengueScheduleExecution(
            execution_key=f"manual:{uuid.uuid4()}",
            execution_code=f"ADS-{uuid.uuid4().hex[:12]}",
            trigger_type="manual_preview",
            scheduled_for=datetime.now(UTC),
            status="preview_ready",
            dispatch_policy="preview_only",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            dispatch_profile_ids=[str(profile.id)],
            preview_id=preview.id,
            preview_summary={"delivery_count": 0},
        )
        session.add(execution)
        session.commit()
        preview_id, execution_id = preview.id, execution.id

        delete_preview_records(session, preview)
        session.commit()

    with Session(engine) as session:
        retained = session.get(AntiDengueScheduleExecution, execution_id)
        assert session.get(WhatsAppDispatchPreview, preview_id) is None
        assert retained is not None
        assert retained.preview_id is None
        assert retained.preview_summary == {"delivery_count": 0}


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


def test_send_worker_snapshots_initial_dispatch_parameters_before_session_closes(monkeypatch) -> None:
    engine = memory_engine()
    approval_id = uuid.uuid4()
    with Session(engine) as session:
        job = add_job(
            session,
            job_type=JobType.whatsapp_dispatch_send.value,
            title="Initial frozen dispatch",
            parameters={"approval_id": str(approval_id)},
        )
        session.commit()
        job_id = job.id

    observed: dict[str, object] = {}

    async def fake_publish(passed_approval_id, passed_job_id):
        observed.update(approval_id=passed_approval_id, job_id=passed_job_id)
        return {"delivered": 1, "failed": 0}

    monkeypatch.setattr("whatsapp_gateway.dispatch.task_entrypoints.engine", engine)
    monkeypatch.setattr(
        "whatsapp_gateway.dispatch.task_entrypoints._publish_approved_deliveries",
        fake_publish,
    )
    result = send_approved_preview_job.run(str(job_id))

    assert result == {"delivered": 1, "failed": 0}
    assert observed == {"approval_id": approval_id, "job_id": str(job_id)}
    with Session(engine) as session:
        persisted = session.get(Job, job_id)
        assert persisted is not None and persisted.status == JobStatus.succeeded.value


def test_send_worker_snapshots_retry_delivery_ids_before_session_closes(monkeypatch) -> None:
    engine = memory_engine()
    approval_id = uuid.uuid4()
    delivery_id = uuid.uuid4()
    with Session(engine) as session:
        job = add_job(
            session,
            job_type=JobType.whatsapp_dispatch_send.value,
            title="Retry failed frozen delivery",
            parameters={
                "approval_id": str(approval_id),
                "retry_delivery_ids": [str(delivery_id)],
            },
        )
        session.commit()
        job_id = job.id

    observed: dict[str, object] = {}

    async def fake_publish_selected(passed_approval_id, passed_job_id, passed_delivery_ids):
        observed.update(
            approval_id=passed_approval_id,
            job_id=passed_job_id,
            delivery_ids=passed_delivery_ids,
        )
        return {"delivered": 1, "failed": 0}

    monkeypatch.setattr("whatsapp_gateway.dispatch.task_entrypoints.engine", engine)
    monkeypatch.setattr(
        "whatsapp_gateway.dispatch.task_entrypoints._publish_selected_approved_deliveries",
        fake_publish_selected,
    )
    result = send_approved_preview_job.run(str(job_id))

    assert result == {"delivered": 1, "failed": 0}
    assert observed == {
        "approval_id": approval_id,
        "job_id": str(job_id),
        "delivery_ids": [delivery_id],
    }
    with Session(engine) as session:
        persisted = session.get(Job, job_id)
        assert persisted is not None and persisted.status == JobStatus.succeeded.value


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


def test_scheduler_lock_connection_never_owns_work_transactions() -> None:
    source = Path(
        "packages/antidengue_automation/antidengue_automation/scheduler_service.py"
    ).read_text(encoding="utf-8")
    assert 'execution_options(isolation_level="AUTOCOMMIT")' in source
    assert "Session(bind=" not in source
    assert source.count("with Session(engine) as session:") >= 3


def test_dashboard_preserves_json_content_type_with_idempotency_header() -> None:
    source = Path("apps/web/src/pages/index.astro").read_text(encoding="utf-8")
    fetch_block = source[source.index("const response = await fetch"):source.index("const data = await response.json")]
    assert fetch_block.index("...init") < fetch_block.index('headers: { "Content-Type": "application/json"')
    assert '"Idempotency-Key": requestKey' in source


def test_recovery_script_delegates_to_canonical_equivalence_logic() -> None:
    source = Path("scripts/recover_antidengue_executions.py").read_text(encoding="utf-8")
    assert "recover_orphaned_executions(session)" in source
    assert "dispatch_profile_id" not in source
    assert "defaultdict" not in source
