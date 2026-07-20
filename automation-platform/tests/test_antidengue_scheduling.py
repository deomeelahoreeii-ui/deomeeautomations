from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.models import (
    AntiDengueSchedule,
    AntiDengueScheduleEvent,
    AntiDengueScheduleExecution,
)
from antidengue_automation.scheduling import (
    advance_execution,
    append_milestone_once,
    combined_execution_logs,
    create_execution,
    ensure_due_executions,
    next_occurrence_after,
    occurrences_between,
    validate_recurrence,
)
from automation_api.main import app
from automation_core.database import get_session
from automation_core.models import Job, JobLog, JobStatus, JobType
from automation_core.worker_runtime import register_worker_runtime
from whatsapp_gateway.previews.compiler.capabilities import (
    PREVIEW_COMPILER_PROTOCOL, PREVIEW_COMPILER_QUEUE,
    compiler_build_id, compiler_capabilities, compiler_fingerprint,
)
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDispatchPreview,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
)


class FakeTask:
    id = "antidengue-schedule-task"


def seed_profile(session: Session) -> WhatsAppDispatchProfile:
    register_worker_runtime(
        session, worker_name="test-preview-worker",
        queues=[PREVIEW_COMPILER_QUEUE],
        protocols={"antidengue_preview": PREVIEW_COMPILER_PROTOCOL},
        capabilities=compiler_capabilities(),
        capability_fingerprint=compiler_fingerprint(),
        build_id=compiler_build_id(), database_fingerprint="test",
    )
    account = WhatsAppAccount(name="Primary", worker_key=f"worker-{uuid.uuid4().hex}")
    application = WhatsAppApplication(key=f"antidengue-{uuid.uuid4().hex}", name="AntiDengue")
    # validate_dispatch_profile requires the canonical key.
    application.key = "antidengue"
    session.add(account)
    session.add(application)
    session.flush()
    report = WhatsAppReportType(application_id=application.id, key="school_activity", name="School activity")
    audience = WhatsAppAudience(application_id=application.id, key="all", name="All routes")
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


def test_recurrence_supports_multiple_times_and_weekdays() -> None:
    spec = validate_recurrence(
        recurrence_type="weekly",
        run_date=None,
        weekdays=[0, 2],
        times=["09:00", "08:30", "09:00"],
        timezone="Asia/Karachi",
    )
    start = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)  # Monday
    end = start + timedelta(days=3)
    occurrences = occurrences_between(spec, start_utc=start, end_utc=end)
    local_labels = [item.astimezone(__import__("zoneinfo").ZoneInfo("Asia/Karachi")).strftime("%a %H:%M") for item in occurrences]
    assert local_labels == ["Mon 08:30", "Mon 09:00", "Wed 08:30", "Wed 09:00"]


def test_due_tick_creates_only_latest_missed_occurrence() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
    now = datetime(2026, 7, 20, 4, 10, tzinfo=UTC)  # 09:10 PKT
    with Session(engine) as session:
        profile = seed_profile(session)
        schedule = AntiDengueSchedule(
            name="Office hours",
            recurrence_type="daily",
            times=["08:55", "09:05"],
            timezone="Asia/Karachi",
            dispatch_profile_id=profile.id,
            missed_run_grace_minutes=20,
        )
        schedule.next_run_at = next_occurrence_after(schedule, now - timedelta(days=1))
        session.add(schedule)
        session.commit()
        created = ensure_due_executions(session, now)
        assert len(created) == 1
        assert created[0].scheduled_for.tzinfo is not None
        assert created[0].scheduled_for.utcoffset() == timedelta(0)
        assert created[0].scheduled_for.astimezone(__import__("zoneinfo").ZoneInfo("Asia/Karachi")).strftime("%H:%M") == "09:05"
        assert created[0].dispatch_policy == "preview_only"

        persisted = session.exec(
            select(AntiDengueScheduleExecution).where(
                AntiDengueScheduleExecution.id == created[0].id
            )
        ).one()
        assert persisted.scheduled_for.tzinfo is not None
        assert persisted.scheduled_for.astimezone(
            __import__("zoneinfo").ZoneInfo("Asia/Karachi")
        ).strftime("%H:%M") == "09:05"


def test_execution_persists_only_explicitly_selected_profiles() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        source = seed_profile(session)
        activity_report = WhatsAppReportType(
            application_id=source.application_id,
            key="hotspot_distance_activity",
            name="Hotspot distance",
        )
        session.add(activity_report)
        session.flush()
        activity = WhatsAppDispatchProfile(
            application_id=source.application_id,
            key="markaz-hotspot",
            name="Markaz hotspot",
            report_type_id=activity_report.id,
            audience_id=source.audience_id,
            account_id=source.account_id,
            recipient_channel="group",
            delivery_mode="groups",
            presentation_policy={"linked_source_profile_id": str(source.id)},
        )
        session.add(activity)
        session.commit()

        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime.now(UTC),
            trigger_type="manual_preview",
            dispatch_policy="preview_only",
            login_mode="auto",
            dispatch_profile_id=source.id,
            dispatch_profile_ids=[activity.id],
            created_by="test",
        )

        assert execution.dispatch_profile_id == activity.id
        assert execution.dispatch_profile_ids == [str(activity.id)]


def test_preview_only_execution_never_uses_direct_live_source_job() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
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
        source = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.succeeded.value,
            parameters={"dry_run": True},
            result={"summary": {"quality_gate": {"passed": True}}},
        )
        session.add(source)
        session.flush()
        preview = WhatsAppDispatchPreview(
            preview_key=f"preview-{uuid.uuid4()}",
            application_id=profile.application_id,
            source_job_id=source.id,
            dispatch_profile_id=profile.id,
            status="ready",
            profile_version=1,
            application_name="AntiDengue",
            report_type_name="School activity",
            audience_name="All routes",
            profile_name=profile.name,
            account_name="Primary",
            ready_count=3,
            delivery_count=3,
            content_sha256="a" * 64,
        )
        session.add(preview)
        session.flush()
        preview_job = Job(
            type=JobType.whatsapp_dispatch_preview.value,
            title="Preview",
            status=JobStatus.succeeded.value,
            parameters={"source_job_id": str(source.id)},
            result={"preview_id": str(preview.id)},
        )
        session.add(preview_job)
        session.flush()
        execution.source_job_id = source.id
        execution.preview_job_id = preview_job.id
        execution.status = "preview_queued"
        session.add(execution)
        session.commit()
        advance_execution(session, execution)
        session.refresh(execution)
        assert execution.status == "preview_ready"
        assert execution.send_job_id is None
        assert source.parameters["dry_run"] is True


def test_auto_send_is_blocked_when_preview_has_warnings() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        profile = seed_profile(session)
        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime.now(UTC),
            trigger_type="manual_send",
            dispatch_policy="auto_send_when_clean",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        source = Job(type=JobType.antidengue_report.value, title="Dry run", status="succeeded", parameters={"dry_run": True}, result={})
        session.add(source)
        session.flush()
        preview = WhatsAppDispatchPreview(
            preview_key=f"preview-{uuid.uuid4()}",
            application_id=profile.application_id,
            source_job_id=source.id,
            dispatch_profile_id=profile.id,
            status="ready",
            profile_version=1,
            application_name="AntiDengue",
            report_type_name="School activity",
            audience_name="All routes",
            profile_name=profile.name,
            account_name="Primary",
            ready_count=2,
            warning_count=1,
            delivery_count=2,
            content_sha256="b" * 64,
        )
        session.add(preview)
        session.flush()
        preview_job = Job(type=JobType.whatsapp_dispatch_preview.value, title="Preview", status="succeeded", parameters={}, result={"preview_id": str(preview.id)})
        session.add(preview_job)
        session.flush()
        execution.source_job_id = source.id
        execution.preview_job_id = preview_job.id
        execution.status = "preview_queued"
        session.add(execution)
        session.commit()
        advance_execution(session, execution)
        session.refresh(execution)
        assert execution.status == "blocked"
        assert "warning" in (execution.error or "").lower()


def test_auto_send_completes_when_daily_acknowledgements_were_already_sent() -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        profile = seed_profile(session)
        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime.now(UTC),
            trigger_type="manual_send",
            dispatch_policy="auto_send_when_clean",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        source = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status="succeeded",
            parameters={"dry_run": True},
            result={},
        )
        session.add(source)
        session.flush()
        preview = WhatsAppDispatchPreview(
            preview_key=f"preview-{uuid.uuid4()}",
            application_id=profile.application_id,
            source_job_id=source.id,
            dispatch_profile_id=profile.id,
            status="ready",
            profile_version=1,
            application_name="AntiDengue",
            report_type_name="School activity",
            audience_name="All routes",
            profile_name=profile.name,
            account_name="Primary",
            ready_count=0,
            warning_count=0,
            blocked_count=0,
            skipped_count=2,
            delivery_count=2,
            content_sha256="c" * 64,
        )
        session.add(preview)
        session.flush()
        preview_job = Job(
            type=JobType.whatsapp_dispatch_preview.value,
            title="Preview",
            status="succeeded",
            parameters={},
            result={"preview_id": str(preview.id)},
        )
        session.add(preview_job)
        session.flush()
        execution.source_job_id = source.id
        execution.preview_job_id = preview_job.id
        execution.status = "preview_queued"
        session.add(execution)
        session.commit()

        advance_execution(session, execution)
        session.refresh(execution)

        assert execution.status == "completed"
        assert execution.send_job_id is None
        assert execution.error is None
        assert execution.send_job_id is None


def test_schedule_api_and_manual_execution_always_queue_dry_run(monkeypatch) -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        profile_id = seed_profile(session).id

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            schedule = client.post(
                "/api/v1/antidengue/schedules",
                json={
                    "name": "Morning reports",
                    "recurrence_type": "daily",
                    "times": ["08:30", "09:00"],
                    "dispatch_profile_id": str(profile_id),
                    "dispatch_policy": "auto_send_when_clean",
                },
            )
            assert schedule.status_code == 201, schedule.text
            assert schedule.json()["next_run_at"]

            execution = client.post(
                "/api/v1/antidengue/executions",
                json={
                    "dispatch_profile_id": str(profile_id),
                    "dispatch_policy": "auto_send_when_clean",
                    "login_mode": "auto",
                },
            )
            assert execution.status_code == 202, execution.text
            body = execution.json()
            assert body["status"] == "dry_run_queued"
            with Session(engine) as session:
                persisted = session.get(AntiDengueScheduleExecution, uuid.UUID(body["id"]))
                source = session.get(Job, persisted.source_job_id)
                assert source.parameters["dry_run"] is True
                assert source.parameters["dispatch_policy"] == "auto_send_when_clean"

            legacy_live = client.post(
                "/api/v1/antidengue/runs",
                json={"dry_run": False, "login_mode": "auto"},
            )
            assert legacy_live.status_code == 410
            assert "Send now" in legacy_live.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_clean_auto_send_queues_and_completes_exact_frozen_dispatch(monkeypatch) -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        profile = seed_profile(session)
        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime.now(UTC),
            trigger_type="manual_send",
            dispatch_policy="auto_send_when_clean",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        source = Job(type=JobType.antidengue_report.value, title="Dry run", status="succeeded", parameters={"dry_run": True}, result={})
        session.add(source)
        session.flush()
        preview = WhatsAppDispatchPreview(
            preview_key=f"preview-{uuid.uuid4()}",
            application_id=profile.application_id,
            source_job_id=source.id,
            dispatch_profile_id=profile.id,
            status="ready",
            profile_version=1,
            application_name="AntiDengue",
            report_type_name="School activity",
            audience_name="All routes",
            profile_name=profile.name,
            account_name="Primary",
            ready_count=2,
            warning_count=0,
            blocked_count=0,
            delivery_count=2,
            content_sha256="c" * 64,
        )
        session.add(preview)
        session.flush()
        preview_job = Job(type=JobType.whatsapp_dispatch_preview.value, title="Preview", status="succeeded", parameters={}, result={"preview_id": str(preview.id)})
        session.add(preview_job)
        session.flush()
        execution.source_job_id = source.id
        execution.preview_job_id = preview_job.id
        execution.status = "preview_queued"
        session.add(execution)
        session.commit()

        async def fake_approve(preview_id, data, passed_session):
            assert preview_id == preview.id
            assert data.acknowledge_warnings is False
            send = Job(
                type=JobType.whatsapp_dispatch_send.value,
                title="Exact frozen send",
                status="queued",
                parameters={"preview_id": str(preview.id)},
            )
            passed_session.add(send)
            passed_session.commit()
            passed_session.refresh(send)
            return send

        monkeypatch.setattr("antidengue_automation.scheduling.approve_preview", fake_approve)
        advance_execution(session, execution)
        session.refresh(execution)
        assert execution.status == "dispatch_queued"
        assert execution.send_job_id is not None

        send_job = session.get(Job, execution.send_job_id)
        send_job.status = "succeeded"
        send_job.result = {"delivered": 2, "failed": 0}
        session.add(send_job)
        session.commit()
        advance_execution(session, execution)
        session.refresh(execution)
        assert execution.status == "completed"
        assert execution.dispatch_summary == {"delivered": 2, "failed": 0}
        sent_events = session.exec(
            select(AntiDengueScheduleEvent).where(
                AntiDengueScheduleEvent.execution_id == execution.id,
                AntiDengueScheduleEvent.event_type == "antidengue.messages.sent",
            )
        ).all()
        assert len(sent_events) == 1
        advance_execution(session, execution)
        assert len(session.exec(
            select(AntiDengueScheduleEvent).where(
                AntiDengueScheduleEvent.execution_id == execution.id,
                AntiDengueScheduleEvent.event_type == "antidengue.messages.sent",
            )
        ).all()) == 1


def test_execution_milestones_are_durable_and_emitted_once() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
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
        source = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.running.value,
            parameters={"dry_run": True},
        )
        session.add(source)
        session.flush()
        execution.source_job_id = source.id
        execution.status = "dry_run_queued"
        session.add(execution)
        session.commit()

        advance_execution(session, execution)
        append_milestone_once(
            session,
            execution,
            "antidengue.execution.started",
            "AntiDengue run started.",
        )
        append_milestone_once(
            session,
            execution,
            "antidengue.execution.started",
            "AntiDengue run started.",
        )
        source.status = JobStatus.succeeded.value
        source.result = {"artifact_count": 4, "summary": {"run_id": "run-1"}}
        session.add(source)
        session.commit()
        advance_execution(session, execution)

        events = session.exec(
            select(AntiDengueScheduleEvent).where(
                AntiDengueScheduleEvent.execution_id == execution.id,
                AntiDengueScheduleEvent.event_type.in_([
                    "antidengue.execution.started",
                    "antidengue.report.downloaded",
                ]),
            )
        ).all()
        assert [event.event_type for event in events].count("antidengue.execution.started") == 1
        assert [event.event_type for event in events].count("antidengue.report.downloaded") == 1
        downloaded = next(event for event in events if event.event_type == "antidengue.report.downloaded")
        assert downloaded.details["artifact_count"] == 4
        assert downloaded.details["notification"] is True

def test_combined_execution_logs_normalize_mixed_sqlite_timestamps_to_utc() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        profile = seed_profile(session)
        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime(2026, 7, 20, 4, 5, tzinfo=UTC),
            trigger_type="manual_preview",
            dispatch_policy="preview_only",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        # create_execution records an automatic orchestration event using the
        # real current clock. Use later relative timestamps so this regression
        # remains deterministic after 2026-07-20 04:06 UTC as well.
        base_time = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=1)
        source = Job(
            type=JobType.antidengue_report.value,
            title="Dry run",
            status=JobStatus.running.value,
            parameters={"dry_run": True},
        )
        session.add(source)
        session.flush()
        execution.source_job_id = source.id
        session.add(execution)
        session.add(
            AntiDengueScheduleEvent(
                execution_id=execution.id,
                event_type="queued",
                message="Orchestrator queued the run.",
                created_at=base_time,
            )
        )
        session.add(
            JobLog(
                job_id=source.id,
                level="info",
                message="Dry-run worker started.",
                created_at=(base_time + timedelta(minutes=1)).replace(tzinfo=None),
            )
        )
        session.commit()

        logs = combined_execution_logs(session, execution)
        assert logs[-1]["source"] == "dry_run"
        assert logs[-1]["message"] == "Dry-run worker started."
        assert all(entry["created_at"].tzinfo is not None for entry in logs)
        assert all(entry["created_at"].utcoffset() == timedelta(0) for entry in logs)
        assert [entry["created_at"] for entry in logs] == sorted(
            entry["created_at"] for entry in logs
        )
        assert logs[-1]["created_at"] == base_time + timedelta(minutes=1)
