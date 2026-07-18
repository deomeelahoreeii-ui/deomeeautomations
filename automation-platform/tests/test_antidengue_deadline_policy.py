from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.deadline_policy import (
    deadline_from_snapshot,
    normalize_deadline,
    resolve_deadline_policy,
    update_deadline_policy,
)
from antidengue_automation.models import AntiDengueSchedule
from antidengue_automation.scheduling import create_execution
from automation_api.main import app
from automation_core.database import get_session
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
)


def _engine():
    return create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )


def _profile(session: Session) -> WhatsAppDispatchProfile:
    suffix = uuid.uuid4().hex
    account = WhatsAppAccount(name="Primary", worker_key=f"worker-{suffix}")
    application = WhatsAppApplication(key="antidengue", name="AntiDengue")
    session.add_all([account, application])
    session.flush()
    report = WhatsAppReportType(
        application_id=application.id, key="school_activity", name="School activity"
    )
    audience = WhatsAppAudience(application_id=application.id, key=f"all-{suffix}", name="All")
    session.add_all([report, audience])
    session.flush()
    profile = WhatsAppDispatchProfile(
        application_id=application.id,
        key=f"profile-{suffix}",
        name="Deadline profile",
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


def test_deadline_validation_is_canonical() -> None:
    assert normalize_deadline("1:15 PM") == "13:15"
    assert normalize_deadline("09:05") == "09:05"
    with pytest.raises(ValueError, match="HH:MM"):
        normalize_deadline("25:00")
    with pytest.raises(ValueError, match="label"):
        deadline_from_snapshot({
            "time": "13:15", "label": "12:30 PM", "timezone": "Asia/Karachi",
            "policy_version": 1, "source": "global",
        })


def test_schedule_override_and_execution_deadline_are_frozen() -> None:
    engine = _engine()
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        profile = _profile(session)
        update_deadline_policy(
            session, submission_deadline="11:45", timezone="Asia/Karachi", updated_by="test"
        )
        schedule = AntiDengueSchedule(
            name="Custom deadline",
            dispatch_profile_id=profile.id,
            submission_deadline_override="13:15",
        )
        session.add(schedule)
        session.commit()
        resolved = resolve_deadline_policy(session, schedule=schedule)
        assert resolved.label == "1:15 PM"
        assert resolved.source == "schedule_override"

        execution = create_execution(
            session,
            schedule=schedule,
            scheduled_for=datetime.now(UTC),
            trigger_type="schedule_run_now",
            dispatch_policy="preview_only",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        frozen = execution.submission_deadline_label
        update_deadline_policy(
            session, submission_deadline="14:00", timezone="Asia/Karachi", updated_by="test"
        )
        session.refresh(execution)
        assert frozen == "1:15 PM"
        assert execution.submission_deadline_label == frozen
        assert execution.deadline_source == "schedule_override"


def test_deadline_policy_api_persists_and_rejects_invalid_values() -> None:
    engine = _engine()
    SQLModel.metadata.create_all(engine)

    def override_session():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = override_session
    try:
        with TestClient(app) as client:
            saved = client.put("/api/v1/antidengue/deadline-policy", json={
                "submission_deadline": "13:40",
                "timezone": "Asia/Karachi",
                "updated_by": "test-operator",
            })
            assert saved.status_code == 200
            assert saved.json()["submission_deadline_label"] == "1:40 PM"
            loaded = client.get("/api/v1/antidengue/deadline-policy")
            assert loaded.status_code == 200
            assert loaded.json()["submission_deadline"] == "13:40"
            invalid = client.put("/api/v1/antidengue/deadline-policy", json={
                "submission_deadline": "tomorrow",
                "timezone": "Asia/Karachi",
            })
            assert invalid.status_code == 422
    finally:
        app.dependency_overrides.clear()
