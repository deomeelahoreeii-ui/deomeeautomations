from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.models import AntiDengueScheduleEvent
from antidengue_automation.notifications import publish_pending_ntfy
from antidengue_automation.scheduling import create_execution
from test_antidengue_scheduling import seed_profile


def test_ntfy_delivery_is_retry_safe_and_does_not_republish(monkeypatch) -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(engine)
    settings = SimpleNamespace(
        antidengue_ntfy_enabled=True,
        antidengue_ntfy_base_url="https://ntfy.example",
        antidengue_ntfy_topic="operations",
        antidengue_ntfy_token="secret",
        antidengue_ntfy_timeout_seconds=2.0,
    )
    monkeypatch.setattr("antidengue_automation.notifications.get_settings", lambda: settings)
    calls = []

    class Response:
        def raise_for_status(self):
            return None

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr("antidengue_automation.notifications.requests.post", fake_post)
    with Session(engine) as session:
        profile = seed_profile(session)
        execution = create_execution(
            session,
            schedule=None,
            scheduled_for=datetime.now(UTC),
            trigger_type="scheduled",
            dispatch_policy="preview_only",
            login_mode="auto",
            dispatch_profile_id=profile.id,
            created_by="test",
        )
        event = AntiDengueScheduleEvent(
            execution_id=execution.id,
            event_type="antidengue.execution.started",
            message="Run started.",
            details={"notification": True, "ntfy_delivery": {"status": "pending", "attempts": 0}},
        )
        session.add(event)
        session.commit()

        assert publish_pending_ntfy(session) == {"sent": 1, "failed": 0}
        assert publish_pending_ntfy(session) == {"sent": 0, "failed": 0}
        assert len(calls) == 1
        assert calls[0][0] == "https://ntfy.example/operations"
        assert calls[0][1]["headers"]["Authorization"] == "Bearer secret"
        session.refresh(event)
        assert event.details["ntfy_delivery"]["status"] == "sent"
