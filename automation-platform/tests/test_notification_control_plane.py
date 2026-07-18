from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import antidengue_automation.models  # noqa: F401
import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from antidengue_automation.models import AntiDengueScheduleEvent
from automation_api.notification_api import (
    NotificationTestInput,
    notification_deliveries,
    notification_overview,
    send_test_notification,
)
from automation_core.config import Settings


def memory_session() -> Session:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def settings() -> Settings:
    return Settings(
        database_url="sqlite://",
        ntfy_enabled=True,
        ntfy_publish_url="http://ntfy:80",
        ntfy_public_base_url="http://localhost:2586",
        ntfy_exposure_mode="local",
        ntfy_token="secret",
        antidengue_ntfy_enabled=True,
        antidengue_ntfy_topic="automation-antidengue",
    )


def seed_delivery(session: Session, *, status: str, event_type: str) -> AntiDengueScheduleEvent:
    event = AntiDengueScheduleEvent(
        execution_id=uuid.uuid4(),
        event_type=event_type,
        message=f"{event_type} message",
        details={
            "execution_code": "AD-TEST",
            "notification": True,
            "ntfy_delivery": {
                "status": status,
                "attempts": 1,
                **({"sent_at": str(datetime.now(UTC))} if status == "sent" else {}),
            },
        },
    )
    session.add(event)
    session.commit()
    return event


def test_notification_overview_aggregates_health_channels_and_deliveries(monkeypatch) -> None:
    with memory_session() as session:
        seed_delivery(session, status="sent", event_type="antidengue.execution.started")
        seed_delivery(session, status="retrying", event_type="antidengue.report.downloaded")
        monkeypatch.setattr(
            "automation_api.notification_api.ntfy_health",
            lambda current: {
                "enabled": True,
                "reachable": True,
                "exposure_mode": current.ntfy_exposure_mode,
                "public_base_url": current.ntfy_public_base_url,
            },
        )
        result = notification_overview(settings=settings(), session=session)

    assert result["transport"]["reachable"] is True
    assert result["transport"]["token_configured"] is True
    assert result["channels"][0]["topic"] == "automation-antidengue"
    assert result["deliveries"]["statuses"] == {"retrying": 1, "sent": 1}
    assert result["deliveries"]["last_sent_at"]


def test_notification_deliveries_filters_and_paginates() -> None:
    with memory_session() as session:
        seed_delivery(session, status="sent", event_type="antidengue.execution.started")
        seed_delivery(session, status="retrying", event_type="antidengue.report.downloaded")
        result = notification_deliveries(status="retrying", page=1, page_size=25, session=session)

    assert result["total"] == 1
    assert result["items"][0]["status"] == "retrying"
    assert result["items"][0]["execution_code"] == "AD-TEST"


def test_send_test_notification_uses_registered_topic_and_token(monkeypatch) -> None:
    calls = []

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"id": "ntfy-message", "topic": "automation-antidengue"}

    def fake_post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr("automation_api.notification_api.requests.post", fake_post)
    result = send_test_notification(
        NotificationTestInput(message="Control plane test"),
        settings=settings(),
    )
    assert result == {
        "sent": True,
        "id": "ntfy-message",
        "topic": "automation-antidengue",
        "channel": "antidengue",
    }
    assert calls[0][0] == "http://ntfy:80/automation-antidengue"
    assert calls[0][1]["headers"]["Authorization"] == "Bearer secret"


def test_notification_control_plane_ui_contract() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    layout = (root / "apps/web/src/layouts/AppLayout.astro").read_text(encoding="utf-8")
    page = (root / "apps/web/src/pages/notifications/index.astro").read_text(encoding="utf-8")
    actions = (root / "apps/web/src/actions/index.ts").read_text(encoding="utf-8")
    assert 'href="/notifications/"' in layout
    assert 'active="notifications"' in page
    assert "actions.notifications.overview" in page
    assert "actions.notifications.deliveries" in page
    assert "actions.notifications.sendTest" in page
    assert 'api("/api/v1/notifications/overview")' in actions
