from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import func
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppInboundAttachment,
    WhatsAppInboundMessage,
)


def test_replayed_inbound_event_is_an_atomic_upsert(tmp_path) -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(
            WhatsAppAccount(
                name="Default",
                worker_key="default",
                health_subject="whatsapp.worker.health",
            )
        )
        session.commit()

    settings = Settings(
        artifact_root=tmp_path / "artifacts",
        whatsapp_inbound_ingest_token="test-secret",
    )

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_settings] = lambda: settings
    payload = {
        "workerId": "default",
        "messageId": "replayed-message-1",
        "remoteJid": "923360249999@s.whatsapp.net",
        "participantJid": None,
        "senderJid": "923360249999@s.whatsapp.net",
        "fromMe": False,
        "chatScope": "direct",
        "messageTimestamp": datetime.now(timezone.utc).isoformat(),
        "pushName": "Faheem",
        "text": None,
        "messageType": "documentMessage",
        "ingestionSource": "history_sync",
        "payloadSha256": "a" * 64,
        "rawPayload": {"key": {"id": "replayed-message-1"}},
        "attachment": {
            "mediaKind": "document",
            "messageKey": "documentMessage",
            "originalFilename": "complaints.pdf",
            "mimeType": "application/pdf",
            "declaredSize": 1234,
            "mediaSha256": None,
            "caption": "CRM complaints",
        },
    }

    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/v1/whatsapp/inbound/events",
                json=payload,
                headers={"x-whatsapp-worker-token": "test-secret"},
            )
            second = client.post(
                "/api/v1/whatsapp/inbound/events",
                json=payload,
                headers={"x-whatsapp-worker-token": "test-secret"},
            )

        assert first.status_code == 200, first.text
        assert first.json()["created"] is True
        assert second.status_code == 200, second.text
        assert second.json()["created"] is False
        assert second.json()["message_id"] == first.json()["message_id"]

        with Session(engine) as session:
            message_count = session.exec(
                select(func.count()).select_from(WhatsAppInboundMessage)
            ).one()
            attachment_count = session.exec(
                select(func.count()).select_from(WhatsAppInboundAttachment)
            ).one()
        assert message_count == 1
        assert attachment_count == 1
    finally:
        app.dependency_overrides.clear()
