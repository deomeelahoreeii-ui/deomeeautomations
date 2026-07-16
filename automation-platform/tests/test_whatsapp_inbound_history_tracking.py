from __future__ import annotations

import json
import uuid
from datetime import timedelta

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppInboundHistoryRequest,
)


class FakeNatsMessage:
    def __init__(self, payload: dict) -> None:
        self.data = json.dumps(payload).encode("utf-8")


class FakeNatsClient:
    async def request(self, _subject, payload, timeout):
        request = json.loads(payload.decode("utf-8"))
        return FakeNatsMessage(
            {
                "accepted": True,
                "requestId": request["requestId"],
                "operationId": "history-operation-1",
                "workerId": "default",
                "remoteJid": "923360249999@s.whatsapp.net",
                "requestedCount": request["count"],
                "anchorMessageId": "anchor-message",
                "anchorTimestamp": "2026-07-15T20:00:00.000Z",
            }
        )

    async def close(self):
        return None


def make_app(tmp_path, monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        account = WhatsAppAccount(name="Default", worker_key="default")
        session.add(account)
        session.flush()
        contact = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="faheem",
            phone_jid="923360249999@s.whatsapp.net",
            display_name="Faheem Bukhari",
        )
        session.add(contact)
        session.commit()
        contact_id = contact.id

    settings = Settings(
        artifact_root=tmp_path / "artifacts",
        whatsapp_inbound_ingest_token="test-secret",
        whatsapp_inbound_history_timeout_seconds=2,
    )

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_settings] = lambda: settings
    monkeypatch.setattr(
        "whatsapp_gateway.inbound_api.nats.connect",
        lambda *_args, **_kwargs: _async_value(FakeNatsClient()),
    )
    return engine, contact_id


async def _async_value(value):
    return value


def test_history_request_is_audited_and_duplicate_active_request_is_blocked(
    tmp_path, monkeypatch
) -> None:
    engine, contact_id = make_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/whatsapp/inbound/history/request",
                json={"contact_id": str(contact_id), "count": 50},
            )
            assert response.status_code == 202, response.text
            payload = response.json()
            assert payload["status"] == "accepted"
            assert payload["requested_count"] == 50
            assert payload["operation_id"] == "history-operation-1"

            duplicate = client.post(
                "/api/v1/whatsapp/inbound/history/request",
                json={"contact_id": str(contact_id), "count": 25},
            )
            assert duplicate.status_code == 409

            listing = client.get(
                "/api/v1/whatsapp/inbound/history/requests",
                params={"contact_id": str(contact_id)},
            )
            assert listing.status_code == 200
            assert len(listing.json()["items"]) == 1
    finally:
        app.dependency_overrides.clear()


def test_history_ingest_updates_progress_and_quiet_request_completes(
    tmp_path, monkeypatch
) -> None:
    engine, contact_id = make_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            created = client.post(
                "/api/v1/whatsapp/inbound/history/request",
                json={"contact_id": str(contact_id), "count": 50},
            )
            request_id = created.json()["id"]
            event = {
                "workerId": "default",
                "messageId": "history-message-1",
                "remoteJid": "923360249999@s.whatsapp.net",
                "participantJid": None,
                "senderJid": "923360249999@s.whatsapp.net",
                "fromMe": False,
                "chatScope": "direct",
                "messageTimestamp": "2026-07-15T19:00:00Z",
                "pushName": "Faheem",
                "text": None,
                "messageType": "documentMessage",
                "ingestionSource": "history_sync",
                "payloadSha256": "1" * 64,
                "rawPayload": {},
                "attachment": {
                    "mediaKind": "document",
                    "messageKey": "documentMessage",
                    "originalFilename": "complaints.pdf",
                    "mimeType": "application/pdf",
                    "declaredSize": 123,
                },
            }
            ingested = client.post(
                "/api/v1/whatsapp/inbound/events",
                json=event,
                headers={"x-whatsapp-worker-token": "test-secret"},
            )
            assert ingested.status_code == 200, ingested.text

            with Session(engine) as session:
                audit = session.get(WhatsAppInboundHistoryRequest, uuid.UUID(request_id))
                assert audit is not None
                assert audit.status == "syncing"
                assert audit.messages_received == 1
                assert audit.attachments_discovered == 1
                audit.last_activity_at = utcnow() - timedelta(seconds=10)
                session.add(audit)
                session.commit()

            status = client.get(
                f"/api/v1/whatsapp/inbound/history/requests/{request_id}"
            )
            assert status.status_code == 200
            assert status.json()["status"] == "succeeded"
            assert status.json()["active"] is False
    finally:
        app.dependency_overrides.clear()
