from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from whatsapp_gateway.models import WhatsAppAccount, WhatsAppDirectoryContact


class FakeMessage:
    def __init__(self, payload: dict) -> None:
        self.data = json.dumps(payload).encode("utf-8")


class FakeBridge:
    requests: list[tuple[str, dict]] = []

    async def request(self, subject, payload, timeout):
        request = json.loads(payload.decode("utf-8"))
        self.requests.append((subject, request))
        if request["action"] == "bridge_health":
            return FakeMessage(
                {
                    "provider": "wwebjs",
                    "protocolVersion": 2,
                    "workerId": "default",
                    "status": "ready",
                    "ready": True,
                    "mode": "browser_url",
                    "historyReady": True,
                    "browserUrl": "http://127.0.0.1:9222",
                }
            )
        return FakeMessage(
            {
                "accepted": True,
                "provider": "wwebjs",
                "requestId": request["requestId"],
                "operationId": f"wwebjs:{request['requestId']}",
                "workerId": "default",
                "remoteJid": request["platformRemoteJid"],
                "requestedCount": request["count"],
                "status": "accepted",
            }
        )

    async def close(self):
        return None


async def _async_value(value):
    return value


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
        whatsapp_inbound_history_provider="wwebjs",
        whatsapp_web_history_subject="whatsapp.web.inbound.history",
        whatsapp_inbound_history_timeout_seconds=2,
    )

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_settings] = lambda: settings
    fake = FakeBridge()
    fake.requests = []
    monkeypatch.setattr(
        "whatsapp_gateway.inbound_api.nats.connect",
        lambda *_args, **_kwargs: _async_value(fake),
    )
    return contact_id, fake


def test_bridge_health_and_history_route_use_wwebjs_subject(tmp_path, monkeypatch):
    contact_id, fake = make_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            health = client.get(
                "/api/v1/whatsapp/inbound/history/bridge/status",
                params={"contact_id": str(contact_id)},
            )
            assert health.status_code == 200, health.text
            assert health.json()["ready"] is True
            assert health.json()["historyReady"] is True
            assert health.json()["mode"] == "browser_url"

            request = client.post(
                "/api/v1/whatsapp/inbound/history/request",
                json={"contact_id": str(contact_id), "count": 50},
            )
            assert request.status_code == 202, request.text
            assert request.json()["provider"] == "wwebjs"

        request_subject, payload = next(
            item for item in fake.requests if item[1]["action"] == "request_history"
        )
        assert request_subject == "whatsapp.web.inbound.history.default"
        assert payload["platformRemoteJid"] == "923360249999@s.whatsapp.net"
        assert payload["beforeTimestamp"] is None
    finally:
        app.dependency_overrides.clear()


def test_bridge_source_files_are_pinned_and_baileys_is_preserved() -> None:
    root = Path(__file__).resolve().parents[2]
    package = json.loads(
        (root / "whatsapp-web-history-bridge" / "package.json").read_text()
    )
    assert package["dependencies"]["whatsapp-web.js"] == "1.34.7"
    assert package["version"] == "1.1.0"
    config_source = (root / "whatsapp-web-history-bridge" / "lib" / "config.js").read_text()
    assert "protocolVersion: 2" in config_source
    assert 'process.env.WWEBJS_MODE || "browser_url"' in config_source
    assert (root / "whatsappbot" / "lib" / "inbound-history.js").is_file()
    assert (root / "whatsappbot" / "ARCHIVED_BAILEYS_HISTORY.md").is_file()
