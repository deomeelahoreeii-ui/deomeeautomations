from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote, urlparse

import requests
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.object_storage import S3ObjectStorage
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchItem,
    WhatsAppInboundBatchEvent,
    WhatsAppInboundHistoryRequest,
    WhatsAppInboundMessage,
    WhatsAppInboundStoredObject,
)


class FakeNatsMessage:
    def __init__(self, payload: dict) -> None:
        self.data = json.dumps(payload).encode("utf-8")


class FakeNatsClient:
    def __init__(self) -> None:
        self.requests: list[dict] = []

    async def request(self, _subject, payload, timeout):
        request = json.loads(payload.decode("utf-8"))
        self.requests.append(request)
        if request.get("action") == "bridge_health":
            return FakeNatsMessage(
                {
                    "provider": "wwebjs",
                    "protocolVersion": 4,
                    "workerId": "default",
                    "status": "ready",
                    "ready": True,
                    "historyReady": True,
                    "capabilities": {"dateRange": True, "receivedOnly": True},
                }
            )
        if request.get("action") == "history_status":
            return FakeNatsMessage(
                {
                    "requestId": request["requestId"],
                    "workerId": "default",
                    "status": "syncing",
                    "messagesReceived": 1,
                    "attachmentsDiscovered": 1,
                }
            )
        return FakeNatsMessage(
            {
                "accepted": True,
                "requestId": request["requestId"],
                "operationId": f"wwebjs:{request['requestId']}",
                "workerId": "default",
                "remoteJid": request["platformRemoteJid"],
                "requestedCount": request["count"],
                "anchorMessageId": request.get("anchorMessageId"),
                "anchorTimestamp": request.get("beforeTimestamp"),
                "status": "accepted",
            }
        )

    async def close(self):
        return None


async def _async_value(value):
    return value


class FakeS3Session:
    def __init__(self) -> None:
        self.buckets: set[str] = set()
        self.objects: dict[tuple[str, str], dict] = {}
        self.put_count = 0

    @staticmethod
    def response(
        status: int, *, headers: dict[str, str] | None = None, body: bytes = b""
    ) -> requests.Response:
        response = requests.Response()
        response.status_code = status
        response.headers.update(headers or {})
        response._content = body
        return response

    def request(self, method, url, *, headers, data, timeout, verify):
        del timeout, verify
        parts = [unquote(part) for part in urlparse(url).path.split("/") if part]
        bucket = parts[0]
        key = "/".join(parts[1:]) if len(parts) > 1 else None
        if method == "HEAD" and key is None:
            return self.response(200 if bucket in self.buckets else 404)
        if method == "PUT" and key is None:
            self.buckets.add(bucket)
            return self.response(200)
        if method == "HEAD":
            item = self.objects.get((bucket, key))
            if item is None:
                return self.response(404)
            return self.response(200, headers=item["headers"])
        if method == "PUT":
            self.buckets.add(bucket)
            body = data if isinstance(data, bytes) else data.read()
            self.put_count += 1
            metadata = {
                name.lower(): value
                for name, value in headers.items()
                if name.lower().startswith("x-amz-meta-")
            }
            response_headers = {
                "content-length": str(len(body)),
                "content-type": headers.get("content-type", "application/octet-stream"),
                "etag": '"fake-etag"',
                **metadata,
            }
            self.objects[(bucket, key)] = {"body": body, "headers": response_headers}
            return self.response(200, headers={"etag": '"fake-etag"'})
        raise AssertionError(f"Unexpected S3 request {method} {url}")


def setup_app(tmp_path, monkeypatch):
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
        object_storage_enabled=False,
    )

    def session_override():
        with Session(engine) as session:
            yield session

    fake_nats = FakeNatsClient()
    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_settings] = lambda: settings
    monkeypatch.setattr(
        "whatsapp_gateway.inbound_api.nats.connect",
        lambda *_args, **_kwargs: _async_value(fake_nats),
    )
    return engine, contact_id, settings, fake_nats


def test_history_request_creates_a_trackable_batch_and_links_files(tmp_path, monkeypatch) -> None:
    engine, contact_id, _settings, fake_nats = setup_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            created = client.post(
                "/api/v1/whatsapp/inbound/history/request",
                json={"contact_id": str(contact_id), "count": 50},
            )
            assert created.status_code == 202, created.text
            history = created.json()
            assert history["batch_id"]
            assert fake_nats.requests[0]["batchId"] == history["batch_id"]

            event = {
                "workerId": "default",
                "batchId": history["batch_id"],
                "messageId": "batch-message-1",
                "remoteJid": "923360249999@s.whatsapp.net",
                "participantJid": None,
                "senderJid": "923360249999@s.whatsapp.net",
                "fromMe": False,
                "chatScope": "direct",
                "messageTimestamp": datetime.now(timezone.utc).isoformat(),
                "pushName": "Faheem",
                "text": None,
                "messageType": "document",
                "ingestionSource": "web_history",
                "payloadSha256": "a" * 64,
                "rawPayload": {},
                "attachment": {
                    "mediaKind": "document",
                    "messageKey": "document",
                    "originalFilename": "complaint.pdf",
                    "mimeType": "application/pdf",
                    "declaredSize": 12,
                },
            }
            ingested = client.post(
                "/api/v1/whatsapp/inbound/events",
                json=event,
                headers={"x-whatsapp-worker-token": "test-secret"},
            )
            assert ingested.status_code == 200, ingested.text

            batch_response = client.get(f"/api/v1/whatsapp/inbound/batches/{history['batch_id']}")
            assert batch_response.status_code == 200, batch_response.text
            batch = batch_response.json()
            assert batch["batch_code"].startswith("WAB-")
            assert batch["files_discovered"] == 1
            assert len(batch["items"]) == 1
            assert batch["items"][0]["original_filename"] == "complaint.pdf"
            assert batch["contact_name"] == "Faheem Bukhari"

            events_response = client.get(
                f"/api/v1/whatsapp/inbound/batches/{history['batch_id']}/events"
            )
            assert events_response.status_code == 200, events_response.text
            event_types = {item["event_type"] for item in events_response.json()["items"]}
            assert {
                "batch_created",
                "history_request_accepted",
                "file_discovered",
                "history_file_received",
            }.issubset(event_types)

        with Session(engine) as session:
            assert len(session.exec(select(WhatsAppInboundBatch)).all()) == 1
            assert len(session.exec(select(WhatsAppInboundBatchItem)).all()) == 1
            assert len(session.exec(select(WhatsAppInboundBatchEvent)).all()) >= 4
    finally:
        app.dependency_overrides.clear()


def test_batch_membership_enforces_date_range_and_received_only(tmp_path, monkeypatch) -> None:
    engine, contact_id, _settings, _fake_nats = setup_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            created = client.post(
                "/api/v1/whatsapp/inbound/history/request",
                json={
                    "contact_id": str(contact_id),
                    "date_from": "2026-07-10T00:00:00Z",
                    "date_to": "2026-07-13T00:00:00Z",
                },
            )
            assert created.status_code == 202, created.text
            batch_id = created.json()["batch_id"]

            def event(message_id: str, timestamp: str, *, from_me: bool) -> dict:
                return {
                    "workerId": "default",
                    "batchId": batch_id,
                    "messageId": message_id,
                    "remoteJid": "923360249999@s.whatsapp.net",
                    "participantJid": None,
                    "senderJid": "923360249999@s.whatsapp.net",
                    "fromMe": from_me,
                    "chatScope": "direct",
                    "messageTimestamp": timestamp,
                    "pushName": None if from_me else "Faheem",
                    "text": "CRM complaint",
                    "messageType": "document",
                    "ingestionSource": "web_history",
                    "payloadSha256": message_id[0] * 64,
                    "rawPayload": {},
                    "attachment": {
                        "mediaKind": "document",
                        "messageKey": "document",
                        "originalFilename": f"{message_id}.pdf",
                        "mimeType": "application/pdf",
                        "declaredSize": 12,
                    },
                }

            for payload in (
                event("outside-range", "2026-07-09T23:59:59Z", from_me=False),
                event("sent-by-me", "2026-07-11T12:00:00Z", from_me=True),
                event("received-in-range", "2026-07-12T12:00:00Z", from_me=False),
            ):
                response = client.post(
                    "/api/v1/whatsapp/inbound/events",
                    json=payload,
                    headers={"x-whatsapp-worker-token": "test-secret"},
                )
                assert response.status_code == 200, response.text

        with Session(engine) as session:
            items = session.exec(select(WhatsAppInboundBatchItem)).all()
            assert len(items) == 1
            message = session.get(WhatsAppInboundMessage, items[0].message_id)
            assert message is not None
            assert message.message_id == "received-in-range"
            assert message.from_me is False
            request = session.exec(select(WhatsAppInboundHistoryRequest)).one()
            assert request.messages_received == 1
            assert request.attachments_discovered == 1
            assert request.received_only is True
    finally:
        app.dependency_overrides.clear()


def test_content_addressed_s3_storage_is_verified_and_reused(tmp_path, monkeypatch) -> None:
    engine, contact_id, settings, _fake_nats = setup_app(tmp_path, monkeypatch)
    fake_s3 = FakeS3Session()
    settings.object_storage_enabled = True
    settings.object_storage_endpoint_url = "http://s3.test"
    settings.object_storage_access_key = ""
    settings.object_storage_secret_key = ""
    adapter = S3ObjectStorage(settings, session=fake_s3)
    monkeypatch.setattr(
        "whatsapp_gateway.inbound.media_upload.store_attachment_object",
        lambda session, attachment, message, source_path, settings: __import__(
            "whatsapp_gateway.inbound.batches", fromlist=["store_attachment_object"]
        ).store_attachment_object(
            session,
            attachment=attachment,
            message=message,
            source_path=source_path,
            settings=settings,
            storage=adapter,
        ),
    )
    try:
        with TestClient(app) as client:
            created = client.post(
                "/api/v1/whatsapp/inbound/history/request",
                json={"contact_id": str(contact_id), "count": 10},
            ).json()
            event = {
                "workerId": "default",
                "batchId": created["batch_id"],
                "messageId": "s3-message-1",
                "remoteJid": "923360249999@s.whatsapp.net",
                "participantJid": None,
                "senderJid": "923360249999@s.whatsapp.net",
                "fromMe": False,
                "chatScope": "direct",
                "messageTimestamp": "2026-07-17T00:00:00Z",
                "pushName": "Faheem",
                "text": None,
                "messageType": "document",
                "ingestionSource": "web_history",
                "payloadSha256": "b" * 64,
                "rawPayload": {},
                "attachment": {
                    "mediaKind": "document",
                    "messageKey": "document",
                    "originalFilename": "complaint.pdf",
                    "mimeType": "application/pdf",
                    "declaredSize": 18,
                },
            }
            accepted = client.post(
                "/api/v1/whatsapp/inbound/events",
                json=event,
                headers={"x-whatsapp-worker-token": "test-secret"},
            ).json()
            content = b"%PDF-1.4\ncontent\n"
            import hashlib

            sha = hashlib.sha256(content).hexdigest()
            headers = {
                "x-whatsapp-worker-token": "test-secret",
                "x-whatsapp-worker-id": "default",
                "x-content-sha256": sha,
                "x-declared-mime-type": "application/pdf",
            }
            first = client.post(
                f"/api/v1/whatsapp/inbound/attachments/{accepted['attachment_id']}/content",
                content=content,
                headers=headers,
            )
            assert first.status_code == 200, first.text
            assert first.json()["object_storage"]["stored"] is True
            assert first.json()["object_storage"]["reused"] is False
            second = client.post(
                f"/api/v1/whatsapp/inbound/attachments/{accepted['attachment_id']}/content",
                content=content,
                headers=headers,
            )
            assert second.status_code == 200, second.text
            assert second.json()["object_storage"]["reused"] is True
            assert fake_s3.put_count == 1

        with Session(engine) as session:
            attachment = session.get(
                WhatsAppInboundAttachment, uuid.UUID(accepted["attachment_id"])
            )
            assert attachment is not None
            assert attachment.storage_status == "stored"
            assert attachment.stored_object_id is not None
            objects = session.exec(select(WhatsAppInboundStoredObject)).all()
            assert len(objects) == 1
            assert objects[0].object_key.endswith(sha)
            item = session.exec(select(WhatsAppInboundBatchItem)).one()
            assert item.status == "already_stored"
            assert item.stored_object_id == objects[0].id
    finally:
        app.dependency_overrides.clear()
