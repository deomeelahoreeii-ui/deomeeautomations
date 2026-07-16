from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from whatsapp_gateway.inbound_media import (
    classify_attachment_metadata,
    detect_file_type,
    safe_filename,
)
from whatsapp_gateway.inbound_service import normalize_media_types


def test_classifies_supported_inbound_metadata() -> None:
    assert classify_attachment_metadata(
        media_kind="image", mime_type="image/jpeg", original_filename=None
    ) == "image"
    assert classify_attachment_metadata(
        media_kind="document",
        mime_type="application/pdf",
        original_filename="complaint.pdf",
    ) == "pdf"
    assert classify_attachment_metadata(
        media_kind="document",
        mime_type="application/octet-stream",
        original_filename="complaints.xlsx",
    ) == "spreadsheet"
    assert classify_attachment_metadata(
        media_kind="document", mime_type="application/zip", original_filename="notes.zip"
    ) is None


def test_detects_pdf_image_csv_and_xlsx_by_content(tmp_path: Path) -> None:
    pdf = tmp_path / "wrong.bin"
    pdf.write_bytes(b"%PDF-1.7\nexample")
    assert detect_file_type(pdf) == ("application/pdf", "pdf", ".pdf")

    image = tmp_path / "photo.dat"
    image.write_bytes(b"\x89PNG\r\n\x1a\n" + b"0" * 32)
    assert detect_file_type(image) == ("image/png", "image", ".png")

    csv_file = tmp_path / "report.dat"
    csv_file.write_text("complaint,status\n123,pending\n", encoding="utf-8")
    assert detect_file_type(csv_file) == ("text/csv", "spreadsheet", ".csv")

    workbook = tmp_path / "workbook.bin"
    with zipfile.ZipFile(workbook, "w") as archive:
        archive.writestr(
            "[Content_Types].xml",
            '<Types><Override ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/></Types>',
        )
        archive.writestr("xl/workbook.xml", "<workbook/>")
    assert detect_file_type(workbook) == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "spreadsheet",
        ".xlsx",
    )


def test_rejects_unsupported_content(tmp_path: Path) -> None:
    executable = tmp_path / "complaints.pdf.exe"
    executable.write_bytes(b"MZ" + b"0" * 64)
    with pytest.raises(ValueError, match="Unsupported or mismatched"):
        detect_file_type(
            executable,
            declared_mime="application/pdf",
            original_filename="complaints.pdf",
        )


def test_normalizes_media_types_and_safe_names() -> None:
    assert normalize_media_types(["pdf", "image", "pdf"]) == ["image", "pdf"]
    with pytest.raises(ValueError, match="Select at least one"):
        normalize_media_types([])
    with pytest.raises(ValueError, match="Unsupported"):
        normalize_media_types(["video"])
    assert safe_filename("../../CRM: complaints?.pdf") == "CRM_ complaints_.pdf"


def test_preview_matches_unreconciled_contact_by_phone_jid() -> None:
    from sqlalchemy.pool import StaticPool
    from sqlmodel import SQLModel, Session, create_engine

    import automation_core.models  # noqa: F401
    import master_data.models  # noqa: F401
    import whatsapp_gateway.models  # noqa: F401
    from automation_core.time import utcnow
    from whatsapp_gateway.inbound_service import build_preview
    from whatsapp_gateway.models import (
        WhatsAppAccount,
        WhatsAppDirectoryContact,
        WhatsAppInboundAttachment,
        WhatsAppInboundMessage,
    )

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
        session.flush()
        message = WhatsAppInboundMessage(
            account_id=account.id,
            worker_key="default",
            message_id="message-1",
            remote_jid=contact.phone_jid,
            sender_jid=contact.phone_jid,
            directory_contact_id=None,
            from_me=False,
            chat_scope="direct",
            message_timestamp=utcnow(),
            message_type="documentMessage",
            ingestion_source="live",
            payload_sha256="0" * 64,
            raw_payload={},
        )
        session.add(message)
        session.flush()
        session.add(
            WhatsAppInboundAttachment(
                message_id=message.id,
                media_kind="document",
                message_key="documentMessage",
                original_filename="crm-complaints.pdf",
                mime_type="application/pdf",
            )
        )
        session.commit()

        preview = build_preview(
            session,
            contact_id=contact.id,
            date_from=None,
            date_to=None,
            chat_scope="direct",
            media_types=["pdf"],
        )
        session.refresh(message)

    assert preview["files_found"] == 1
    assert preview["category_counts"] == {"pdf": 1}
    assert message.directory_contact_id == contact.id


def test_worker_upload_archives_supported_content(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from sqlalchemy.pool import StaticPool
    from sqlmodel import SQLModel, Session, create_engine

    import automation_core.models  # noqa: F401
    import master_data.models  # noqa: F401
    import whatsapp_gateway.models  # noqa: F401
    from automation_api.main import app
    from automation_core.config import Settings, get_settings
    from automation_core.database import get_session
    from automation_core.time import utcnow
    from whatsapp_gateway.models import (
        WhatsAppAccount,
        WhatsAppInboundAttachment,
        WhatsAppInboundMessage,
    )

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
        message = WhatsAppInboundMessage(
            account_id=account.id,
            worker_key="default",
            message_id="message-upload",
            remote_jid="923360249999@s.whatsapp.net",
            sender_jid="923360249999@s.whatsapp.net",
            from_me=False,
            chat_scope="direct",
            message_timestamp=utcnow(),
            message_type="documentMessage",
            ingestion_source="live",
            payload_sha256="1" * 64,
            raw_payload={},
        )
        session.add(message)
        session.flush()
        attachment = WhatsAppInboundAttachment(
            message_id=message.id,
            media_kind="document",
            message_key="documentMessage",
            original_filename="complaint.pdf",
            mime_type="application/pdf",
        )
        session.add(attachment)
        session.commit()
        attachment_id = attachment.id

    settings = Settings(
        artifact_root=tmp_path / "artifacts",
        whatsapp_inbound_ingest_token="test-secret",
        whatsapp_inbound_media_max_bytes=1024 * 1024,
    )

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_settings] = lambda: settings
    try:
        content = b"%PDF-1.7\nexample inbound complaint"
        import hashlib

        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/whatsapp/inbound/attachments/{attachment_id}/content",
                content=content,
                headers={
                    "x-whatsapp-worker-token": "test-secret",
                    "x-whatsapp-worker-id": "default",
                    "x-content-sha256": hashlib.sha256(content).hexdigest(),
                    "x-declared-mime-type": "application/pdf",
                },
            )
        assert response.status_code == 200, response.text
        assert response.json()["media_category"] == "pdf"
        with Session(engine) as session:
            stored = session.get(WhatsAppInboundAttachment, attachment_id)
            assert stored is not None
            assert stored.download_status == "archived"
            assert stored.detected_mime_type == "application/pdf"
            assert Path(stored.stored_path).read_bytes() == content
    finally:
        app.dependency_overrides.clear()


def test_worker_upload_rejects_another_account_worker(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient
    from sqlalchemy.pool import StaticPool
    from sqlmodel import SQLModel, Session, create_engine

    import automation_core.models  # noqa: F401
    import master_data.models  # noqa: F401
    import whatsapp_gateway.models  # noqa: F401
    from automation_api.main import app
    from automation_core.config import Settings, get_settings
    from automation_core.database import get_session
    from automation_core.time import utcnow
    from whatsapp_gateway.models import (
        WhatsAppAccount,
        WhatsAppInboundAttachment,
        WhatsAppInboundMessage,
    )

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
        message = WhatsAppInboundMessage(
            account_id=account.id,
            worker_key="default",
            message_id="message-account-bound",
            remote_jid="923360249999@s.whatsapp.net",
            sender_jid="923360249999@s.whatsapp.net",
            from_me=False,
            chat_scope="direct",
            message_timestamp=utcnow(),
            message_type="documentMessage",
            ingestion_source="live",
            payload_sha256="3" * 64,
            raw_payload={},
        )
        session.add(message)
        session.flush()
        attachment = WhatsAppInboundAttachment(
            message_id=message.id,
            media_kind="document",
            message_key="documentMessage",
            original_filename="complaint.pdf",
            mime_type="application/pdf",
        )
        session.add(attachment)
        session.commit()
        attachment_id = attachment.id

    settings = Settings(
        artifact_root=tmp_path / "artifacts",
        whatsapp_inbound_ingest_token="test-secret",
    )

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_settings] = lambda: settings
    try:
        import hashlib

        content = b"%PDF-1.7\nwrong worker"
        with TestClient(app) as client:
            response = client.post(
                f"/api/v1/whatsapp/inbound/attachments/{attachment_id}/content",
                content=content,
                headers={
                    "x-whatsapp-worker-token": "test-secret",
                    "x-whatsapp-worker-id": "secondary",
                    "x-content-sha256": hashlib.sha256(content).hexdigest(),
                },
            )
        assert response.status_code == 403
    finally:
        app.dependency_overrides.clear()


def test_packages_archived_files_with_manifests(tmp_path: Path) -> None:
    from sqlalchemy.pool import StaticPool
    from sqlmodel import SQLModel, Session, create_engine

    import automation_core.models  # noqa: F401
    import master_data.models  # noqa: F401
    import whatsapp_gateway.models  # noqa: F401
    from automation_core.models import Job, JobType
    from automation_core.time import utcnow
    from whatsapp_gateway.inbound_service import package_export
    from whatsapp_gateway.models import (
        WhatsAppAccount,
        WhatsAppDirectoryContact,
        WhatsAppInboundAttachment,
        WhatsAppInboundExportItem,
        WhatsAppInboundExportRun,
        WhatsAppInboundMessage,
    )

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    archived = tmp_path / "archived.pdf"
    archived.write_bytes(b"%PDF-1.7\narchived")
    import hashlib

    checksum = hashlib.sha256(archived.read_bytes()).hexdigest()
    with Session(engine) as session:
        account = WhatsAppAccount(name="Default", worker_key="default")
        session.add(account)
        session.flush()
        contact = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="faheem",
            phone_jid="923360249999@s.whatsapp.net",
            display_name="Faheem Bukhari CEO Office",
        )
        session.add(contact)
        session.flush()
        job = Job(type=JobType.whatsapp_inbound_export.value, title="Export")
        session.add(job)
        session.flush()
        run = WhatsAppInboundExportRun(
            job_id=job.id,
            account_id=account.id,
            contact_id=contact.id,
            contact_name=contact.display_name,
            media_types=["pdf"],
            files_matched=1,
            files_downloaded=1,
            total_bytes=archived.stat().st_size,
        )
        session.add(run)
        session.flush()
        message = WhatsAppInboundMessage(
            account_id=account.id,
            worker_key="default",
            message_id="message-package",
            remote_jid=contact.phone_jid,
            sender_jid=contact.phone_jid,
            directory_contact_id=contact.id,
            from_me=False,
            chat_scope="direct",
            message_timestamp=utcnow(),
            message_type="documentMessage",
            ingestion_source="live",
            payload_sha256="2" * 64,
            raw_payload={},
        )
        session.add(message)
        session.flush()
        attachment = WhatsAppInboundAttachment(
            message_id=message.id,
            media_kind="document",
            message_key="documentMessage",
            original_filename="CRM complaint.pdf",
            mime_type="application/pdf",
            detected_mime_type="application/pdf",
            media_category="pdf",
            safe_extension=".pdf",
            actual_size=archived.stat().st_size,
            actual_sha256=checksum,
            stored_path=str(archived),
            download_status="archived",
        )
        session.add(attachment)
        session.flush()
        session.add(
            WhatsAppInboundExportItem(
                export_run_id=run.id,
                attachment_id=attachment.id,
                media_category="pdf",
                status="ready",
            )
        )
        session.commit()

        result = package_export(session, run=run, output_root=tmp_path / "export")

    assert result["zip_path"].is_file()
    assert result["manifest_csv"].is_file()
    assert result["summary_json"].is_file()
    with zipfile.ZipFile(result["zip_path"]) as archive:
        names = archive.namelist()
    assert any(name.endswith("manifest.csv") for name in names)
    assert any("/pdfs/" in name and name.endswith(".pdf") for name in names)


def test_requests_contact_history_from_bound_worker(monkeypatch) -> None:
    import asyncio
    import json

    from sqlalchemy.pool import StaticPool
    from sqlmodel import SQLModel, Session, create_engine

    import automation_core.models  # noqa: F401
    import master_data.models  # noqa: F401
    import whatsapp_gateway.models  # noqa: F401
    from automation_core.config import Settings
    from whatsapp_gateway.inbound_api import (
        RequestInboundHistory,
        request_inbound_history,
    )
    from whatsapp_gateway.models import WhatsAppAccount, WhatsAppDirectoryContact

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

    requests = []

    class FakeMessage:
        data = json.dumps(
            {
                "accepted": True,
                "workerId": "default",
                "requestedCount": 50,
                "anchorMessageId": "message-1",
                "anchorTimestamp": "2026-07-15T22:23:03.000Z",
            }
        ).encode()

    class FakeClient:
        async def request(self, subject, payload, timeout):
            requests.append((subject, json.loads(payload), timeout))
            return FakeMessage()

        async def close(self):
            return None

    async def fake_connect(*_args, **_kwargs):
        return FakeClient()

    monkeypatch.setattr("whatsapp_gateway.inbound_api.nats.connect", fake_connect)
    settings = Settings(
        whatsapp_nats_url="nats://127.0.0.1:4222",
        whatsapp_inbound_history_subject="whatsapp.worker.inbound.history",
        whatsapp_inbound_history_timeout_seconds=15,
    )
    with Session(engine) as session:
        result = asyncio.run(
            request_inbound_history(
                RequestInboundHistory(contact_id=contact_id, count=50),
                session=session,
                settings=settings,
            )
        )

    assert result["accepted"] is True
    assert requests[0][0] == "whatsapp.worker.inbound.history.default"
    assert requests[0][1]["remoteJids"] == ["923360249999@s.whatsapp.net"]
    assert requests[0][1]["count"] == 50
