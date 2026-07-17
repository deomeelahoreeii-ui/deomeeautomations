from __future__ import annotations

import uuid
from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.models import ComplaintCase
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchItem,
    WhatsAppInboundMessage,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


class FakeTask:
    id = "task-4c"


def setup_processing_app(tmp_path, monkeypatch):
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    source = tmp_path / "104-6609317.pdf"
    source.write_bytes(b"%PDF-1.4\nplaceholder")
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
        message = WhatsAppInboundMessage(
            account_id=account.id,
            worker_key="default",
            message_id="processing-message-1",
            remote_jid=contact.phone_jid,
            sender_jid=contact.phone_jid,
            directory_contact_id=contact.id,
            from_me=False,
            chat_scope="direct",
            message_timestamp=datetime.now(timezone.utc),
            push_name="Faheem Bukhari CEO Office",
            text_content="CRM complaint",
            message_type="document",
            ingestion_source="web_history",
            payload_sha256="a" * 64,
            raw_payload={},
        )
        session.add(message)
        session.flush()
        attachment = WhatsAppInboundAttachment(
            message_id=message.id,
            media_kind="document",
            message_key="document",
            original_filename=source.name,
            mime_type="application/pdf",
            detected_mime_type="application/pdf",
            media_category="pdf",
            stored_path=str(source),
            actual_size=source.stat().st_size,
            actual_sha256="b" * 64,
            storage_status="local_only",
            download_status="archived",
        )
        session.add(attachment)
        session.flush()
        batch = WhatsAppInboundBatch(
            batch_code="WAB-TEST-4C",
            account_id=account.id,
            contact_id=contact.id,
            worker_key="default",
            provider="wwebjs",
            requested_count=1,
            remote_jid=contact.phone_jid,
            status="completed",
            messages_discovered=1,
            files_discovered=1,
            files_stored=0,
            files_reused=0,
            total_bytes=source.stat().st_size,
            storage_backend="local",
        )
        session.add(batch)
        session.flush()
        batch_item = WhatsAppInboundBatchItem(
            batch_id=batch.id,
            attachment_id=attachment.id,
            message_id=message.id,
            status="storage_pending",
            original_filename=source.name,
            message_timestamp=message.message_timestamp,
            mime_type=attachment.mime_type,
            sha256=attachment.actual_sha256,
            size_bytes=attachment.actual_size,
        )
        session.add(batch_item)
        session.commit()
        batch_id = batch.id

    settings = Settings(
        artifact_root=tmp_path / "artifacts",
        object_storage_enabled=False,
        paperless_url="",
    )

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    app.dependency_overrides[get_settings] = lambda: settings
    monkeypatch.setattr(
        "whatsapp_gateway.inbound.processing_api.process_inbound_batch.delay",
        lambda _run_id: FakeTask(),
    )
    return engine, batch_id


def test_processing_run_creation_and_manual_review(tmp_path, monkeypatch) -> None:
    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": True},
            )
            assert response.status_code == 202, response.text
            created = response.json()
            run = created["processing_run"]
            assert run["run_code"].startswith("WAP-")
            assert run["contact_name"] == "Faheem Bukhari CEO Office"
            assert created["task_id"] == "task-4c"

            captured_runs = client.get(
                "/api/v1/whatsapp/inbound/batches", params={"limit": 20}
            )
            assert captured_runs.status_code == 200, captured_runs.text
            captured = next(
                item
                for item in captured_runs.json()["items"]
                if item["id"] == str(batch_id)
            )
            assert captured["processing_run_id"] == run["id"]
            assert captured["processing_status"] == "queued"

            detail = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}"
            )
            assert detail.status_code == 200, detail.text
            item = detail.json()["items"][0]
            reviewed = client.post(
                f"/api/v1/whatsapp/inbound/processing-items/{item['id']}/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "category": "crm_complaint",
                    "complaint_number": "104-6609317",
                    "note": "Confirmed from complaint form.",
                },
            )
            assert reviewed.status_code == 200, reviewed.text
            body = reviewed.json()
            assert body["review_status"] == "approved"
            assert body["status"] == "approved"
            assert body["detected_complaint_number"] == "104-6609317"

            group_review = client.post(
                f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}/complaint-groups/104-6609317/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "note": "Approved as one complaint group.",
                },
            )
            assert group_review.status_code == 200, group_review.text
            assert group_review.json()["case_state"] == "fresh"
            assert group_review.json()["case_id"]

            grouped = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}"
            ).json()["complaint_groups"]
            assert len(grouped) == 1
            assert grouped[0]["complaint_number"] == "104-6609317"
            assert grouped[0]["item_count"] == 1
            assert grouped[0]["categories"] == {"crm_complaint": 1}
            assert grouped[0]["approved_items"] == 1
            assert grouped[0]["review_bucket"] == "approved"

            events = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}/events"
            )
            assert events.status_code == 200
            event_types = {row["event_type"] for row in events.json()["items"]}
            assert {"processing_run_created", "review_decision_updated"}.issubset(event_types)

        with Session(engine) as session:
            assert len(session.exec(select(WhatsAppInboundProcessingRun)).all()) == 1
            persisted = session.exec(select(WhatsAppInboundProcessingItem)).one()
            assert persisted.reviewed_by == "Ahmad"
    finally:
        app.dependency_overrides.clear()


def test_batch_approval_only_accepts_explicit_ready_groups(tmp_path, monkeypatch) -> None:
    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            created = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": True},
            ).json()["processing_run"]
            with Session(engine) as session:
                item = session.exec(select(WhatsAppInboundProcessingItem)).one()
                item.status = "eligible"
                item.primary_category = "crm_complaint"
                item.detected_complaint_number = "104-6609317"
                item.confidence = 1.0
                item.extracted_text = """Complaint No: 104-6609317
Person Name: Fatima Khan
Mobile No: 03001234567
Complaint District: LAHORE
Complaint Remarks: The complaint requires review.
"""
                item.extraction_method = "pdftotext+field"
                item.extracted_metadata_json = {"needs_review": False}
                item.paperless_category = "fresh"
                item.paperless_reason = "No Paperless match."
                session.add(item)
                session.commit()

            approved = client.post(
                f"/api/v1/whatsapp/inbound/processing-runs/{created['id']}/complaint-group-approvals",
                json={
                    "complaint_numbers": ["104-6609317", "104-6609317"],
                    "reviewed_by": "Ahmad",
                    "note": "Verified in the Ready queue.",
                },
            )

            assert approved.status_code == 200, approved.text
            assert approved.json()["approved_count"] == 1
            assert approved.json()["complaint_numbers"] == ["104-6609317"]

            stale = client.post(
                f"/api/v1/whatsapp/inbound/processing-runs/{created['id']}/complaint-group-approvals",
                json={"complaint_numbers": ["104-6609317"]},
            )
            assert stale.status_code == 409
            assert "no longer Ready" in stale.json()["detail"]

        with Session(engine) as session:
            case = session.exec(select(ComplaintCase)).one()
            assert case.state == "fresh"
            assert case.complainant_name == "Fatima Khan"
            assert case.district == "LAHORE"
    finally:
        app.dependency_overrides.clear()


def test_processing_worker_classifies_a_local_spreadsheet_without_paperless(tmp_path, monkeypatch) -> None:
    from openpyxl import Workbook
    from whatsapp_gateway.inbound.processing import create_processing_run
    from whatsapp_gateway.inbound.processing_tasks import process_inbound_batch
    from whatsapp_gateway.models import WhatsAppInboundProcessingEvent

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    source = tmp_path / "crm-register.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Complaint No", "Status"])
    sheet.append(["104-6609317", "Pending"])
    workbook.save(source)
    settings = Settings(
        artifact_root=tmp_path / "artifacts",
        object_storage_enabled=False,
        paperless_url="",
    )
    with Session(engine) as session:
        account = WhatsAppAccount(name="Default", worker_key="default")
        session.add(account)
        session.flush()
        contact = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key="faheem-worker",
            phone_jid="923360249999@s.whatsapp.net",
            display_name="Faheem Bukhari CEO Office",
        )
        session.add(contact)
        session.flush()
        message = WhatsAppInboundMessage(
            account_id=account.id,
            worker_key="default",
            message_id="worker-processing-message",
            remote_jid=contact.phone_jid,
            sender_jid=contact.phone_jid,
            directory_contact_id=contact.id,
            from_me=False,
            chat_scope="direct",
            message_timestamp=datetime.now(timezone.utc),
            push_name="Faheem",
            text_content="CRM register",
            message_type="document",
            ingestion_source="web_history",
            payload_sha256="c" * 64,
            raw_payload={},
        )
        session.add(message)
        session.flush()
        attachment = WhatsAppInboundAttachment(
            message_id=message.id,
            media_kind="document",
            message_key="document",
            original_filename=source.name,
            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            detected_mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            media_category="spreadsheet",
            stored_path=str(source),
            actual_size=source.stat().st_size,
            actual_sha256="d" * 64,
            storage_status="local_only",
            download_status="archived",
        )
        session.add(attachment)
        session.flush()
        batch = WhatsAppInboundBatch(
            batch_code="WAB-WORKER-4C",
            account_id=account.id,
            contact_id=contact.id,
            worker_key="default",
            provider="wwebjs",
            requested_count=1,
            remote_jid=contact.phone_jid,
            status="completed",
            messages_discovered=1,
            files_discovered=1,
            total_bytes=source.stat().st_size,
            storage_backend="local",
        )
        session.add(batch)
        session.flush()
        session.add(
            WhatsAppInboundBatchItem(
                batch_id=batch.id,
                attachment_id=attachment.id,
                message_id=message.id,
                status="storage_pending",
                original_filename=source.name,
                message_timestamp=message.message_timestamp,
                mime_type=attachment.mime_type,
                sha256=attachment.actual_sha256,
                size_bytes=attachment.actual_size,
            )
        )
        session.flush()
        run = create_processing_run(
            session,
            batch=batch,
            settings=settings,
            paperless_check=False,
        )
        session.commit()
        run_id = run.id

    monkeypatch.setattr("whatsapp_gateway.inbound.processing_tasks.engine", engine)
    monkeypatch.setattr(
        "whatsapp_gateway.inbound.processing_tasks.get_settings", lambda: settings
    )
    result = process_inbound_batch.run(str(run_id))
    assert result["status"] == "completed"
    with Session(engine) as session:
        run = session.get(WhatsAppInboundProcessingRun, run_id)
        assert run is not None
        assert run.supporting_documents == 1
        item = session.exec(
            select(WhatsAppInboundProcessingItem).where(
                WhatsAppInboundProcessingItem.run_id == run_id
            )
        ).one()
        assert item.primary_category == "crm_supporting_document"
        assert item.detected_complaint_number == "104-6609317"
        assert item.status == "needs_review"
        event_types = {
            event.event_type
            for event in session.exec(
                select(WhatsAppInboundProcessingEvent).where(
                    WhatsAppInboundProcessingEvent.run_id == run_id
                )
            ).all()
        }
        assert {"extraction_started", "item_classified", "processing_run_completed"}.issubset(event_types)
