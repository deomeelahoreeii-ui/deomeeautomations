from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.models import ComplaintCase, ComplaintDocument, ComplaintDocumentCaseLink
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


def add_repeat_batch(
    session: Session,
    *,
    tmp_path,
    batch_code: str,
    message_id: str,
    sha256: str,
) -> WhatsAppInboundBatch:
    account = session.exec(select(WhatsAppAccount)).one()
    contact = WhatsAppDirectoryContact(
        account_id=account.id,
        canonical_key=f"repeat-{message_id}",
        phone_jid="923044308144@s.whatsapp.net",
        display_name="Repeat evidence sender",
    )
    session.add(contact)
    session.flush()
    source = tmp_path / f"{message_id}-104-6609317.pdf"
    source.write_bytes(b"%PDF-1.4\nplaceholder")
    message = WhatsAppInboundMessage(
        account_id=account.id,
        worker_key="default",
        message_id=message_id,
        remote_jid=contact.phone_jid,
        sender_jid=contact.phone_jid,
        directory_contact_id=contact.id,
        from_me=False,
        chat_scope="direct",
        message_timestamp=datetime.now(timezone.utc),
        push_name=contact.display_name,
        text_content="Repeated CRM evidence",
        message_type="document",
        ingestion_source="web_history",
        payload_sha256=uuid.uuid4().hex * 2,
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
        actual_sha256=sha256,
        storage_status="local_only",
        download_status="archived",
    )
    session.add(attachment)
    session.flush()
    batch = WhatsAppInboundBatch(
        batch_code=batch_code,
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
            sha256=sha256,
            size_bytes=attachment.actual_size,
        )
    )
    session.flush()
    return batch


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
            assert created["reused"] is False

            reopened = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": True},
            )
            assert reopened.status_code == 202, reopened.text
            assert reopened.json()["processing_run"]["id"] == run["id"]
            assert reopened.json()["task_id"] is None
            assert reopened.json()["reused"] is True

            captured_runs = client.get("/api/v1/whatsapp/inbound/batches", params={"limit": 20})
            assert captured_runs.status_code == 200, captured_runs.text
            captured = next(
                item for item in captured_runs.json()["items"] if item["id"] == str(batch_id)
            )
            assert captured["processing_run_id"] == run["id"]
            assert captured["processing_status"] == "queued"

            detail = client.get(f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}")
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

            grouped = client.get(f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}").json()[
                "complaint_groups"
            ]
            assert len(grouped) == 1
            assert grouped[0]["complaint_number"] == "104-6609317"
            assert grouped[0]["item_count"] == 1
            assert grouped[0]["categories"] == {"crm_complaint": 1}
            assert grouped[0]["approved_items"] == 1
            assert grouped[0]["review_bucket"] == "approved"

            group_page = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}/complaint-groups",
                params={
                    "bucket": "approved",
                    "search": "6609317",
                    "minimum_confidence": 0,
                    "limit": 10,
                    "offset": 0,
                    "sort": "item_count",
                    "order": "desc",
                },
            )
            assert group_page.status_code == 200, group_page.text
            assert group_page.json()["total"] == 1
            assert group_page.json()["items"][0]["paperless_category"] == "not_checked"
            assert group_page.json()["items"][0]["paperless_document_ids"] == []
            assert group_page.json()["bucket_counts"]["approved"] == 1

            item_detail = client.get(f"/api/v1/whatsapp/inbound/processing-items/{item['id']}")
            assert item_detail.status_code == 200, item_detail.text
            assert item_detail.json()["filename"] == "104-6609317.pdf"

            events = client.get(f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}/events")
            assert events.status_code == 200
            event_types = {row["event_type"] for row in events.json()["items"]}
            assert {"processing_run_created", "review_decision_updated"}.issubset(event_types)

            with Session(engine) as session:
                completed_run = session.get(WhatsAppInboundProcessingRun, uuid.UUID(run["id"]))
                assert completed_run is not None
                completed_run.status = "completed"
                session.add(completed_run)
                session.commit()

            reopened_completed = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": True},
            )
            assert reopened_completed.status_code == 202, reopened_completed.text
            assert reopened_completed.json()["processing_run"]["id"] == run["id"]
            assert reopened_completed.json()["task_id"] is None
            assert reopened_completed.json()["reused"] is True

        with Session(engine) as session:
            assert len(session.exec(select(WhatsAppInboundProcessingRun)).all()) == 1
            persisted = session.exec(select(WhatsAppInboundProcessingItem)).one()
            assert persisted.reviewed_by == "Ahmad"
    finally:
        app.dependency_overrides.clear()


def test_duplicate_only_group_approval_completes_review_without_regressing_case(
    tmp_path, monkeypatch
) -> None:
    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            created = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": True},
            ).json()
            run_id = created["processing_run"]["id"]
            item_id = (
                created["processing_run"]["items"][0]["id"]
                if created["processing_run"].get("items")
                else None
            )
            if item_id is None:
                item_id = client.get(f"/api/v1/whatsapp/inbound/processing-runs/{run_id}").json()[
                    "items"
                ][0]["id"]

            with Session(engine) as session:
                item = session.get(WhatsAppInboundProcessingItem, uuid.UUID(item_id))
                assert item is not None
                item.status = "eligible"
                item.primary_category = "crm_complaint"
                item.detected_complaint_number = "104-6609317"
                item.confidence = 1.0
                item.extracted_text = "Complaint No 104-6609317"
                item.extraction_method = "pdftotext"
                item.paperless_category = "fresh"
                session.add(item)
                case = ComplaintCase(complaint_number="104-6609317", state="review_required")
                session.add(case)
                session.flush()
                canonical = ComplaintDocument(
                    complaint_case_id=case.id,
                    source_processing_item_id=uuid.uuid4(),
                    source_sha256="b" * 64,
                    original_filename="earlier-copy.pdf",
                    mime_type="application/pdf",
                    role="main_complaint",
                    review_state="proposed",
                )
                session.add(canonical)
                session.flush()
                session.add(
                    ComplaintDocumentCaseLink(
                        complaint_case_id=case.id,
                        complaint_document_id=canonical.id,
                        role="main_complaint",
                        review_state="accepted",
                    )
                )
                session.commit()
                case_id = case.id

            reviewed = client.post(
                f"/api/v1/whatsapp/inbound/processing-runs/{run_id}/complaint-groups/104-6609317/review",
                json={"decision": "approved", "reviewed_by": "Ahmad"},
            )
            assert reviewed.status_code == 200, reviewed.text
            assert reviewed.json()["case_state"] == "review_required"

            with Session(engine) as session:
                case = session.get(ComplaintCase, case_id)
                duplicate = session.exec(
                    select(ComplaintDocument).where(
                        ComplaintDocument.source_processing_item_id == uuid.UUID(item_id)
                    )
                ).one()
                item = session.get(WhatsAppInboundProcessingItem, uuid.UUID(item_id))
                assert case is not None and case.state == "review_required"
                assert duplicate.review_state == "duplicate"
                assert item is not None and item.review_status == "approved"
    finally:
        app.dependency_overrides.clear()


def test_manual_attachment_links_existing_case_and_preserves_role_and_state(
    tmp_path, monkeypatch
) -> None:
    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    try:
        with TestClient(app) as client:
            run = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": True},
            ).json()["processing_run"]
            detail = client.get(f"/api/v1/whatsapp/inbound/processing-runs/{run['id']}").json()
            item_id = detail["items"][0]["id"]

            with Session(engine) as session:
                case = ComplaintCase(
                    source_system="crm_portal",
                    complaint_number="104-6609317",
                    state="published",
                    complainant_name="Existing complainant",
                )
                session.add(case)
                session.commit()
                existing_case_id = case.id

            reviewed = client.post(
                f"/api/v1/whatsapp/inbound/processing-items/{item_id}/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "category": "crm_supporting_document",
                    "complaint_number": "104-6609317",
                    "note": "Verified as an attachment to the existing complaint.",
                },
            )

            assert reviewed.status_code == 200, reviewed.text
            resolution = reviewed.json()["case_resolution"]
            assert resolution == {
                "case_id": str(existing_case_id),
                "complaint_number": "104-6609317",
                "case_state": "published",
                "case_created": False,
                "document_role": "attachment",
            }

        with Session(engine) as session:
            case = session.get(ComplaintCase, existing_case_id)
            document = session.exec(select(ComplaintDocument)).one()
            link = session.exec(select(ComplaintDocumentCaseLink)).one()
            assert case is not None and case.state == "published"
            assert document.complaint_case_id == existing_case_id
            assert document.role == "attachment"
            assert document.review_state == "accepted"
            assert link.complaint_case_id == existing_case_id
            assert link.role == "attachment"
            assert link.review_state == "accepted"
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


def test_processing_worker_classifies_a_local_spreadsheet_without_paperless(
    tmp_path, monkeypatch
) -> None:
    from openpyxl import Workbook
    from whatsapp_gateway.inbound.processing import create_processing_run
    from whatsapp_gateway.inbound.processing_tasks import process_inbound_batch
    from whatsapp_gateway.models import WhatsAppInboundProcessingEvent
    from crm_domain.models import (
        ComplaintCase,
        CrmSpreadsheetIntakeBatch,
        CrmSpreadsheetIntakeRow,
    )

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
    sheet.append(["104-6609318", "Fresh"])
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
    monkeypatch.setattr("whatsapp_gateway.inbound.processing_tasks.get_settings", lambda: settings)
    result = process_inbound_batch.run(str(run_id))
    assert result["status"] == "completed"
    with Session(engine) as session:
        run = session.get(WhatsAppInboundProcessingRun, run_id)
        assert run is not None
        assert run.supporting_documents == 0
        assert run.non_crm == 1
        item = session.exec(
            select(WhatsAppInboundProcessingItem).where(
                WhatsAppInboundProcessingItem.run_id == run_id
            )
        ).one()
        assert item.primary_category == "spreadsheet"
        assert item.detected_complaint_number is None
        assert item.status == "extracted"
        assert session.exec(select(ComplaintCase)).all() == []
        spreadsheet_batch = session.exec(select(CrmSpreadsheetIntakeBatch)).one()
        assert spreadsheet_batch.total_rows == 2
        assert spreadsheet_batch.candidate_rows == 2
        spreadsheet_rows = list(
            session.exec(
                select(CrmSpreadsheetIntakeRow).order_by(
                    CrmSpreadsheetIntakeRow.row_number
                )
            ).all()
        )
        assert [row.complaint_number for row in spreadsheet_rows] == [
            "104-6609317",
            "104-6609318",
        ]
        assert all(row.status == "candidate" for row in spreadsheet_rows)
        assert all(row.complaint_case_id is None for row in spreadsheet_rows)

        # A row becomes a durable case only after an explicit row-level decision.
        from whatsapp_gateway.inbound.spreadsheet_intake import (
            persist_spreadsheet_paperless_reconciliation,
            review_spreadsheet_row,
        )

        persist_spreadsheet_paperless_reconciliation(
            session,
            run_id=run_id,
            paperless_requested=True,
            paperless_results={
                "104-6609317": SimpleNamespace(
                    category="submitted",
                    reason="Exact complaint number exists in Paperless.",
                    matched_document_ids=[731],
                    matched_statuses=["Submitted"],
                ),
                "104-6609318": SimpleNamespace(
                    category="fresh",
                    reason="No Paperless match.",
                    matched_document_ids=[],
                    matched_statuses=[],
                ),
            },
        )
        session.flush()
        assert spreadsheet_rows[0].status == "existing"
        assert spreadsheet_rows[0].paperless_document_ids == [731]
        assert spreadsheet_rows[1].status == "fresh"
        assert session.exec(select(ComplaintCase)).all() == []

        promoted = review_spreadsheet_row(
            session,
            row=spreadsheet_rows[0],
            decision="approved",
            reviewed_by="test-operator",
            note="Verified against the original workbook.",
        )
        review_spreadsheet_row(
            session,
            row=spreadsheet_rows[1],
            decision="rejected",
            reviewed_by="test-operator",
            note="Not a complaint candidate.",
        )
        session.commit()
        cases = list(session.exec(select(ComplaintCase)).all())
        assert len(cases) == 1
        assert promoted is not None
        assert promoted.complaint_number == "104-6609317"
        assert promoted.registry_status == "active"
        assert spreadsheet_rows[0].status == "approved"
        assert spreadsheet_rows[0].complaint_case_id == promoted.id
        assert spreadsheet_rows[1].status == "rejected"
        assert spreadsheet_rows[1].complaint_case_id is None
        with pytest.raises(ValueError, match="cannot be promoted"):
            review_spreadsheet_row(
                session,
                row=spreadsheet_rows[1],
                decision="approved",
                reviewed_by="test-operator",
                note=None,
            )
        event_types = {
            event.event_type
            for event in session.exec(
                select(WhatsAppInboundProcessingEvent).where(
                    WhatsAppInboundProcessingEvent.run_id == run_id
                )
            ).all()
        }
        assert {"extraction_started", "item_classified", "processing_run_completed"}.issubset(
            event_types
        )


def test_processing_worker_reuses_exact_approved_evidence_before_extraction(
    tmp_path, monkeypatch
) -> None:
    from whatsapp_gateway.inbound.processing import create_processing_run
    from whatsapp_gateway.inbound.processing_tasks import process_inbound_batch

    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    settings = Settings(
        artifact_root=tmp_path / "artifacts",
        object_storage_enabled=False,
        paperless_url="",
    )
    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": False},
            ).json()["processing_run"]
            source_item = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{first['id']}"
            ).json()["items"][0]
            approved = client.post(
                f"/api/v1/whatsapp/inbound/processing-items/{source_item['id']}/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "category": "crm_complaint",
                    "complaint_number": "104-6609317",
                },
            )
            assert approved.status_code == 200, approved.text

        with Session(engine) as session:
            repeat_batch = add_repeat_batch(
                session,
                tmp_path=tmp_path,
                batch_code="WAB-REPEAT-4C",
                message_id="processing-message-repeat",
                sha256="b" * 64,
            )
            repeat_run = create_processing_run(
                session,
                batch=repeat_batch,
                settings=settings,
                paperless_check=True,
            )
            session.commit()
            repeat_run_id = repeat_run.id

        monkeypatch.setattr("whatsapp_gateway.inbound.processing_tasks.engine", engine)
        monkeypatch.setattr(
            "whatsapp_gateway.inbound.processing_tasks.get_settings", lambda: settings
        )
        monkeypatch.setattr(
            "whatsapp_gateway.inbound.processing_tasks.extract_document",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("exact duplicates must skip extraction")
            ),
        )
        monkeypatch.setattr(
            "whatsapp_gateway.inbound.processing_tasks._write_derived_record",
            lambda **_kwargs: None,
        )

        result = process_inbound_batch.run(str(repeat_run_id))
        assert result["status"] == "completed"
        with Session(engine) as session:
            repeat_run = session.get(WhatsAppInboundProcessingRun, repeat_run_id)
            repeat_item = session.exec(
                select(WhatsAppInboundProcessingItem).where(
                    WhatsAppInboundProcessingItem.run_id == repeat_run_id
                )
            ).one()
            source_document = session.exec(
                select(ComplaintDocument).where(
                    ComplaintDocument.source_processing_item_id
                    == uuid.UUID(source_item["id"])
                )
            ).one()
            repeat_document = session.exec(
                select(ComplaintDocument).where(
                    ComplaintDocument.source_processing_item_id == repeat_item.id
                )
            ).one()
            assert repeat_run is not None
            assert repeat_run.content_duplicate_items == 1
            assert repeat_run.review_items == 0
            assert repeat_item.content_match_kind == "exact_reused"
            assert repeat_item.canonical_processing_item_id == uuid.UUID(source_item["id"])
            assert repeat_item.status == "approved"
            assert repeat_item.review_status == "approved"
            assert repeat_item.detected_complaint_number == "104-6609317"
            assert repeat_item.paperless_category == "not_applicable"
            assert repeat_document.review_state == "duplicate"
            assert repeat_document.duplicate_of_document_id == source_document.id
    finally:
        app.dependency_overrides.clear()


def test_exact_duplicate_with_conflicting_prior_roles_isolated_from_manual_queue(
    tmp_path, monkeypatch
) -> None:
    from whatsapp_gateway.inbound.content_duplicates import (
        resolve_exact_duplicate_before_extraction,
    )
    from whatsapp_gateway.inbound.processing import create_processing_run, recalculate_processing_run

    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    settings = Settings(object_storage_enabled=False, paperless_url="")
    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": False},
            ).json()["processing_run"]
            source_item_id = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{first['id']}"
            ).json()["items"][0]["id"]
            approved = client.post(
                f"/api/v1/whatsapp/inbound/processing-items/{source_item_id}/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "category": "crm_complaint",
                    "complaint_number": "104-6609317",
                },
            )
            assert approved.status_code == 200, approved.text

        with Session(engine) as session:
            source_document = session.exec(
                select(ComplaintDocument).where(
                    ComplaintDocument.source_processing_item_id
                    == uuid.UUID(source_item_id)
                )
            ).one()
            source_document.role = "unclassified"
            session.add(source_document)
            accepted_link = session.exec(
                select(ComplaintDocumentCaseLink).where(
                    ComplaintDocumentCaseLink.complaint_document_id
                    == source_document.id
                )
            ).one()
            session.add(
                ComplaintDocumentCaseLink(
                    complaint_case_id=accepted_link.complaint_case_id,
                    complaint_document_id=source_document.id,
                    role="attachment",
                    review_state="accepted",
                    source_locator="legacy-conflicting-role",
                )
            )
            repeat_batch = add_repeat_batch(
                session,
                tmp_path=tmp_path,
                batch_code="WAB-CONFLICT-4C",
                message_id="processing-message-conflict",
                sha256="b" * 64,
            )
            repeat_run = create_processing_run(
                session,
                batch=repeat_batch,
                settings=settings,
                paperless_check=False,
            )
            session.flush()
            repeat_item = session.exec(
                select(WhatsAppInboundProcessingItem).where(
                    WhatsAppInboundProcessingItem.run_id == repeat_run.id
                )
            ).one()
            assert resolve_exact_duplicate_before_extraction(session, item=repeat_item)
            recalculate_processing_run(session, repeat_run)
            session.flush()
            assert repeat_item.content_match_kind == "exact_conflict"
            assert repeat_item.status == "deferred"
            assert repeat_item.review_status == "deferred"
            assert repeat_item.detected_complaint_number == "104-6609317"
            assert repeat_run.content_duplicate_items == 1
            assert repeat_run.review_items == 0
            assert len(repeat_item.content_match_details_json["relationships"]) == 2
            repeat_item_id = repeat_item.id
            session.commit()

        with TestClient(app) as client:
            resolved = client.post(
                f"/api/v1/whatsapp/inbound/processing-items/{repeat_item_id}/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "category": "crm_supporting_document",
                    "complaint_number": "104-6609317",
                    "note": "The identical evidence is one supporting attachment.",
                },
            )
            assert resolved.status_code == 200, resolved.text
            assert resolved.json()["content_match_kind"] == "exact_reused"

        with Session(engine) as session:
            repeat_item = session.get(WhatsAppInboundProcessingItem, repeat_item_id)
            assert repeat_item is not None
            assert repeat_item.content_match_details_json["decision"] == "conflict_resolved"
            documents = list(
                session.exec(
                    select(ComplaintDocument).where(
                        ComplaintDocument.source_sha256 == "b" * 64
                    )
                ).all()
            )
            assert {document.role for document in documents} == {"attachment"}
            assert sum(document.review_state == "accepted" for document in documents) == 1
            assert sum(document.review_state == "duplicate" for document in documents) == 1
    finally:
        app.dependency_overrides.clear()


def test_repeated_pending_evidence_shares_and_then_inherits_one_review(
    tmp_path, monkeypatch
) -> None:
    from whatsapp_gateway.inbound.content_duplicates import (
        resolve_exact_duplicate_before_extraction,
    )
    from whatsapp_gateway.inbound.processing import create_processing_run, recalculate_processing_run

    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    settings = Settings(object_storage_enabled=False, paperless_url="")
    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": False},
            ).json()["processing_run"]
            source_item_id = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{first['id']}"
            ).json()["items"][0]["id"]

            with Session(engine) as session:
                source_item = session.get(
                    WhatsAppInboundProcessingItem, uuid.UUID(source_item_id)
                )
                assert source_item is not None
                source_item.primary_category = "possible_crm_complaint"
                source_item.status = "needs_review"
                session.add(source_item)
                repeat_batch = add_repeat_batch(
                    session,
                    tmp_path=tmp_path,
                    batch_code="WAB-PENDING-4C",
                    message_id="processing-message-pending",
                    sha256="b" * 64,
                )
                repeat_run = create_processing_run(
                    session,
                    batch=repeat_batch,
                    settings=settings,
                    paperless_check=False,
                )
                session.flush()
                repeat_item = session.exec(
                    select(WhatsAppInboundProcessingItem).where(
                        WhatsAppInboundProcessingItem.run_id == repeat_run.id
                    )
                ).one()
                assert resolve_exact_duplicate_before_extraction(
                    session, item=repeat_item
                )
                recalculate_processing_run(session, repeat_run)
                assert repeat_item.content_match_kind == "exact_pending"
                assert repeat_run.review_items == 0
                repeat_item_id = repeat_item.id
                repeat_run_id = repeat_run.id
                session.commit()

            reviewed = client.post(
                f"/api/v1/whatsapp/inbound/processing-items/{source_item_id}/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "category": "crm_supporting_document",
                    "complaint_number": "104-6609317",
                },
            )
            assert reviewed.status_code == 200, reviewed.text

        with Session(engine) as session:
            repeat_item = session.get(WhatsAppInboundProcessingItem, repeat_item_id)
            repeat_run = session.get(WhatsAppInboundProcessingRun, repeat_run_id)
            assert repeat_item is not None and repeat_run is not None
            assert repeat_item.content_match_kind == "exact_reused"
            assert repeat_item.review_status == "approved"
            assert repeat_item.status == "approved"
            assert repeat_item.detected_complaint_number == "104-6609317"
            assert repeat_run.review_items == 0
    finally:
        app.dependency_overrides.clear()


def test_exact_normalized_content_reuses_decision_only_for_same_complaint(
    tmp_path, monkeypatch
) -> None:
    from whatsapp_gateway.inbound.content_duplicates import (
        normalized_content_sha256,
        resolve_normalized_duplicate_after_extraction,
    )
    from whatsapp_gateway.inbound.processing import create_processing_run

    engine, batch_id = setup_processing_app(tmp_path, monkeypatch)
    settings = Settings(object_storage_enabled=False, paperless_url="")
    text = (
        "Complaint Number 104-6609317. Applicant details and complaint remarks. "
        "This deliberately long evidence body describes the same complaint document "
        "and contains enough stable text for a normalized content fingerprint. " * 3
    )
    try:
        with TestClient(app) as client:
            first = client.post(
                "/api/v1/whatsapp/inbound/processing-runs",
                json={"batch_id": str(batch_id), "paperless_check": False},
            ).json()["processing_run"]
            source_item_id = client.get(
                f"/api/v1/whatsapp/inbound/processing-runs/{first['id']}"
            ).json()["items"][0]["id"]
            approved = client.post(
                f"/api/v1/whatsapp/inbound/processing-items/{source_item_id}/review",
                json={
                    "decision": "approved",
                    "reviewed_by": "Ahmad",
                    "category": "crm_complaint",
                    "complaint_number": "104-6609317",
                },
            )
            assert approved.status_code == 200, approved.text

        with Session(engine) as session:
            source_item = session.get(
                WhatsAppInboundProcessingItem, uuid.UUID(source_item_id)
            )
            assert source_item is not None
            source_item.extracted_text = text
            source_item.normalized_content_sha256 = normalized_content_sha256(text)
            session.add(source_item)
            repeat_batch = add_repeat_batch(
                session,
                tmp_path=tmp_path,
                batch_code="WAB-NORMALIZED-4C",
                message_id="processing-message-normalized",
                sha256="c" * 64,
            )
            repeat_run = create_processing_run(
                session,
                batch=repeat_batch,
                settings=settings,
                paperless_check=False,
            )
            session.flush()
            repeat_item = session.exec(
                select(WhatsAppInboundProcessingItem).where(
                    WhatsAppInboundProcessingItem.run_id == repeat_run.id
                )
            ).one()
            repeat_item.primary_category = "possible_crm_complaint"
            repeat_item.detected_complaint_number = "104-6609317"
            repeat_item.extracted_text = "  " + text.upper().replace(" ", "   ") + "  "
            repeat_item.extraction_method = "pdftotext"
            repeat_item.status = "needs_review"
            assert resolve_normalized_duplicate_after_extraction(
                session, item=repeat_item
            )
            assert repeat_item.content_match_kind == "normalized_reused"
            assert repeat_item.status == "approved"
            assert repeat_item.canonical_processing_item_id == uuid.UUID(source_item_id)
            repeat_document = session.exec(
                select(ComplaintDocument).where(
                    ComplaintDocument.source_processing_item_id == repeat_item.id
                )
            ).one()
            assert repeat_document.duplicate_of_document_id is not None
    finally:
        app.dependency_overrides.clear()
