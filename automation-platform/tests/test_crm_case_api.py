from __future__ import annotations

import uuid

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from crm_domain import api as crm_api
from automation_api.main import app
from automation_core.database import get_session
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintMatch,
    PaperlessPublication,
)


class _Task:
    id = "crm-publication-task"


def test_case_registry_filters_exact_paperless_status_and_paginates() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        existing = ComplaintCase(
            complaint_number="104-6609317",
            state="existing",
            canonical_paperless_document_id=731,
            complainant_name="Maryam Noor",
            district="Lahore",
        )
        fresh = ComplaintCase(
            complaint_number="104-6609318",
            state="review_required",
            complainant_name="Ali Ahmad",
            district="Kasur",
        )
        session.add(existing)
        session.add(fresh)
        session.flush()
        session.add(
            ComplaintMatch(
                complaint_case_id=existing.id,
                paperless_document_id=731,
                proposed_decision="existing",
                final_decision="existing",
                score=1.0,
                reason="Exact complaint number matched one CRM main complaint.",
                signals_json={
                    "paperless_category": "submitted",
                    "paperless_statuses": ["Submitted"],
                },
            )
        )
        session.add(
            ComplaintMatch(
                complaint_case_id=existing.id,
                proposed_decision="fresh",
                score=0.0,
                reason="A later capture did not find another Paperless document.",
                signals_json={
                    "paperless_category": "fresh",
                    "paperless_statuses": [],
                },
            )
        )
        session.commit()

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            response = client.get(
                "/api/v1/crm/cases",
                params={
                    "paperless": "submitted",
                    "district": "lah",
                    "limit": 1,
                    "offset": 0,
                    "sort": "complaint_number",
                    "order": "asc",
                },
            )
            assert response.status_code == 200, response.text
            assert response.json()["total"] == 1
            item = response.json()["items"][0]
            assert item["complaint_number"] == "104-6609317"
            assert item["paperless_result"] == {
                "category": "submitted",
                "statuses": ["Submitted"],
                "reason": "Exact complaint number matched one CRM main complaint.",
                "document_id": 731,
            }

            needs_review = client.get(
                "/api/v1/crm/cases", params={"state": "needs_review", "limit": 10}
            )
            assert needs_review.status_code == 200, needs_review.text
            assert needs_review.json()["total"] == 1
            assert needs_review.json()["items"][0]["complaint_number"] == "104-6609318"
    finally:
        app.dependency_overrides.clear()


def test_publication_staging_collapses_identical_captures_with_main_role_winning() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        case = ComplaintCase(
            complaint_number="104-6609317",
            state="fresh",
            remarks="Complete complaint remarks.",
        )
        session.add(case)
        session.flush()
        for role in ("complaint_details", "main_complaint"):
            document = ComplaintDocument(
                source_processing_item_id=uuid.uuid4(),
                source_sha256="f" * 64,
                original_filename="same-complaint.pdf",
                role=role,
            )
            session.add(document)
            session.flush()
            session.add(
                ComplaintDocumentCaseLink(
                    complaint_case_id=case.id,
                    complaint_document_id=document.id,
                    role=role,
                    review_state="accepted",
                )
            )
        session.commit()

        documents = crm_api._accepted_publication_documents(session, case)
        assert len(documents) == 1
        assert documents[0][1].role == "main_complaint"
        crm_api._stage_case_publications(session, case)
        session.commit()
        publications = session.exec(select(PaperlessPublication)).all()
        assert len(publications) == 1
        assert publications[0].intended_fields_json["role"] == "main_complaint"


def test_case_specific_document_review_is_required_before_publication(monkeypatch) -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        case = ComplaintCase(
            complaint_number="104-6609317",
            state="review_required",
            remarks="The school matter requires investigation.",
        )
        session.add(case)
        session.flush()
        document = ComplaintDocument(
            source_processing_item_id=uuid.uuid4(),
            source_sha256="a" * 64,
            original_filename="complaint.pdf",
            mime_type="application/pdf",
            role="unclassified",
        )
        session.add(document)
        session.flush()
        link = ComplaintDocumentCaseLink(
            complaint_case_id=case.id,
            complaint_document_id=document.id,
            role="main_complaint",
            review_state="proposed",
            source_locator="sheet:Sheet1:row:42",
        )
        session.add(link)
        duplicate = ComplaintDocument(
            source_processing_item_id=uuid.uuid4(),
            source_sha256="a" * 64,
            original_filename="complaint.pdf",
            mime_type="application/pdf",
            role="main_complaint",
            review_state="duplicate",
        )
        session.add(duplicate)
        session.flush()
        session.add(
            ComplaintDocumentCaseLink(
                complaint_case_id=case.id,
                complaint_document_id=duplicate.id,
                role="main_complaint",
                review_state="duplicate",
                source_locator="document",
            )
        )
        session.commit()
        case_id, document_id = case.id, document.id

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    monkeypatch.setattr("crm_domain.api.celery_app.send_task", lambda *args, **kwargs: _Task())
    try:
        with TestClient(app) as client:
            case_list = client.get("/api/v1/crm/cases")
            assert case_list.status_code == 200, case_list.text
            assert case_list.json()["items"][0]["document_count"] == 1

            detail = client.get(f"/api/v1/crm/cases/{case_id}")
            assert detail.status_code == 200, detail.text
            assert detail.json()["documents"][0]["role"] == "main_complaint"
            assert detail.json()["documents"][0]["review_state"] == "proposed"
            assert detail.json()["document_count"] == 1
            assert detail.json()["capture_count"] == 2
            assert detail.json()["documents"][0]["duplicate_capture_count"] == 2
            assert detail.json()["documents"][1]["duplicate_of_document_id"] == str(document_id)

            blocked = client.post(f"/api/v1/crm/cases/{case_id}/approve-fresh")
            assert blocked.status_code == 409

            reviewed = client.patch(
                f"/api/v1/crm/cases/{case_id}/documents/{document_id}",
                json={"role": "main_complaint", "accepted": True},
            )
            assert reviewed.status_code == 200, reviewed.text
            approved = client.post(f"/api/v1/crm/cases/{case_id}/approve-fresh")
            assert approved.status_code == 200, approved.text
            published = client.post(f"/api/v1/crm/cases/{case_id}/publish")
            assert published.status_code == 202, published.text
            assert published.json()["task_id"] == "crm-publication-task"

        with Session(engine) as session:
            publication = session.exec(select(PaperlessPublication)).one()
            assert publication.intended_fields_json["role"] == "main_complaint"
            persisted_document = session.get(ComplaintDocument, document_id)
            assert persisted_document is not None
            assert persisted_document.role == "unclassified"
    finally:
        app.dependency_overrides.clear()


def test_batch_publication_is_atomic_and_requires_verified_case_data(monkeypatch) -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        case_ids: list[uuid.UUID] = []
        for number, remarks in (
            ("104-6609317", "Complete first complaint remarks."),
            ("104-6609318", None),
        ):
            case = ComplaintCase(
                complaint_number=number,
                state="fresh",
                remarks=remarks,
            )
            session.add(case)
            session.flush()
            document = ComplaintDocument(
                source_processing_item_id=uuid.uuid4(),
                source_sha256=number.replace("-", "").ljust(64, "0"),
                original_filename=f"{number}.pdf",
                mime_type="application/pdf",
                role="main_complaint",
                review_state="accepted",
            )
            session.add(document)
            session.flush()
            session.add(
                ComplaintDocumentCaseLink(
                    complaint_case_id=case.id,
                    complaint_document_id=document.id,
                    role="main_complaint",
                    review_state="accepted",
                )
            )
            case_ids.append(case.id)
        session.commit()

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    monkeypatch.setattr("crm_domain.api.celery_app.send_task", lambda *args, **kwargs: _Task())
    try:
        with TestClient(app) as client:
            statistics = client.get("/api/v1/crm/cases/statistics")
            assert statistics.status_code == 200, statistics.text
            assert statistics.json() == {
                "unique_cases": 2,
                "needs_review": 0,
                "approved_cases": 2,
                "awaiting_publication": 2,
                "ready_to_publish": 1,
                "blocked_from_publication": 1,
                "publishing": 0,
                "published": 0,
                "existing": 0,
                "rejected": 0,
            }
            blocked = client.post(
                "/api/v1/crm/cases/publication-batches",
                json={"case_ids": [str(case_id) for case_id in case_ids]},
            )
            assert blocked.status_code == 409, blocked.text
            assert "complete complaint remarks" in blocked.text

            with Session(engine) as session:
                assert session.exec(select(PaperlessPublication)).all() == []
                assert all(
                    session.get(ComplaintCase, case_id).state == "fresh" for case_id in case_ids
                )
                second = session.get(ComplaintCase, case_ids[1])
                assert second is not None
                second.remarks = "Complete second complaint remarks."
                session.add(second)
                session.commit()

            queued = client.post(
                "/api/v1/crm/cases/publication-batches",
                json={"case_ids": [str(case_id) for case_id in case_ids]},
            )
            assert queued.status_code == 202, queued.text
            assert queued.json()["queued_count"] == 2
            statistics = client.get("/api/v1/crm/cases/statistics")
            assert statistics.status_code == 200, statistics.text
            assert statistics.json()["unique_cases"] == 2
            assert statistics.json()["approved_cases"] == 2
            assert statistics.json()["publishing"] == 2
            assert statistics.json()["ready_to_publish"] == 0

        with Session(engine) as session:
            assert len(session.exec(select(PaperlessPublication)).all()) == 2
            assert all(
                session.get(ComplaintCase, case_id).state == "publishing" for case_id in case_ids
            )
    finally:
        app.dependency_overrides.clear()
