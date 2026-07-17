from __future__ import annotations

import uuid

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.database import get_session
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    PaperlessPublication,
)


class _Task:
    id = "crm-publication-task"


def test_case_specific_document_review_is_required_before_publication(monkeypatch) -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        case = ComplaintCase(complaint_number="104-6609317", state="review_required")
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
        session.commit()
        case_id, document_id = case.id, document.id

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    monkeypatch.setattr("crm_domain.api.celery_app.send_task", lambda *args, **kwargs: _Task())
    try:
        with TestClient(app) as client:
            detail = client.get(f"/api/v1/crm/cases/{case_id}")
            assert detail.status_code == 200, detail.text
            assert detail.json()["documents"][0]["role"] == "main_complaint"
            assert detail.json()["documents"][0]["review_state"] == "proposed"

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
