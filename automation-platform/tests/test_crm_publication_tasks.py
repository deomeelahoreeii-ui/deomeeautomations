from __future__ import annotations

import uuid

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from crm_domain.models import ComplaintCase, ComplaintDocument, PaperlessPublication
from whatsapp_gateway.inbound import publication_tasks


def test_unexpected_publication_failure_returns_case_to_retryable_state(monkeypatch) -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        case = ComplaintCase(complaint_number="104-6609317", state="publishing")
        session.add(case)
        session.flush()
        document = ComplaintDocument(
            source_processing_item_id=uuid.uuid4(),
            original_filename="complaint.pdf",
            role="main_complaint",
        )
        session.add(document)
        session.flush()
        publication = PaperlessPublication(
            complaint_case_id=case.id,
            complaint_document_id=document.id,
            idempotency_key="publication-recovery-test",
            state="pending",
        )
        session.add(publication)
        session.commit()
        case_id, publication_id = case.id, publication.id

    monkeypatch.setattr(publication_tasks, "engine", engine)
    monkeypatch.setattr(
        publication_tasks,
        "_publish_complaint_case",
        lambda _case_id: (_ for _ in ()).throw(RuntimeError("worker crashed")),
    )

    with pytest.raises(RuntimeError, match="worker crashed"):
        publication_tasks.publish_complaint_case.run(str(case_id))

    with Session(engine) as session:
        recovered_case = session.get(ComplaintCase, case_id)
        recovered_publication = session.get(PaperlessPublication, publication_id)
        assert recovered_case is not None
        assert recovered_case.state == "fresh"
        assert recovered_publication is not None
        assert recovered_publication.state == "failed"
        assert recovered_publication.last_error == "worker crashed"
