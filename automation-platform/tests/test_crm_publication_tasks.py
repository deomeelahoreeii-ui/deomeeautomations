from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from crm_domain.models import ComplaintCase, ComplaintDocument, PaperlessPublication
from whatsapp_gateway.inbound import publication_tasks


class _VerificationClient:
    field_values = {
        "Complaint Number": ("1", None),
        "Source": ("2", "source-crm"),
        "Status": ("3", "status-pending"),
        "Document Role": ("4", "role-main"),
    }

    def __init__(self) -> None:
        self.document = {
            "title": "CRM - 104-6609317 - Main Complaint - v1",
            "document_type": 10,
            "correspondent": 20,
            "custom_fields": [
                {"field": field_id, "value": value or "104-6609317"}
                for field_id, value in self.field_values.values()
            ],
        }

    def _get_document(self, _document_id: int):
        return self.document

    def custom_field_payload(self, values):
        return [
            {"field": field_id, "value": value or values[name]}
            for name, (field_id, value) in self.field_values.items()
        ]


def test_main_publication_must_match_live_crm_pending_contract() -> None:
    metadata = SimpleNamespace(
        complaint_type_id=10,
        correspondent_id=20,
        field_ids_by_name={
            "complaint number": "1",
            "source": "2",
            "status": "3",
            "document role": "4",
        },
    )
    case = ComplaintCase(
        complaint_number="104-6609317", state="publishing", version=1
    )
    client = _VerificationClient()
    publication_tasks._verify_main_publication_contract(client, metadata, case, 701)

    client.document["custom_fields"] = [
        item for item in client.document["custom_fields"] if item["field"] != "3"
    ]
    with pytest.raises(RuntimeError, match="Status"):
        publication_tasks._verify_main_publication_contract(client, metadata, case, 701)


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
