from __future__ import annotations

import uuid

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.database import get_session
from crm_domain.models import CrmSpreadsheetIntakeBatch, CrmSpreadsheetIntakeRow


def test_spreadsheet_list_and_row_review_pages_receive_their_runtime_payloads() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    processing_item_id = uuid.uuid4()
    run_id = uuid.uuid4()
    with Session(engine) as session:
        batch = CrmSpreadsheetIntakeBatch(
            processing_item_id=processing_item_id,
            run_id=run_id,
            source_filename="complaints.xlsx",
            source_sha256="a" * 64,
            status="awaiting_review",
            total_rows=1,
            candidate_rows=1,
            fresh_rows=1,
        )
        session.add(batch)
        session.flush()
        row = CrmSpreadsheetIntakeRow(
            batch_id=batch.id,
            processing_item_id=processing_item_id,
            sheet_name="Complaints",
            row_number=2,
            source_locator="sheet:Complaints:row:2",
            row_sha256="b" * 64,
            values_json={"Complaint Number": "104-7200001", "Name": "Test Person"},
            complaint_number="104-7200001",
            status="fresh",
            paperless_category="fresh",
            paperless_reason="No Paperless match.",
        )
        session.add(row)
        session.commit()
        batch_id = batch.id

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            listing = client.get(
                "/api/v1/whatsapp/inbound/spreadsheet-batches",
                params={"run_id": str(run_id), "status": "awaiting_review"},
            )
            assert listing.status_code == 200, listing.text
            assert listing.json()["total"] == 1
            listed = listing.json()["items"][0]
            assert listed["source_filename"] == "complaints.xlsx"
            assert listed["review_url"] == f"/crm/intake/spreadsheets/{batch_id}"
            assert listed["candidate_rows"] == 1

            searched = client.get(
                "/api/v1/whatsapp/inbound/spreadsheet-batches",
                params={"search": "COMPLAINTS.XLSX"},
            )
            assert searched.status_code == 200, searched.text
            assert searched.json()["total"] == 1

            detail = client.get(
                f"/api/v1/whatsapp/inbound/spreadsheet-batches/{batch_id}",
                params={"status": "fresh", "paperless_category": "fresh"},
            )
            assert detail.status_code == 200, detail.text
            payload = detail.json()
            assert payload["batch"]["source_filename"] == "complaints.xlsx"
            assert payload["total"] == 1
            assert payload["items"][0]["complaint_number"] == "104-7200001"
            assert payload["items"][0]["values"]["Name"] == "Test Person"
    finally:
        app.dependency_overrides.clear()
