from __future__ import annotations

import io
import zipfile

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_api.main import app
from automation_core.database import get_session
from crm_domain.models import ComplaintCase


def test_published_complaints_export_import_and_generate_native_letters() -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add_all(
            [
                ComplaintCase(
                    complaint_number="104-6000001",
                    state="published",
                    remarks="First complete complaint narrative.",
                    canonical_paperless_document_id=701,
                ),
                ComplaintCase(
                    complaint_number="104-6000002",
                    state="published",
                    remarks="Second complete complaint narrative.",
                    canonical_paperless_document_id=702,
                ),
                ComplaintCase(
                    complaint_number="104-6000003",
                    state="fresh",
                    remarks="Not published yet.",
                ),
            ]
        )
        session.commit()

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            statistics = client.get("/api/v1/crm/replies/statistics")
            assert statistics.json() == {
                "published_cases": 2,
                "awaiting_reply": 2,
                "replies_imported": 0,
                "letters_generated": 0,
            }

            exported = client.get("/api/v1/crm/replies/complaints.csv")
            assert exported.status_code == 200
            assert "Complaint Number,Complaint Remarks" in exported.text
            assert "104-6000001,First complete complaint narrative." in exported.text
            assert "104-6000003" not in exported.text

            imported = client.post(
                "/api/v1/crm/replies/imports",
                files={
                    "file": (
                        "chatgpt-replies.csv",
                        "Complaint Number,Reply\n"
                        '104-6000001,"Respected Authority, first reply."\n'
                        '104-6000002,"Respected Authority, second reply."\n',
                        "text/csv",
                    )
                },
            )
            assert imported.status_code == 200, imported.text
            assert imported.json()["imported"] == 2

            package = client.post("/api/v1/crm/replies/letter-packages")
            assert package.status_code == 200, package.text
            with zipfile.ZipFile(io.BytesIO(package.content)) as archive:
                names = archive.namelist()
                assert "104-6000001/104-6000001 - DEO Report.odt" in names
                assert "104-6000002/104-6000002 - DEO Report.odt" in names
                assert "manifest.csv" in names
                with zipfile.ZipFile(
                    io.BytesIO(
                        archive.read("104-6000001/104-6000001 - DEO Report.odt")
                    )
                ) as odt:
                    content = odt.read("content.xml").decode()
                    assert "COMPLAINT NO. 104-6000001" in content
                    assert "First complete complaint narrative." in content
                    assert "Respected Authority, first reply." in content

            statistics = client.get("/api/v1/crm/replies/statistics")
            assert statistics.json()["awaiting_reply"] == 0
            assert statistics.json()["letters_generated"] == 2
    finally:
        app.dependency_overrides.clear()


def test_reply_import_is_atomic_when_any_row_is_invalid() -> None:
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        session.add(
            ComplaintCase(
                complaint_number="104-6000001",
                state="published",
                remarks="Complaint narrative.",
            )
        )
        session.commit()

    def session_override():
        with Session(engine) as session:
            yield session

    app.dependency_overrides[get_session] = session_override
    try:
        with TestClient(app) as client:
            response = client.post(
                "/api/v1/crm/replies/imports",
                files={
                    "file": (
                        "bad.csv",
                        "Complaint Number,Reply\n104-6000001,Valid reply\n104-9999999,Unknown\n",
                        "text/csv",
                    )
                },
            )
            assert response.status_code == 422
            assert "104-9999999" in response.text
            assert client.get("/api/v1/crm/replies/statistics").json()["replies_imported"] == 0
    finally:
        app.dependency_overrides.clear()
