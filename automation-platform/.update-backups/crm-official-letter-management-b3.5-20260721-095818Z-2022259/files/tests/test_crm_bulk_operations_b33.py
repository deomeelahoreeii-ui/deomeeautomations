from __future__ import annotations

import io
import uuid
import zipfile
from pathlib import Path

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.bulk_operations import (
    BulkOperationValidationError,
    CrmBulkOperationService,
    reference_file,
)
from crm_domain.models import (
    ComplaintCase,
    ComplaintReply,
    CrmBulkOperationBatch,
    CrmBulkOperationItem,
)


ROOT = Path(__file__).resolve().parents[1]
BULK_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/index.astro"
BATCH_LIST = ROOT / "apps/web/src/pages/crm/replies/bulk/batches/index.astro"
BATCH_DETAIL = ROOT / "apps/web/src/pages/crm/replies/bulk/batches/[id].astro"
RECORDS = ROOT / "apps/web/src/pages/crm/replies/bulk/records/index.astro"
REPLY_QUEUE = ROOT / "apps/web/src/pages/crm/replies/index.astro"
MAIN = ROOT / "apps/api/automation_api/main.py"
MIGRATION = ROOT / "alembic/versions/f1a5c7e9b306_crm_bulk_operation_batches.py"
CRM_CSS = ROOT / "apps/web/src/styles/crm.css"


def engine():
    db = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db)
    return db


def settings(tmp_path: Path) -> Settings:
    return Settings(
        database_url="sqlite://",
        artifact_root=tmp_path / "artifacts",
        deomee_root=tmp_path,
    )


def seed(session: Session) -> list[ComplaintCase]:
    rows = [
        ComplaintCase(
            complaint_number="104-8100001",
            state="published",
            remarks="First full complaint for a controlled export batch.",
            canonical_paperless_document_id=901,
        ),
        ComplaintCase(
            complaint_number="104-8100002",
            state="published",
            remarks="Second full complaint for a controlled export batch.",
            canonical_paperless_document_id=902,
        ),
    ]
    session.add_all(rows)
    session.commit()
    for row in rows:
        session.refresh(row)
    return rows


def test_export_import_and_letters_preserve_batch_lineage(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        cases = seed(session)
        service = CrmBulkOperationService(session, settings(tmp_path))

        export = service.create_export_batch(scope="awaiting")
        assert export["operation_type"] == "reply_export"
        assert export["status"] == "completed"
        assert export["total_items"] == 2
        export_artifact = next(item for item in export["artifacts"] if item["kind"] == "export_package")
        _, export_path = service.artifact_path(uuid.UUID(export_artifact["id"]))
        with zipfile.ZipFile(export_path) as archive:
            assert {
                "01-complaints-for-chatgpt.csv",
                "02-reply-template.csv",
                "03-chatgpt-instructions.txt",
                "04-example-completed-replies.csv",
                "batch-manifest.csv",
                "batch-summary.json",
            } <= set(archive.namelist())
            complaints = archive.read("01-complaints-for-chatgpt.csv").decode("utf-8-sig")
            assert "104-8100001" in complaints
            assert "First full complaint" in complaints

        validation = service.validate_import_batch(
            content=(
                "Complaint Number,Reply\n"
                '104-8100001,"The first matter has been examined and addressed."\n'
            ).encode(),
            filename="completed-replies.csv",
            parent_batch_id=uuid.UUID(export["id"]),
        )
        assert validation["status"] == "ready"
        assert validation["valid_items"] == 1
        assert validation["failed_items"] == 1
        assert validation["parent"]["id"] == export["id"]
        assert any(item["kind"] == "validation_errors" for item in validation["artifacts"])
        with pytest.raises(BulkOperationValidationError, match="Strict import"):
            service.commit_import_batch(uuid.UUID(validation["id"]))

        committed = service.commit_import_batch(
            uuid.UUID(validation["id"]),
            allow_partial=True,
        )
        assert committed["status"] == "completed_with_errors"
        assert committed["commit_summary"] == {
            "imported": 1,
            "updated": 0,
            "unchanged": 0,
            "letters_ready": 1,
        }
        reply = session.exec(
            select(ComplaintReply).where(ComplaintReply.complaint_case_id == cases[0].id)
        ).one()
        assert reply.version == 1
        assert reply.generated_at is None

        letters = service.create_letter_batch(
            parent_batch_id=uuid.UUID(committed["id"]),
            scope="import_batch",
        )
        assert letters["operation_type"] == "formal_letters"
        assert letters["parent"]["id"] == committed["id"]
        assert letters["successful_items"] == 1
        letter_artifact = next(item for item in letters["artifacts"] if item["kind"] == "letter_package")
        _, letter_path = service.artifact_path(uuid.UUID(letter_artifact["id"]))
        with zipfile.ZipFile(letter_path) as archive:
            assert "104-8100001/104-8100001 - DEO Report.odt" in archive.namelist()
            assert "104-8100002/104-8100002 - DEO Report.odt" not in archive.namelist()

        batches = session.exec(select(CrmBulkOperationBatch)).all()
        items = session.exec(select(CrmBulkOperationItem)).all()
        assert len(batches) == 3
        assert {row.operation_type for row in batches} == {"reply_export", "reply_import", "formal_letters"}
        assert any(row.status == "missing" for row in items)
        assert any(row.status == "generated" for row in items)


def test_reference_files_use_exact_chatgpt_contract() -> None:
    name, content_type, content = reference_file("reply-template")
    assert name.endswith(".csv")
    assert content_type.startswith("text/csv")
    assert content.decode("utf-8-sig").splitlines()[0] == "Complaint Number,Reply"

    name, _, sample = reference_file("example-completed")
    assert name.endswith(".csv")
    assert "104-1234567" in sample.decode("utf-8-sig")

    name, _, instructions = reference_file("chatgpt-instructions")
    assert name.endswith(".txt")
    text = instructions.decode()
    assert "exactly these two columns" in text
    assert "Do not add explanations" in text


def test_batch_ui_replaces_flat_register_with_clickable_tables() -> None:
    queue = REPLY_QUEUE.read_text(encoding="utf-8")
    bulk = BULK_PAGE.read_text(encoding="utf-8")
    batches = BATCH_LIST.read_text(encoding="utf-8")
    detail = BATCH_DETAIL.read_text(encoding="utf-8")
    records = RECORDS.read_text(encoding="utf-8")
    css = CRM_CSS.read_text(encoding="utf-8")

    assert "/crm/replies/bulk/" in queue
    assert "Legacy register" not in queue
    assert "CSV reply coverage" not in queue
    for token in (
        "Export batch",
        "ChatGPT reference files",
        "Validate before changing replies",
        "Formal-letter batch",
        "Copy ChatGPT prompt",
        "reference/reply-template",
        "reference/example-completed",
        "import-batches/validate",
        "Commit import batch",
        "letter-batches",
        "metric-link",
        "recent-batches-table",
    ):
        assert token in bulk
    assert "DataTable" in batches
    assert "manualPagination: true" in batches
    for token in ("Batch items", "batch-items-table", "Related batches", "Downloads", "retry-batch"):
        assert token in detail
    assert "bulk-records-table" in records
    assert 'header:"Complaint No."' in records
    assert "table-preview-cell" in records
    assert ".bulk-workflow-grid" in css
    assert ".batch-detail-grid" in css


def test_router_and_migration_register_batch_feature() -> None:
    main = MAIN.read_text(encoding="utf-8")
    migration = MIGRATION.read_text(encoding="utf-8")
    assert "app.include_router(crm_bulk_operations_router)" in main
    assert 'revision: str = "f1a5c7e9b306"' in migration
    assert 'down_revision: Union[str, None] = "e9b3c5d7f104"' in migration
    for table in (
        "crm_bulk_operation_batches",
        "crm_bulk_operation_items",
        "crm_bulk_operation_artifacts",
    ):
        assert f'"{table}"' in migration
        assert f'op.drop_table("{table}")' in migration
    assert "CRM-IMP-HISTORICAL" in migration
    assert "CRM-LET-HISTORICAL" in migration
