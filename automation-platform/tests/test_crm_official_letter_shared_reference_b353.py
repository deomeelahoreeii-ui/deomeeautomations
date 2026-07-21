from __future__ import annotations

import csv
import io
import uuid
import zipfile
from datetime import date
from pathlib import Path

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.bulk_operations import CrmBulkOperationService
from crm_domain.models import (
    ComplaintCase,
    ComplaintReplyRevision,
    CrmOfficialLetter,
)
from crm_domain.official_letters import OfficialLetterService

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "alembic/versions/fbe5c7d9e286_crm_shared_official_letter_reference.py"
SETTINGS_PAGE = ROOT / "apps/web/src/pages/crm/replies/official-letters/settings/index.astro"
PREPARE_PAGE = ROOT / "apps/web/src/pages/crm/replies/[id]/official-letter.astro"


def engine():
    return create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,
        database_url="sqlite://",
        artifact_root=tmp_path / "artifacts",
        deomee_root=tmp_path,
    )


def seed_approved(session: Session) -> list[tuple[ComplaintCase, ComplaintReplyRevision]]:
    rows: list[tuple[ComplaintCase, ComplaintReplyRevision]] = []
    for index in range(2):
        case = ComplaintCase(
            complaint_number=f"104-920000{index + 1}",
            state="published",
            remarks=f"Complaint {index + 1} for the shared office reference test.",
            category="Administrative",
            sub_category="Others",
        )
        session.add(case)
        session.flush()
        revision = ComplaintReplyRevision(
            complaint_case_id=case.id,
            reply_text=f"Approved reply {index + 1}.",
            content_hash=(str(index + 1) * 64),
            approval_status="Approved",
            source_reference=f"HD-SHARED-{index + 1}",
        )
        session.add(revision)
        rows.append((case, revision))
    session.commit()
    return rows


def test_configuration_persists_one_shared_number_and_date(tmp_path: Path) -> None:
    db = engine()
    SQLModel.metadata.create_all(db)
    with Session(db) as session:
        service = OfficialLetterService(session, settings(tmp_path))
        configured = service.update_configuration(
            {
                "current_letter_number": "1595/PMDU/CRM",
                "current_letter_date": "2026-07-21",
                "require_unique_number": False,
            },
            actor="test",
        )
        values = configured["settings"]
        assert values["current_letter_number"] == "1595/PMDU/CRM"
        assert values["suggested_letter_number"] == "1595/PMDU/CRM"
        assert values["current_letter_date"] == "2026-07-21"
        assert values["require_unique_number"] is False
        assert service.suggest_letter_number() == "1595/PMDU/CRM"


def test_bulk_generation_reuses_shared_reference_for_every_letter(tmp_path: Path) -> None:
    db = engine()
    SQLModel.metadata.create_all(db)
    with Session(db) as session:
        rows = seed_approved(session)
        official = OfficialLetterService(session, settings(tmp_path))
        official.update_configuration(
            {
                "current_letter_number": "1595/PMDU/CRM",
                "current_letter_date": "2026-07-21",
            },
            actor="test",
        )
        result = CrmBulkOperationService(session, settings(tmp_path)).create_letter_batch(
            scope="selected",
            case_ids=[case.id for case, _revision in rows],
            actor="test",
        )
        assert result["successful_items"] == 2
        letters = session.exec(select(CrmOfficialLetter).order_by(CrmOfficialLetter.complaint_number_snapshot)).all()
        assert [item.letter_number for item in letters] == ["1595/PMDU/CRM", "1595/PMDU/CRM"]
        assert [item.letter_date for item in letters] == [date(2026, 7, 21), date(2026, 7, 21)]
        package = next(item for item in result["artifacts"] if item["kind"] == "letter_package")
        _artifact, package_path = CrmBulkOperationService(session, settings(tmp_path)).artifact_path(uuid.UUID(package["id"]))
        with zipfile.ZipFile(package_path) as archive:
            manifest = archive.read("manifest.csv").decode("utf-8-sig")
            parsed = list(csv.DictReader(io.StringIO(manifest)))
        assert {row["Letter Number"] for row in parsed} == {"1595/PMDU/CRM"}
        assert {row["Letter Date"] for row in parsed} == {"2026-07-21"}


def test_shared_reference_migration_and_ui_contract() -> None:
    migration = MIGRATION.read_text(encoding="utf-8")
    settings_page = SETTINGS_PAGE.read_text(encoding="utf-8")
    prepare_page = PREPARE_PAGE.read_text(encoding="utf-8")
    assert 'revision: str = "fbe5c7d9e286"' in migration
    assert 'down_revision: Union[str, None] = "f9e3a5c7d064"' in migration
    assert "current_letter_number" in migration
    assert "current_letter_date" in migration
    assert 'op.drop_constraint(' in migration
    assert '"uq_crm_official_letters_number"' in migration
    assert "Current letter number" in settings_page
    assert "Current letter date" in settings_page
    assert "same letter number and date" in settings_page
    assert "Unique letter number required" not in settings_page
    assert "Shared letter number" in prepare_page
    assert "Shared letter date" in prepare_page
    assert "Reused for all new letters until changed." in prepare_page
