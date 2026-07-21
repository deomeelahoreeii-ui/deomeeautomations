from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.models import ComplaintCase, ComplaintReplyRevision, CrmOfficialLetter
from crm_domain.official_letters import OfficialLetterService, OfficialLetterValidationError

ROOT = Path(__file__).resolve().parents[1]
PREPARE_PAGE = ROOT / "apps/web/src/pages/crm/replies/[id]/official-letter.astro"
REGISTER_PAGE = ROOT / "apps/web/src/pages/crm/replies/official-letters/index.astro"
MIGRATION = ROOT / "alembic/versions/ff29cbe4a5f0_official_letter_preview_lifecycle.py"


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
        _env_file=None,
        database_url="sqlite://",
        artifact_root=tmp_path / "artifacts",
        deomee_root=tmp_path,
    )


def seed(session: Session) -> tuple[ComplaintCase, ComplaintReplyRevision]:
    case = ComplaintCase(
        complaint_number="104-6747651",
        state="published",
        remarks="Complaint details for lifecycle testing.",
        category="Fee",
        sub_category="Summer Vacations",
    )
    session.add(case)
    session.flush()
    revision = ComplaintReplyRevision(
        complaint_case_id=case.id,
        reply_text="Respected Worthy Chief Executive Officer (DEA),\n\nApproved reply.",
        content_hash="f" * 64,
        approval_status="Approved",
        source_reference="HD-0029",
    )
    session.add(revision)
    session.commit()
    session.refresh(case)
    session.refresh(revision)
    return case, revision


def generation_kwargs(revision: ComplaintReplyRevision) -> dict[str, object]:
    return {
        "letter_number": "1595/PMDU/CRM",
        "letter_date": date(2026, 7, 13),
        "recipient_name": "The Chief Executive Officer (DEA),",
        "recipient_location": "Lahore",
        "subject_prefix": "COMPLAINT NO.",
        "cc_entries": "Office Record.",
        "template_id": None,
        "signature_profile_id": None,
        "reply_revision_id": revision.id,
        "actor": "test",
        "finalize": False,
    }


def test_repeated_preview_generation_updates_one_active_record(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, revision = seed(session)
        service = OfficialLetterService(session, settings(tmp_path))
        first = service.generate(case.id, **generation_kwargs(revision))
        second = service.generate(case.id, **generation_kwargs(revision))
        assert second["id"] == first["id"]
        rows = session.exec(
            select(CrmOfficialLetter).where(
                CrmOfficialLetter.complaint_case_id == case.id
            )
        ).all()
        assert [(row.status, row.revision) for row in rows] == [("preview", 1)]
        prepared = service.prepare_case(case.id)
        assert prepared["active_letter"]["id"] == first["id"]
        assert len(prepared["letters"]) == 1


def test_finalization_discards_noisy_previews_and_blocks_implicit_second_final(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, revision = seed(session)
        service = OfficialLetterService(session, settings(tmp_path))
        preview = service.generate(case.id, **generation_kwargs(revision))
        finalized = service.finalize(uuid.UUID(preview["id"]), actor="test")
        assert finalized["status"] == "finalized"
        prepared = service.prepare_case(case.id)
        assert prepared["active_letter"]["id"] == finalized["id"]
        assert [letter["status"] for letter in prepared["letters"]] == ["finalized"]
        with pytest.raises(OfficialLetterValidationError, match="Create Revised Letter"):
            service.generate(case.id, **generation_kwargs(revision))
        listed = service.list_letters()
        assert listed["total"] == 1
        assert listed["items"][0]["status"] == "finalized"


def test_explicit_revised_preview_is_reused_and_supersedes_current_final(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, revision = seed(session)
        service = OfficialLetterService(session, settings(tmp_path))
        initial = service.generate(
            case.id,
            **{**generation_kwargs(revision), "finalize": True},
        )
        revised_kwargs = {
            **generation_kwargs(revision),
            "letter_number": "1596/PMDU/CRM",
            "letter_date": date(2026, 7, 14),
            "supersedes_letter_id": uuid.UUID(initial["id"]),
        }
        first_preview = service.generate(case.id, **revised_kwargs)
        second_preview = service.generate(case.id, **revised_kwargs)
        assert first_preview["id"] == second_preview["id"]
        revised = service.finalize(uuid.UUID(first_preview["id"]), actor="test")
        assert revised["status"] == "finalized"
        previous = session.get(CrmOfficialLetter, uuid.UUID(initial["id"]))
        assert previous and previous.status == "superseded"
        statuses = session.exec(
            select(CrmOfficialLetter.status).where(
                CrmOfficialLetter.complaint_case_id == case.id
            )
        ).all()
        assert sorted(statuses) == ["finalized", "superseded"]


def test_model_migration_and_ui_enforce_active_letter_lifecycle() -> None:
    table = CrmOfficialLetter.__table__
    index_names = {index.name for index in table.indexes}
    assert "uq_crm_official_letters_active_preview" in index_names
    assert "uq_crm_official_letters_current_finalized" in index_names
    migration = MIGRATION.read_text(encoding="utf-8")
    assert "status = 'discarded'" in migration
    assert "_repair_existing_rows" in migration
    assert 'revision: str = "ff29cbe4a5f0"' in migration
    prepare = PREPARE_PAGE.read_text(encoding="utf-8")
    assert "data.active_letter" in prepare
    assert "letter.supersedes_letter_id || ''" in prepare
    register = REGISTER_PAGE.read_text(encoding="utf-8")
    assert "All statuses" in register
