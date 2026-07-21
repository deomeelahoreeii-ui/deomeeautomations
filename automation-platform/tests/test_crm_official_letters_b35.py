from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from zipfile import ZipFile

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.models import (
    ComplaintCase,
    ComplaintReply,
    ComplaintReplyRevision,
    CrmOfficialLetter,
)
from crm_domain.official_letters import (
    OfficialLetterService,
    OfficialLetterValidationError,
    render_official_letter_odt,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "alembic/versions/f9e3a5c7d064_crm_official_letter_management.py"
EDITOR = ROOT / "apps/web/src/pages/crm/replies/[id].astro"
PREPARE = ROOT / "apps/web/src/pages/crm/replies/[id]/official-letter.astro"
REGISTER = ROOT / "apps/web/src/pages/crm/replies/official-letters/index.astro"
SETTINGS = ROOT / "apps/web/src/pages/crm/replies/official-letters/settings/index.astro"
QUEUE = ROOT / "apps/web/src/pages/crm/replies/index.astro"
ARCHIVE = ROOT / "apps/web/src/pages/crm/knowledge/index.astro"
TEMPLATE = ROOT / "data/crm/official-letter-sample/deo-crm-official-letter-v1.odt"


def engine():
    db = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    SQLModel.metadata.create_all(db)
    return db


def settings(tmp_path: Path) -> Settings:
    return Settings(_env_file=None, database_url="sqlite://", artifact_root=tmp_path / "artifacts", deomee_root=tmp_path)


def seed(session: Session, *, approved: bool = True) -> tuple[ComplaintCase, ComplaintReplyRevision | None]:
    case = ComplaintCase(
        complaint_number="104-6131659",
        state="published",
        remarks="The complainant seeks review of a service and group-insurance matter.",
        category="Administrative",
        sub_category="Others",
    )
    session.add(case)
    session.flush()
    revision = None
    if approved:
        revision = ComplaintReplyRevision(
            complaint_case_id=case.id,
            reply_text=(
                "Respected Worthy Chief Executive Officer (DEA),\n\n"
                "The matter has been reviewed on the basis of available record.\n\n"
                "Submitted for kind perusal and further necessary action, please."
            ),
            content_hash="a" * 64,
            approval_status="Approved",
            source_reference="HD-0001",
        )
        session.add(revision)
    session.commit()
    session.refresh(case)
    if revision:
        session.refresh(revision)
    return case, revision


def test_default_template_is_exact_legal_size_sample_with_required_markers() -> None:
    assert TEMPLATE.exists()
    with ZipFile(TEMPLATE) as archive:
        assert archive.read("mimetype").decode() == "application/vnd.oasis.opendocument.text"
        content = archive.read("content.xml").decode("utf-8")
        styles = archive.read("styles.xml").decode("utf-8")
    for marker in (
        "{{letter_number}}", "{{letter_date}}", "{{recipient_name}}",
        "{{complaint_number}}", "{{complaint_details}}", "{{approved_reply}}",
        "{{signatory_designation}}", "{{cc_entries}}",
    ):
        assert marker in content
    assert 'fo:page-width="8.5in"' in styles or 'fo:page-width="8.50in"' in styles
    assert 'fo:page-height="14in"' in styles or 'fo:page-height="14.00in"' in styles


def test_approved_reply_generates_preview_and_finalized_odt_pdf(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, revision = seed(session)
        assert revision is not None
        service = OfficialLetterService(session, settings(tmp_path))
        prepared = service.prepare_case(case.id)
        assert prepared["approved_revision"]["id"] == str(revision.id)
        assert prepared["defaults"]["suggested_letter_number"] == "1511/PMDU/CRM"
        preview = service.generate(
            case.id,
            letter_number="1511/PMDU/CRM",
            letter_date=date(2026, 7, 21),
            recipient_name="The Chief Executive Officer (DEA),",
            recipient_location="Lahore",
            subject_prefix="COMPLAINT NO.",
            cc_entries="Office Record.",
            template_id=None,
            signature_profile_id=None,
            reply_revision_id=revision.id,
            actor="test",
            finalize=False,
        )
        assert preview["status"] == "preview"
        artifacts = {item["kind"]: item for item in preview["artifacts"]}
        assert set(artifacts) == {"odt", "pdf"}
        for item in artifacts.values():
            record, path = service.artifact_path(uuid.UUID(item["id"]))
            assert path.exists()
            assert record.sha256 == item["sha256"]
        odt_path = service.artifact_path(uuid.UUID(artifacts["odt"]["id"]))[1]
        with ZipFile(odt_path) as archive:
            content = archive.read("content.xml").decode("utf-8")
        assert "1511/PMDU/CRM" in content
        assert "104-6131659" in content
        assert "{{approved_reply}}" not in content
        finalized = service.finalize(uuid.UUID(preview["id"]), actor="test")
        assert finalized["status"] == "finalized"
        assert finalized["finalized_at"]
        persisted = session.exec(select(CrmOfficialLetter)).one()
        assert persisted.reply_revision_id == revision.id
        assert persisted.reply_text_snapshot == revision.reply_text
        assert persisted.configuration_snapshot_json["template"]["sha256"]
        assert persisted.configuration_snapshot_json["signature"]["sha256"]


def test_unapproved_reply_cannot_create_official_letter(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, _ = seed(session, approved=False)
        with pytest.raises(OfficialLetterValidationError, match="Approve or issue"):
            OfficialLetterService(session, settings(tmp_path)).prepare_case(case.id)


def test_duplicate_number_and_finalized_overwrite_are_blocked(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, revision = seed(session)
        assert revision
        service = OfficialLetterService(session, settings(tmp_path))
        kwargs = dict(
            letter_number="1511/PMDU/CRM", letter_date=date.today(),
            recipient_name="The Chief Executive Officer (DEA),", recipient_location="Lahore",
            subject_prefix="COMPLAINT NO.", cc_entries="Office Record.", template_id=None,
            signature_profile_id=None, reply_revision_id=revision.id, actor="test", finalize=True,
        )
        first = service.generate(case.id, **kwargs)
        with pytest.raises(OfficialLetterValidationError, match="already in use"):
            service.generate(case.id, **kwargs)
        with pytest.raises(OfficialLetterValidationError, match="cannot be overwritten"):
            service.generate(case.id, letter_id=uuid.UUID(first["id"]), **{**kwargs, "letter_number": "1512/PMDU/CRM"})



def test_revised_letter_supersedes_finalized_record_and_statistics_are_exact(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, revision = seed(session)
        assert revision
        service = OfficialLetterService(session, settings(tmp_path))
        first = service.generate(
            case.id, letter_number="1511/PMDU/CRM", letter_date=date(2026, 7, 21),
            recipient_name="The Chief Executive Officer (DEA),", recipient_location="Lahore",
            subject_prefix="COMPLAINT NO.", cc_entries="Office Record.", template_id=None,
            signature_profile_id=None, reply_revision_id=revision.id, actor="test", finalize=True,
        )
        second = service.generate(
            case.id, letter_number="1512/PMDU/CRM", letter_date=date(2026, 7, 22),
            recipient_name="The Chief Executive Officer (DEA),", recipient_location="Lahore",
            subject_prefix="COMPLAINT NO.", cc_entries="Office Record.", template_id=None,
            signature_profile_id=None, reply_revision_id=revision.id, actor="test", finalize=False,
            supersedes_letter_id=uuid.UUID(first["id"]),
        )
        finalized = service.finalize(uuid.UUID(second["id"]), actor="test")
        assert finalized["status"] == "finalized"
        previous = session.get(CrmOfficialLetter, uuid.UUID(first["id"]))
        assert previous and previous.status == "superseded"
        stats = service.statistics()
        assert stats == {"awaiting_letter": 0, "previews": 0, "finalized": 1, "superseded": 1, "failures": 0}
        assert service.suggest_letter_number() == "1513/PMDU/CRM"


def test_configured_date_format_signature_effectivity_and_split_markers(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        case, revision = seed(session)
        assert revision
        service = OfficialLetterService(session, settings(tmp_path))
        config = service.configuration()
        config = service.update_configuration({
            **config["settings"],
            "date_format": "YYYY-MM-DD",
            "require_unique_number": True,
        }, actor="test")
        result = service.generate(
            case.id, letter_number="1511/PMDU/CRM", letter_date=date(2026, 7, 21),
            recipient_name="The Chief Executive Officer (DEA),", recipient_location="Lahore",
            subject_prefix="COMPLAINT NO.", cc_entries="Office Record.", template_id=None,
            signature_profile_id=None, reply_revision_id=revision.id, actor="test", finalize=False,
        )
        odt = service.artifact_path(uuid.UUID(next(x["id"] for x in result["artifacts"] if x["kind"] == "odt")))[1]
        with ZipFile(odt) as archive:
            content = archive.read("content.xml").decode("utf-8")
        assert "2026-07-21" in content
        assert result["pdf_page_count"] >= 1
        with pytest.raises(OfficialLetterValidationError, match="must remain unique"):
            service.update_configuration({"require_unique_number": False}, actor="test")


def test_uploaded_jpeg_signature_is_normalized_to_png(tmp_path: Path) -> None:
    from PIL import Image as PillowImage
    import io

    db = engine()
    with Session(db) as session:
        service = OfficialLetterService(session, settings(tmp_path))
        service.ensure_defaults()
        source = io.BytesIO()
        PillowImage.new("RGB", (180, 80), "white").save(source, format="JPEG")
        profile = service.upload_signature(
            source.getvalue(), filename="signature.jpg", name="Temporary signatory",
            officer_name="Test Officer", designation="DISTRICT EDUCATION OFFICER (M-EE)",
            office_location="LAHORE", effective_from=date(2026, 1, 1),
            effective_to=date(2026, 12, 31), actor="test", make_default=False,
        )
        record, image_path = service.signature_path(uuid.UUID(profile["id"]))
        assert image_path.suffix == ".png"
        assert image_path.read_bytes().startswith(b"\x89PNG")
        assert record.effective_from == date(2026, 1, 1)


def test_ui_api_migration_and_bulk_contracts() -> None:
    editor = EDITOR.read_text()
    prepare = PREPARE.read_text()
    register = REGISTER.read_text()
    settings_page = SETTINGS.read_text()
    queue = QUEUE.read_text()
    archive = ARCHIVE.read_text()
    migration = MIGRATION.read_text()
    bulk = (ROOT / "packages/crm_domain/crm_domain/bulk_operations.py").read_text()
    main = (ROOT / "apps/api/automation_api/main.py").read_text()
    assert "Create Official Letter" in editor
    for token in ("Generate Preview", "Finalize Official Letter", "Create Revised Letter", "letter-preview-frame", "reply-revision-id"):
        assert token in prepare
    for token in ("Official register", "Letter Configuration", "official-letter-table"):
        assert token in register
    for token in ("Template versions", "Signature profiles", "Unique letter number required", "Effective from"):
        assert token in settings_page
    assert "Official letter" in queue
    assert "Official letter" in archive
    assert 'revision: str = "f9e3a5c7d064"' in migration
    assert 'down_revision: Union[str, None] = "f7d1e3a5c842"' in migration
    assert "crm_official_letters" in migration
    assert "crm_official_letter_artifacts" in migration
    assert "crm_official_letters_router" in main
    assert '/statistics' in (ROOT / "packages/crm_domain/crm_domain/official_letters_api.py").read_text()
    assert "OfficialLetterService" in bulk
    assert "Approved/Issued" in bulk
    assert "build_deo_report_odt" not in bulk
