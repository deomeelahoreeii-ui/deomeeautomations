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
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintReplyRevision,
    CrmDispatchBatch,
    CrmOfficialLetter,
    CrmOfficialLetterArtifact,
    CrmOfficialLetterSignatureProfile,
)
from crm_domain.official_letters import OfficialLetterService, OfficialLetterValidationError


ROOT = Path(__file__).resolve().parents[1]
EDITOR = ROOT / "apps/web/src/pages/crm/replies/[id].astro"
API = ROOT / "packages/crm_domain/crm_domain/official_letters_api.py"
DEV = ROOT / "scripts/dev.sh"


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


def seed(session: Session, *, approved: bool = True) -> tuple[ComplaintCase, ComplaintReplyRevision | None]:
    case = ComplaintCase(
        complaint_number=f"104-{uuid.uuid4().int % 10_000_000:07d}",
        state="published",
        remarks="A complete complaint record that must follow the official letter in the packet.",
        category="Administrative",
        sub_category="Others",
    )
    session.add(case)
    session.flush()
    revision = None
    if approved:
        revision = ComplaintReplyRevision(
            complaint_case_id=case.id,
            reply_text="The complaint was examined and the verified matter has been addressed.",
            content_hash="a" * 64,
            approval_status="Approved",
            source_reference="HD-QUICK-1",
            is_current=True,
        )
        session.add(revision)
    session.commit()
    session.refresh(case)
    if revision:
        session.refresh(revision)
    return case, revision


def create_from_preparation(
    service: OfficialLetterService,
    case_id: uuid.UUID,
    *,
    supersede: bool = False,
) -> dict:
    prepared = service.prepare_final_packet(case_id)
    revision = prepared["approved_revision"]
    assert revision
    return service.create_final_packet(
        case_id,
        confirmation_token=prepared["confirmation_token"],
        approved_revision_id=uuid.UUID(revision["id"]),
        configuration_updated_at=prepared["defaults"]["configuration_updated_at"],
        supersede_current=supersede,
        actor="test-operator",
    )


def test_quick_packet_is_blocked_until_reply_is_approved(tmp_path: Path) -> None:
    with Session(engine()) as session:
        case, _ = seed(session, approved=False)
        prepared = OfficialLetterService(session, settings(tmp_path)).prepare_final_packet(case.id)
        assert prepared["action"] == "blocked"
        assert prepared["approved_revision"] is None
        assert "Approve or issue" in prepared["reason"]
        assert prepared["defaults"]["template"]["active"] is True
        assert prepared["defaults"]["signature"]["active"] is True


def test_one_command_creates_ordered_complete_packet_and_retry_is_idempotent(tmp_path: Path) -> None:
    with Session(engine()) as session:
        case, revision = seed(session)
        assert revision
        service = OfficialLetterService(session, settings(tmp_path))
        prepared = service.prepare_final_packet(case.id)
        assert prepared["action"] == "create"

        created = create_from_preparation(service, case.id)
        assert created["status"] == "finalized"
        assert created["action_performed"] == "create"
        complete = next(item for item in created["artifacts"] if item["kind"] == "complete_pdf")
        assert complete["size_bytes"] > 0
        assert created["complete_pdf"]["page_count"] >= 2
        assert created["complete_pdf"]["order"][:2] == ["official_letter", "main_complaint"]

        retried = create_from_preparation(service, case.id)
        assert retried["id"] == created["id"]
        assert retried["idempotent"] is True
        assert len(session.exec(select(CrmOfficialLetter)).all()) == 1
        artifacts = session.exec(
            select(CrmOfficialLetterArtifact).where(
                CrmOfficialLetterArtifact.official_letter_id == uuid.UUID(created["id"]),
                CrmOfficialLetterArtifact.kind == "complete_pdf",
            )
        ).all()
        assert len(artifacts) == 1
        events = session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.complaint_case_id == case.id,
                ComplaintAuditEvent.event_type == "final_packet_created",
            )
        ).all()
        assert len(events) == 1
        assert session.exec(select(CrmDispatchBatch)).all() == []


def test_quick_packet_blocks_an_expired_default_signature(tmp_path: Path) -> None:
    with Session(engine()) as session:
        case, revision = seed(session)
        assert revision
        service = OfficialLetterService(session, settings(tmp_path))
        configuration = service.configuration()
        signature = session.get(
            CrmOfficialLetterSignatureProfile,
            uuid.UUID(configuration["settings"]["default_signature_id"]),
        )
        assert signature
        signature.effective_to = date(2020, 1, 1)
        session.add(signature)
        session.commit()
        prepared = service.prepare_final_packet(case.id)
        assert prepared["action"] == "blocked"
        assert "expired" in prepared["reason"]


def test_failed_quick_packet_rolls_back_records_files_and_records_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Session(engine()) as session:
        case, revision = seed(session)
        assert revision
        service = OfficialLetterService(session, settings(tmp_path))
        prepared = service.prepare_final_packet(case.id)

        def fail_composition(*args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("forced packet composition failure")

        monkeypatch.setattr(service, "compose_complete_pdf", fail_composition)
        with pytest.raises(RuntimeError, match="forced packet"):
            service.create_final_packet(
                case.id,
                confirmation_token=prepared["confirmation_token"],
                approved_revision_id=revision.id,
                configuration_updated_at=prepared["defaults"]["configuration_updated_at"],
                supersede_current=False,
                actor="test",
            )
        assert session.exec(select(CrmOfficialLetter)).all() == []
        assert list(service.letters_root.iterdir()) == []
        event = session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.complaint_case_id == case.id,
                ComplaintAuditEvent.event_type == "final_packet_created",
                ComplaintAuditEvent.state == "failed",
            )
        ).one()
        assert "forced packet composition failure" in (event.error or "")


def test_quick_packet_finishes_preview_and_existing_finalized_letter(tmp_path: Path) -> None:
    with Session(engine()) as session:
        first_case, first_revision = seed(session)
        second_case, second_revision = seed(session)
        assert first_revision and second_revision
        service = OfficialLetterService(session, settings(tmp_path))
        defaults = service.configuration()["settings"]
        common = dict(
            letter_number=defaults["current_letter_number"],
            letter_date=date.fromisoformat(defaults["current_letter_date"]),
            recipient_name=defaults["default_recipient_name"],
            recipient_location=defaults["default_recipient_location"],
            subject_prefix=defaults["default_subject_prefix"],
            cc_entries=defaults["default_cc_entries"],
            template_id=uuid.UUID(defaults["default_template_id"]),
            signature_profile_id=uuid.UUID(defaults["default_signature_id"]),
            actor="test",
        )
        preview = service.generate(
            first_case.id,
            reply_revision_id=first_revision.id,
            finalize=False,
            **common,
        )
        assert service.prepare_final_packet(first_case.id)["action"] == "finalize_preview"
        completed_preview = create_from_preparation(service, first_case.id)
        assert completed_preview["id"] == preview["id"]
        assert completed_preview["status"] == "finalized"

        finalized = service.generate(
            second_case.id,
            reply_revision_id=second_revision.id,
            finalize=True,
            **common,
        )
        assert service.prepare_final_packet(second_case.id)["action"] == "finish"
        finished = create_from_preparation(service, second_case.id)
        assert finished["id"] == finalized["id"]
        assert any(item["kind"] == "complete_pdf" for item in finished["artifacts"])


def test_stale_confirmation_and_revision_supersession_are_explicit(tmp_path: Path) -> None:
    with Session(engine()) as session:
        case, revision = seed(session)
        assert revision
        service = OfficialLetterService(session, settings(tmp_path))
        stale = service.prepare_final_packet(case.id)
        service.update_configuration(
            {"current_letter_number": "2000/PMDU/CRM", "current_letter_date": "2026-07-22"},
            actor="administrator",
        )
        with pytest.raises(OfficialLetterValidationError, match="defaults changed"):
            service.create_final_packet(
                case.id,
                confirmation_token=stale["confirmation_token"],
                approved_revision_id=revision.id,
                configuration_updated_at=stale["defaults"]["configuration_updated_at"],
                supersede_current=False,
                actor="test",
            )

        first = create_from_preparation(service, case.id)
        revision.is_current = False
        newer = ComplaintReplyRevision(
            complaint_case_id=case.id,
            reply_text="A revised approved response based on newly verified evidence.",
            content_hash="b" * 64,
            approval_status="Issued",
            source_reference="HD-QUICK-2",
            is_current=True,
        )
        session.add(revision)
        session.add(newer)
        session.commit()
        prepared = service.prepare_final_packet(case.id)
        assert prepared["action"] == "revise"
        with pytest.raises(OfficialLetterValidationError, match="supersede"):
            create_from_preparation(service, case.id, supersede=False)
        revised = create_from_preparation(service, case.id, supersede=True)
        assert revised["id"] != first["id"]
        assert session.get(CrmOfficialLetter, uuid.UUID(first["id"])).status == "superseded"
        assert service.prepare_final_packet(case.id)["action"] == "existing"


def test_quick_packet_api_ui_and_dev_runtime_contracts() -> None:
    api = API.read_text(encoding="utf-8")
    editor = EDITOR.read_text(encoding="utf-8")
    dev = DEV.read_text(encoding="utf-8")
    for token in (
        '/cases/{case_id}/final-packet/prepare',
        '/cases/{case_id}/final-packet',
        "confirmation_token",
        "supersede_current",
    ):
        assert token in api
    for token in (
        'id="quick-final-packet"',
        'id="final-packet-dialog"',
        "Create and finalize packet",
        "Nothing was dispatched",
        "AbortSignal.timeout(120000)",
        'root.dataset.editorState = "module-started"',
        'import("../../../scripts/alerts")',
    ):
        assert token in editor
    assert 'import { confirmAction } from "../../../scripts/alerts"' not in editor
    assert "wait_for_frontend_module_graph" in dev
    assert "npm run dev -- --force" in dev


def test_database_constraints_guard_concurrent_packet_commands() -> None:
    letter_indexes = {item.name for item in CrmOfficialLetter.__table__.indexes}
    assert "uq_crm_official_letters_current_finalized" in letter_indexes
    artifact_constraints = {item.name for item in CrmOfficialLetterArtifact.__table__.constraints}
    assert "uq_crm_official_letter_artifact_kind" in artifact_constraints
