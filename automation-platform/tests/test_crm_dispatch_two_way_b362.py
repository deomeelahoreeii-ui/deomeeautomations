from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

from automation_core.config import Settings
from automation_core.job_service import add_job
from automation_core.models import JobType
from crm_domain.dispatch import CrmDispatchService, CrmDispatchValidationError
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintDocument,
    ComplaintMatch,
    ComplaintReplyRevision,
    CrmDispatchArtifact,
    CrmDispatchBatch,
    CrmDispatchItem,
    CrmOfficialLetter,
    CrmOfficialLetterArtifact,
    CrmOfficialLetterSignatureProfile,
    CrmOfficialLetterTemplate,
    CrmPaperlessStatusSync,
)
from crm_filters.paperless import PaperlessStatusUpdateResult
from crm_domain.reply_workspace import ComplaintReplyWorkspaceService
from whatsapp_gateway.models import (
    WhatsAppDelivery,
    WhatsAppDirectoryContact,
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewDelivery,
)


def engine():
    db = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db)
    return db


def settings(tmp_path: Path) -> Settings:
    return Settings(artifact_root=tmp_path / "artifacts", object_storage_enabled=False)


def seed_pending_case(session: Session, number: str = "104-7100001") -> ComplaintCase:
    case = ComplaintCase(
        complaint_number=number,
        state="published",
        remarks="A parent requests field verification and a report from the concerned office.",
        category="Administrative",
        sub_category="Others",
        district="Lahore",
        tehsil="Model Town",
        source_system="crm_portal",
    )
    session.add(case)
    session.commit()
    session.refresh(case)
    return case


def seed_letter(session: Session, tmp_path: Path) -> CrmOfficialLetter:
    import hashlib

    case = seed_pending_case(session, "104-7100002")
    revision = ComplaintReplyRevision(
        complaint_case_id=case.id,
        reply_text="Approved compliance reply.",
        content_hash="a" * 64,
        approval_status="Approved",
    )
    template_path = tmp_path / "sync-template.odt"
    template_path.write_bytes(b"odt")
    signature_path = tmp_path / "sync-signature.png"
    signature_path.write_bytes(b"png")
    template = CrmOfficialLetterTemplate(
        name="Sync template",
        file_path=str(template_path),
        sha256="b" * 64,
    )
    signature = CrmOfficialLetterSignatureProfile(
        name="Sync signatory",
        image_path=str(signature_path),
        image_sha256="c" * 64,
    )
    session.add_all([revision, template, signature])
    session.flush()
    letter = CrmOfficialLetter(
        complaint_case_id=case.id,
        reply_revision_id=revision.id,
        template_id=template.id,
        signature_profile_id=signature.id,
        letter_number="1700/PMDU/CRM",
        letter_date=date(2026, 7, 22),
        recipient_name="The Chief Executive Officer (DEA),",
        recipient_location="Lahore",
        status="finalized",
        complaint_number_snapshot=case.complaint_number,
        complaint_text_snapshot=case.remarks or "",
        reply_text_snapshot=revision.reply_text,
    )
    session.add(letter)
    session.flush()
    packet = tmp_path / "sync-complete.pdf"
    packet.write_bytes(b"%PDF-1.4\n1 0 obj <</Type /Page>> endobj\n%%EOF")
    content = packet.read_bytes()
    session.add(
        CrmOfficialLetterArtifact(
            official_letter_id=letter.id,
            kind="complete_pdf",
            name=packet.name,
            path=str(packet),
            content_type="application/pdf",
            size_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        )
    )
    session.commit()
    return letter


def seed_profile(session: Session, service: CrmDispatchService, *, direction: str) -> dict:
    defaults = service.ensure_defaults()
    contact = WhatsAppDirectoryContact(
        account_id=defaults["account"].id,
        canonical_key=f"92300{uuid.uuid4().int % 10_000_000:07d}",
        phone_jid=f"92300{uuid.uuid4().int % 10_000_000:07d}@c.us",
        display_name="DDEO Model Town" if direction == "downward" else "CEO Office",
        active=True,
    )
    session.add(contact)
    session.commit()
    return service.save_profile(
        profile_id=None,
        name="Lower Office Profile" if direction == "downward" else "Higher Office Profile",
        target_type="contact",
        target_ids=[contact.id],
        template_body="Complaint {{complaint_number}} · {{dispatch_purpose_label}}",
        packet_policy="complete_pdf",
        privacy_policy="full",
        office_level="lower_office" if direction == "downward" else "higher_office",
        allowed_directions=[direction],
        max_packet_bytes=15 * 1024 * 1024,
        require_approval=True,
        messages_per_minute=20,
        max_retries=5,
        enabled=True,
        actor="test",
    )


def test_downward_request_uses_pending_case_and_assignment_packet(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        case = seed_pending_case(session)
        profile = seed_profile(session, service, direction="downward")
        service.save_rule(
            rule_id=None,
            name="Model Town compliance requests",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={
                "tehsils": ["Model Town"],
                "directions": ["downward"],
                "purposes": ["compliance_request"],
            },
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )

        result = service.create_batch(
            direction="downward",
            case_ids=[case.id],
            purpose="compliance_request",
            actor="test",
        )
        assert result["batch"]["direction"] == "downward"
        assert result["batch"]["source_mode"] == "complaint_cases"
        item = result["items"][0]
        assert item["official_letter_id"] is None
        assert item["packet_artifact_id"]
        artifact = session.get(CrmDispatchArtifact, uuid.UUID(item["packet_artifact_id"]))
        assert artifact is not None
        assert artifact.kind == "assignment_packet"
        assert Path(artifact.path).is_file()
        assert Path(artifact.path).read_bytes().startswith(b"%PDF")

        compiled = service.compile_previews(uuid.UUID(result["batch"]["id"]), actor="test")
        assert compiled["previews"]
        delivery = session.scalar(select(WhatsAppDispatchPreviewDelivery))
        assert delivery is not None
        assert case.complaint_number in delivery.message
        assert "compliance request" in delivery.message


def test_direction_restricted_profile_cannot_be_used_for_downward_request(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        case = seed_pending_case(session)
        profile = seed_profile(session, service, direction="upward")
        service.save_rule(
            rule_id=None,
            name="Incorrect higher-office rule",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"categories": ["Administrative"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        batch = service.create_batch(
            direction="downward", case_ids=[case.id], actor="test",
        )
        assert batch["batch"]["status"] == "review_required"
        assert batch["items"][0]["route_status"] == "needs_review"
        assert "not enabled" in batch["items"][0]["route_summary"]["reason"]
        with pytest.raises(CrmDispatchValidationError, match="not enabled"):
            service.set_manual_route(
                item_id=uuid.UUID(batch["items"][0]["id"]),
                profile_ids=[uuid.UUID(profile["id"])],
                reason="Manual test",
                actor="test",
            )


def test_lower_office_file_is_linked_to_dispatch_and_marks_compliance_received(tmp_path: Path) -> None:
    db = engine()
    cfg = settings(tmp_path)
    with Session(db) as session:
        dispatch = CrmDispatchService(session, cfg)
        case = seed_pending_case(session)
        batch = dispatch.create_batch(
            direction="downward", case_ids=[case.id], actor="test",
        )
        batch_id = uuid.UUID(batch["batch"]["id"])
        item_id = uuid.UUID(batch["items"][0]["id"])
        workspace = ComplaintReplyWorkspaceService(session, cfg, client=object())
        uploaded = workspace.upload_case_document(
            case.id,
            filename="DDEO-report.pdf",
            content=b"%PDF-1.4\n1 0 obj <</Type /Page>> endobj\n%%EOF",
            content_type="application/pdf",
            role="report",
            actor="test",
            dispatch_batch_id=batch_id,
            dispatch_item_id=item_id,
        )
        document = session.get(ComplaintDocument, uuid.UUID(uploaded["id"]))
        assert document is not None
        assert document.source_kind == "manual_upload"
        assert document.source_dispatch_batch_id == batch_id
        assert document.source_dispatch_item_id == item_id
        refreshed = dispatch.refresh(batch_id)
        assert refreshed["items"][0]["compliance_status"] == "received"
        item = session.get(CrmDispatchItem, item_id)
        assert item is not None and item.compliance_status == "received"


def test_routing_test_explains_direction_and_purpose(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        case = seed_pending_case(session)
        profile = seed_profile(session, service, direction="downward")
        service.save_rule(
            rule_id=None,
            name="Evidence route",
            description=None,
            priority=110,
            selection_mode="suggested",
            conditions={"directions": ["downward"], "purposes": ["evidence_request"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        unmatched = service.test_routing(case.id, direction="downward", purpose="compliance_request")
        assert unmatched["selected_profiles"] == []
        matched = service.test_routing(case.id, direction="downward", purpose="evidence_request")
        assert matched["selected_profiles"][0]["name"] == "Lower Office Profile"
        assert matched["context"]["direction"] == "downward"
        assert matched["context"]["purpose"] == "evidence_request"


def test_confirmed_upward_delivery_rolls_up_and_retries_paperless_idempotently(
    tmp_path: Path,
) -> None:
    from unittest.mock import MagicMock

    db = engine()
    cfg = Settings(
        artifact_root=tmp_path / "artifacts",
        object_storage_enabled=False,
        paperless_url="https://paperless.test",
        paperless_token="token",
    )
    paperless = MagicMock()
    paperless.connect.return_value = object()
    paperless.set_document_status.side_effect = [
        RuntimeError("temporary Paperless outage"),
        PaperlessStatusUpdateResult(
            document_id=756,
            status_before="Pending",
            status_after="Submitted",
            changed=True,
        ),
    ]

    with Session(db) as session:
        service = CrmDispatchService(
            session,
            cfg,
            paperless_client_factory=lambda _settings: paperless,
        )
        profile = seed_profile(session, service, direction="upward")
        service.save_rule(
            rule_id=None,
            name="Higher office compliance submission",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"directions": ["upward"], "purposes": ["compliance_submission"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        letter = seed_letter(session, tmp_path)
        case = session.get(ComplaintCase, letter.complaint_case_id)
        assert case is not None
        case.canonical_paperless_document_id = 756
        session.add(case)
        session.commit()
        created = service.create_batch(
            direction="upward",
            official_letter_ids=[letter.id],
            purpose="compliance_submission",
            actor="test",
        )
        batch_id = uuid.UUID(created["batch"]["id"])
        compiled = service.compile_previews(batch_id, actor="test")
        preview = session.get(
            WhatsAppDispatchPreview,
            uuid.UUID(compiled["previews"][0]["id"]),
        )
        frozen = session.scalar(
            select(WhatsAppDispatchPreviewDelivery).where(
                WhatsAppDispatchPreviewDelivery.preview_id == preview.id
            )
        )
        assert preview is not None and frozen is not None
        job = add_job(
            session,
            job_type=JobType.whatsapp_dispatch_send.value,
            title="Send CRM compliance",
            parameters={},
        )
        approval = WhatsAppDispatchApproval(
            preview_id=preview.id,
            job_id=job.id,
            preview_content_sha256=preview.content_sha256,
            approved_content_sha256=preview.content_sha256,
            delivery_count=1,
            status="completed",
        )
        session.add(approval)
        session.flush()
        account = service.ensure_defaults()["account"]
        session.add(
            WhatsAppDelivery(
                approval_id=approval.id,
                preview_delivery_id=frozen.id,
                account_id=account.id,
                recipient_type=frozen.target_type,
                recipient_name=frozen.target_name,
                target=frozen.target_jid,
                message=frozen.message,
                status="delivered",
                status_subject=f"whatsapp.status.platform.{uuid.uuid4().hex}",
                completed_at=frozen.created_at,
            )
        )
        session.commit()

        refreshed = service.refresh(batch_id)

        assert refreshed["batch"]["status"] == "completed"
        assert refreshed["batch"]["successful_items"] == 1
        assert refreshed["items"][0]["compliance_status"] == "submitted"
        assert refreshed["items"][0]["paperless_sync"]["state"] == "failed"
        assert refreshed["reconciliation"]["failed"] == 1
        sync = session.scalar(select(CrmPaperlessStatusSync))
        assert sync is not None and sync.attempts == 1

        repaired = service.refresh(batch_id)
        assert repaired["items"][0]["paperless_sync"]["state"] == "succeeded"
        assert repaired["items"][0]["paperless_sync"]["status_after"] == "Submitted"
        assert repaired["reconciliation"]["synchronized"] == 1
        assert session.scalar(select(CrmPaperlessStatusSync)).attempts == 2
        match = session.scalar(
            select(ComplaintMatch).order_by(ComplaintMatch.created_at.desc())
        )
        assert match is not None
        assert match.signals_json["paperless_statuses"] == ["Submitted"]
        assert session.scalar(select(ComplaintAuditEvent)) is not None

        repeated = service.refresh(batch_id)
        assert repeated["items"][0]["paperless_sync"]["state"] == "succeeded"
        assert session.scalar(select(CrmPaperlessStatusSync)).attempts == 2
        assert paperless.set_document_status.call_count == 2
        paperless.set_document_status.assert_called_with(
            756,
            "Submitted",
            allowed_from_statuses={"Pending"},
        )
