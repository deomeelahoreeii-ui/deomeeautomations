from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

from crm_domain.dispatch import CrmDispatchService
from crm_domain.models import (
    ComplaintCase,
    ComplaintReplyRevision,
    CrmDispatchBatch,
    CrmDispatchItem,
    CrmDispatchTarget,
    CrmOfficialLetter,
    CrmOfficialLetterArtifact,
    CrmOfficialLetterSignatureProfile,
    CrmOfficialLetterTemplate,
)
from whatsapp_gateway.models import (
    WhatsAppDirectoryContact,
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


def seed_letter(session: Session, tmp_path: Path) -> CrmOfficialLetter:
    case = ComplaintCase(
        complaint_number="104-7000001",
        state="published",
        remarks="Private school fee complaint.",
        category="Fee",
        sub_category="Summer Vacations",
        district="Lahore",
        tehsil="Model Town",
    )
    session.add(case)
    session.flush()
    revision = ComplaintReplyRevision(
        complaint_case_id=case.id,
        reply_text="Respected Worthy Chief Executive Officer (DEA),\n\nApproved reply.",
        content_hash="a" * 64,
        approval_status="Approved",
    )
    template_file = tmp_path / "template.odt"
    template_file.write_bytes(b"odt")
    signature_file = tmp_path / "signature.png"
    signature_file.write_bytes(b"png")
    template = CrmOfficialLetterTemplate(
        name="Template",
        file_path=str(template_file),
        sha256="b" * 64,
    )
    signature = CrmOfficialLetterSignatureProfile(
        name="DEO",
        image_path=str(signature_file),
        image_sha256="c" * 64,
    )
    session.add_all([revision, template, signature])
    session.flush()
    letter = CrmOfficialLetter(
        complaint_case_id=case.id,
        reply_revision_id=revision.id,
        template_id=template.id,
        signature_profile_id=signature.id,
        letter_number="1600/PMDU/CRM",
        letter_date=date(2026, 7, 21),
        recipient_name="The Chief Executive Officer (DEA),",
        recipient_location="Lahore",
        status="finalized",
        complaint_number_snapshot=case.complaint_number,
        complaint_text_snapshot=case.remarks or "",
        reply_text_snapshot=revision.reply_text,
    )
    session.add(letter)
    session.flush()
    packet = tmp_path / "complete.pdf"
    packet.write_bytes(b"%PDF-1.4\n1 0 obj <</Type /Page>> endobj\n%%EOF")
    import hashlib

    content = packet.read_bytes()
    artifact = CrmOfficialLetterArtifact(
        official_letter_id=letter.id,
        kind="complete_pdf",
        name="complete.pdf",
        path=str(packet),
        content_type="application/pdf",
        size_bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
    )
    session.add(artifact)
    session.commit()
    return letter


def test_dispatch_profile_rule_batch_and_generic_preview(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session)
        defaults = service.ensure_defaults()
        contact = WhatsAppDirectoryContact(
            account_id=defaults["account"].id,
            canonical_key="923001234567",
            phone_jid="923001234567@c.us",
            display_name="DDEO Model Town",
            active=True,
        )
        session.add(contact)
        session.commit()
        profile = service.save_profile(
            profile_id=None,
            name="DDEO Model Town — CRM Complaints",
            target_type="contact",
            target_ids=[contact.id],
            template_body="Complaint {{complaint_number}} · letter {{letter_number}}",
            packet_policy="complete_pdf",
            privacy_policy="full",
            max_packet_bytes=15 * 1024 * 1024,
            require_approval=True,
            messages_per_minute=20,
            max_retries=5,
            enabled=True,
            actor="test",
        )
        service.save_rule(
            rule_id=None,
            name="Model Town fee complaints",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"categories": ["Fee"], "tehsils": ["Model Town"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        letter = seed_letter(session, tmp_path)
        result = service.create_batch(
            official_letter_ids=[letter.id],
            actor="test",
        )
        assert result["batch"]["status"] == "ready"
        assert result["items"][0]["route_status"] == "ready"
        compiled = service.compile_previews(uuid.UUID(result["batch"]["id"]), actor="test")
        assert compiled["previews"]
        preview_id = uuid.UUID(compiled["previews"][0]["id"])
        preview = session.get(WhatsAppDispatchPreview, preview_id)
        assert preview is not None
        assert preview.source_job_id is None
        assert preview.source_kind == "crm_dispatch_batch"
        assert preview.source_reference_id == uuid.UUID(result["batch"]["id"])
        delivery = session.scalar(
            select(WhatsAppDispatchPreviewDelivery).where(
                WhatsAppDispatchPreviewDelivery.preview_id == preview.id
            )
        )
        assert delivery is not None
        assert delivery.target_jid == "923001234567@c.us"
        assert "104-7000001" in delivery.message
        assert len(delivery.attachment_ids) == 1


def test_no_route_is_review_required(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session)
        service.ensure_defaults()
        letter = seed_letter(session, tmp_path)
        result = service.create_batch(official_letter_ids=[letter.id], actor="test")
        assert result["batch"]["status"] == "review_required"
        assert result["items"][0]["route_status"] == "needs_review"
        assert session.scalar(select(CrmDispatchBatch)) is not None
        assert session.scalar(select(CrmDispatchItem)) is not None
        assert session.scalar(select(CrmDispatchTarget)) is None


def test_routing_test_is_explainable_and_manual_override_requires_reason(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session)
        defaults = service.ensure_defaults()
        contact = WhatsAppDirectoryContact(
            account_id=defaults["account"].id,
            canonical_key="923009999999",
            phone_jid="923009999999@c.us",
            display_name="CRM Routing Officer",
            active=True,
        )
        session.add(contact)
        session.commit()
        profile = service.save_profile(
            profile_id=None,
            name="CRM Routing Officer",
            target_type="contact",
            target_ids=[contact.id],
            template_body="Complaint {{complaint_number}}",
            packet_policy="complete_pdf",
            privacy_policy="full",
            max_packet_bytes=15 * 1024 * 1024,
            require_approval=True,
            messages_per_minute=20,
            max_retries=5,
            enabled=True,
            actor="test",
        )
        service.save_rule(
            rule_id=None,
            name="Fee route",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"categories": ["Fee"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        letter = seed_letter(session, tmp_path)
        test_result = service.test_routing(letter.complaint_case_id)
        assert test_result["conflict"] is False
        assert test_result["selected_profiles"][0]["name"] == "CRM Routing Officer"
        assert "priority 100" in test_result["explanation"]
        batch = service.create_batch(official_letter_ids=[letter.id], actor="test")
        item_id = uuid.UUID(batch["items"][0]["id"])
        import pytest
        from crm_domain.dispatch import CrmDispatchValidationError
        with pytest.raises(CrmDispatchValidationError, match="reason"):
            service.set_manual_route(
                item_id=item_id,
                profile_ids=[uuid.UUID(profile["id"])],
                reason="",
                actor="test",
            )


def test_confidential_complaint_requires_restricted_destination(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session)
        defaults = service.ensure_defaults()
        contact = WhatsAppDirectoryContact(
            account_id=defaults["account"].id,
            canonical_key="923008888888",
            phone_jid="923008888888@c.us",
            display_name="General CRM Group Admin",
            active=True,
        )
        session.add(contact)
        session.commit()
        profile = service.save_profile(
            profile_id=None,
            name="General CRM Destination",
            target_type="contact",
            target_ids=[contact.id],
            template_body="Complaint {{complaint_number}}",
            packet_policy="complete_pdf",
            privacy_policy="full",
            max_packet_bytes=15 * 1024 * 1024,
            require_approval=True,
            messages_per_minute=20,
            max_retries=5,
            enabled=True,
            actor="test",
        )
        letter = seed_letter(session, tmp_path)
        case = session.get(ComplaintCase, letter.complaint_case_id)
        assert case is not None
        from crm_domain.models import CrmComplaintTag, CrmComplaintTagLink
        tag = CrmComplaintTag(
            name="confidentiality-requested",
            display_name="Confidentiality requested",
            normalized_name="confidentiality-requested",
            group_name="handling",
            active=True,
        )
        session.add(tag)
        session.flush()
        session.add(CrmComplaintTagLink(complaint_case_id=case.id, tag_id=tag.id))
        session.commit()
        service.save_rule(
            rule_id=None,
            name="Confidential fee route",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"categories": ["Fee"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        batch = service.create_batch(official_letter_ids=[letter.id], actor="test")
        compiled = service.compile_previews(uuid.UUID(batch["batch"]["id"]), actor="test")
        assert compiled["batch"]["status"] == "review_required"
        assert compiled["items"][0]["targets"][0]["business_status"] == "blocked"
        assert "restricted destination" in compiled["items"][0]["targets"][0]["error"]
