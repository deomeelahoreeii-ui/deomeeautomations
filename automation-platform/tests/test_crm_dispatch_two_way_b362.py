from __future__ import annotations

import uuid
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

from automation_core.config import Settings
from crm_domain.dispatch import CrmDispatchService, CrmDispatchValidationError
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    CrmDispatchArtifact,
    CrmDispatchBatch,
    CrmDispatchItem,
)
from crm_domain.reply_workspace import ComplaintReplyWorkspaceService
from whatsapp_gateway.models import WhatsAppDirectoryContact, WhatsAppDispatchPreviewDelivery


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
