from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pytest
from fastapi import HTTPException
from sqlalchemy import event, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine

from automation_core.config import Settings
from automation_core.job_service import add_job
from automation_core.models import Job, JobStatus, JobType, TaskOutbox
from automation_core.time import utcnow
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
    CrmUpwardSubmissionClaim,
    CrmDispatchTarget,
)
from crm_filters.paperless import PaperlessStatusUpdateResult
from crm_domain.reply_workspace import ComplaintReplyWorkspaceService
from crm_domain.repairs import audit_upward_submission_claim_backfill
from whatsapp_gateway.models import (
    WhatsAppActivity,
    WhatsAppDelivery,
    WhatsAppDirectoryContact,
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewDelivery,
)
from crm_domain.dispatch_api import ActorInput, retry_failed_upward_deliveries
from whatsapp_gateway.previews.deletion import hard_delete_previews_bulk
from whatsapp_gateway.previews.schemas import PreviewIdsInput


def engine():
    db = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(db)
    return db


def foreign_key_engine():
    db = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(db, "connect")
    def enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

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


def test_crm_recompile_detaches_target_before_deleting_old_preview(
    tmp_path: Path,
) -> None:
    db = foreign_key_engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        case = seed_pending_case(session, "104-7100091")
        profile = seed_profile(session, service, direction="downward")
        service.save_rule(
            rule_id=None,
            name="Foreign-key-safe recompile",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"directions": ["downward"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        created = service.create_batch(
            direction="downward",
            case_ids=[case.id],
            purpose="compliance_request",
            actor="test",
        )
        batch_id = uuid.UUID(created["batch"]["id"])
        service.compile_previews(batch_id, actor="test")
        target = session.scalar(select(CrmDispatchTarget))
        assert target is not None and target.preview_id is not None
        old_preview_id = target.preview_id

        service.compile_previews(batch_id, actor="test")

        session.refresh(target)
        assert session.get(WhatsAppDispatchPreview, old_preview_id) is None
        assert target.preview_id is not None
        assert target.preview_id != old_preview_id


def test_bulk_delete_rejects_crm_owned_preview_before_any_mutation(
    tmp_path: Path,
) -> None:
    db = foreign_key_engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        case = seed_pending_case(session, "104-7100092")
        profile = seed_profile(session, service, direction="downward")
        service.save_rule(
            rule_id=None,
            name="Protected preview deletion",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"directions": ["downward"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        created = service.create_batch(
            direction="downward",
            case_ids=[case.id],
            purpose="compliance_request",
            actor="test",
        )
        batch_id = uuid.UUID(created["batch"]["id"])
        service.compile_previews(batch_id, actor="test")
        target = session.scalar(select(CrmDispatchTarget))
        assert target is not None and target.preview_id is not None
        owned = session.get(WhatsAppDispatchPreview, target.preview_id)
        assert owned is not None
        standalone = WhatsAppDispatchPreview(
            preview_key=f"AD-{uuid.uuid4()}",
            application_id=owned.application_id,
            source_kind="manual_preview",
            dispatch_profile_id=owned.dispatch_profile_id,
            status="ready",
            profile_version=owned.profile_version,
            application_name=owned.application_name,
            report_type_name=owned.report_type_name,
            audience_name=owned.audience_name,
            profile_name=owned.profile_name,
            account_name=owned.account_name,
        )
        session.add(standalone)
        session.commit()

        with pytest.raises(HTTPException) as caught:
            hard_delete_previews_bulk(
                PreviewIdsInput(preview_ids=[standalone.id, owned.id]),
                session,
            )

        assert caught.value.status_code == 409
        assert "owned by their CRM batch" in str(caught.value.detail)
        assert session.get(WhatsAppDispatchPreview, owned.id) is not None
        assert session.get(WhatsAppDispatchPreview, standalone.id) is not None


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
            direction="downward",
            case_ids=[case.id],
            actor="test",
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


def test_lower_office_file_is_linked_to_dispatch_and_marks_compliance_received(
    tmp_path: Path,
) -> None:
    db = engine()
    cfg = settings(tmp_path)
    with Session(db) as session:
        dispatch = CrmDispatchService(session, cfg)
        case = seed_pending_case(session)
        batch = dispatch.create_batch(
            direction="downward",
            case_ids=[case.id],
            actor="test",
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


def test_refresh_maps_only_each_targets_exact_frozen_deliveries(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        first_case = seed_pending_case(session, "104-7199901")
        second_case = seed_pending_case(session, "104-7199902")
        profile = seed_profile(session, service, direction="downward")
        service.save_rule(
            rule_id=None,
            name="Exact delivery ownership",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"directions": ["downward"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        created = service.create_batch(
            direction="downward",
            case_ids=[first_case.id, second_case.id],
            actor="test",
        )
        batch_id = uuid.UUID(created["batch"]["id"])
        service.compile_previews(batch_id, actor="test")

        targets = list(
            session.scalars(
                select(CrmDispatchTarget)
                .join(
                    CrmDispatchItem,
                    CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id,
                )
                .where(CrmDispatchItem.batch_id == batch_id)
                .order_by(CrmDispatchTarget.id)
            ).all()
        )
        assert len(targets) == 2
        assert all(len(target.preview_delivery_ids_json) == 1 for target in targets)
        assert targets[0].preview_id == targets[1].preview_id

        preview = session.get(WhatsAppDispatchPreview, targets[0].preview_id)
        assert preview is not None
        account = service.ensure_defaults()["account"]
        job = add_job(
            session,
            job_type=JobType.whatsapp_dispatch_send.value,
            title="Exact ownership dispatch",
            parameters={"preview_id": str(preview.id)},
        )
        approval = WhatsAppDispatchApproval(
            preview_id=preview.id,
            job_id=job.id,
            approved_by="test",
            preview_content_sha256=preview.content_sha256,
            approved_content_sha256=preview.content_sha256,
            delivery_count=2,
            status="failed",
        )
        session.add(approval)
        session.flush()

        delivery_ids: list[uuid.UUID] = []
        for target, status_value in zip(
            targets, ["delivered", "failed"], strict=True
        ):
            preview_delivery_id = uuid.UUID(target.preview_delivery_ids_json[0])
            frozen = session.get(WhatsAppDispatchPreviewDelivery, preview_delivery_id)
            assert frozen is not None
            delivery = WhatsAppDelivery(
                approval_id=approval.id,
                preview_delivery_id=frozen.id,
                account_id=account.id,
                recipient_type=frozen.target_type,
                recipient_name=frozen.target_name,
                target=frozen.target_jid,
                message=frozen.message,
                status_subject=f"whatsapp.status.test.{uuid.uuid4().hex}",
                status=status_value,
                error=(
                    "gateway rejected delivery" if status_value == "failed" else None
                ),
                completed_at=utcnow(),
            )
            session.add(delivery)
            session.flush()
            delivery_ids.append(delivery.id)
        session.commit()

        refreshed = service.refresh(batch_id)

        persisted_targets = list(
            session.scalars(
                select(CrmDispatchTarget)
                .join(
                    CrmDispatchItem,
                    CrmDispatchItem.id == CrmDispatchTarget.dispatch_item_id,
                )
                .where(CrmDispatchItem.batch_id == batch_id)
                .order_by(CrmDispatchTarget.id)
            ).all()
        )
        assert persisted_targets[0].whatsapp_delivery_ids_json == [
            str(delivery_ids[0])
        ]
        assert persisted_targets[1].whatsapp_delivery_ids_json == [
            str(delivery_ids[1])
        ]
        assert [target.business_status for target in persisted_targets] == [
            "delivered",
            "failed",
        ]
        assert refreshed["batch"]["status"] == "completed_with_errors"


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
        unmatched = service.test_routing(
            case.id, direction="downward", purpose="compliance_request"
        )
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
        match = session.scalar(select(ComplaintMatch).order_by(ComplaintMatch.created_at.desc()))
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


def test_upward_claim_removes_packet_from_queue_and_blocks_duplicate_batch(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        letter = seed_letter(session, tmp_path)

        before = service.eligible_sources(direction="upward")
        assert before["total"] == 1
        assert service.statistics()["ready_upward"] == 1

        created = service.create_batch(
            direction="upward",
            official_letter_ids=[letter.id],
            actor="test",
        )
        assert created["items"][0]["submission_claim"]["status"] == "reserved"
        assert service.eligible_sources(direction="upward")["total"] == 0
        assert service.statistics()["ready_upward"] == 0
        assert service.statistics()["reserved_upward"] == 1

        with pytest.raises(
            CrmDispatchValidationError, match="already in dispatch batch"
        ) as conflict:
            service.create_batch(
                direction="upward",
                official_letter_ids=[letter.id],
                actor="duplicate-test",
            )
        assert conflict.value.detail["existing_batch_number"] == created["batch"]["batch_number"]
        assert conflict.value.detail["existing_batch_url"].endswith(f"/{created['batch']['id']}/")
        assert len(session.exec(select(CrmDispatchBatch)).all()) == 1
        assert len(session.exec(select(CrmDispatchItem)).all()) == 1
        assert len(session.exec(select(CrmUpwardSubmissionClaim)).all()) == 1


def test_discarding_unapproved_upward_batch_releases_packet_with_audit(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        letter = seed_letter(session, tmp_path)
        created = service.create_batch(
            direction="upward", official_letter_ids=[letter.id], actor="test"
        )

        discarded = service.discard_upward_batch(
            uuid.UUID(created["batch"]["id"]), actor="release-test"
        )

        assert discarded["batch"]["status"] == "cancelled"
        assert discarded["items"][0]["submission_claim"]["status"] == "released"
        assert service.eligible_sources(direction="upward")["total"] == 1
        event = session.scalar(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.event_type == "upward_submission_released"
            )
        )
        assert event is not None
        assert event.actor == "release-test"

        replacement = service.create_batch(
            direction="upward", official_letter_ids=[letter.id], actor="test"
        )
        assert replacement["batch"]["id"] != created["batch"]["id"]


def test_attempted_upward_packet_stays_out_of_queue_and_is_grouped_by_batch(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        letter = seed_letter(session, tmp_path)
        profile = seed_profile(session, service, direction="upward")
        service.save_rule(
            rule_id=None,
            name="Higher-office submission route",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"directions": ["upward"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        created = service.create_batch(
            direction="upward", official_letter_ids=[letter.id], actor="test"
        )
        item = session.get(CrmDispatchItem, uuid.UUID(created["items"][0]["id"]))
        assert item is not None
        target = session.scalar(
            select(CrmDispatchTarget).where(CrmDispatchTarget.dispatch_item_id == item.id)
        )
        assert target is not None
        target.business_status = "delivered"
        target.sent_at = utcnow()
        target.completed_at = utcnow()
        session.add(target)
        session.commit()

        refreshed = service.refresh(uuid.UUID(created["batch"]["id"]))
        assert refreshed["batch"]["status"] == "completed"
        assert refreshed["items"][0]["submission_claim"]["status"] == "sent"
        assert service.eligible_sources(direction="upward")["total"] == 0
        with pytest.raises(CrmDispatchValidationError, match="cannot be returned"):
            service.discard_upward_batch(uuid.UUID(created["batch"]["id"]), actor="test")

        sent = service.list_upward_submissions(phase="sent")
        assert sent["total"] == 1
        assert sent["items"][0]["batch_number"] == created["batch"]["batch_number"]
        assert sent["items"][0]["packet_count"] == 1
        assert sent["items"][0]["target_counts"] == {"delivered": 1}


def test_failed_attempt_is_not_reintroduced_as_a_new_submission(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        letter = seed_letter(session, tmp_path)
        profile = seed_profile(session, service, direction="upward")
        service.save_rule(
            rule_id=None,
            name="Failure route",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"directions": ["upward"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        created = service.create_batch(
            direction="upward", official_letter_ids=[letter.id], actor="test"
        )
        target = session.scalar(select(CrmDispatchTarget))
        assert target is not None
        target.business_status = "failed"
        target.sent_at = utcnow()
        target.error = "gateway rejected delivery"
        session.add(target)
        session.commit()

        refreshed = service.refresh(uuid.UUID(created["batch"]["id"]))
        assert refreshed["batch"]["status"] == "failed"
        assert service.eligible_sources(direction="upward")["total"] == 0
        assert service.list_upward_submissions(phase="attention")["total"] == 1
        assert service.list_upward_submissions(phase="sent")["total"] == 1


def test_failed_retry_creates_immutable_attempt_job_and_preserves_failure_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import automation_core.task_outbox as task_outbox

    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        letter = seed_letter(session, tmp_path)
        profile = seed_profile(session, service, direction="upward")
        service.save_rule(
            rule_id=None,
            name="Retry audit route",
            description=None,
            priority=100,
            selection_mode="suggested",
            conditions={"directions": ["upward"]},
            profile_ids=[uuid.UUID(profile["id"])],
            stop_after_match=True,
            enabled=True,
            actor="test",
        )
        created = service.create_batch(
            direction="upward",
            official_letter_ids=[letter.id],
            actor="test",
        )
        batch_id = uuid.UUID(created["batch"]["id"])
        service.compile_previews(batch_id, actor="test")
        target = session.scalar(select(CrmDispatchTarget))
        assert target is not None and len(target.preview_delivery_ids_json) == 1
        preview = session.get(WhatsAppDispatchPreview, target.preview_id)
        frozen = session.get(
            WhatsAppDispatchPreviewDelivery,
            uuid.UUID(target.preview_delivery_ids_json[0]),
        )
        assert preview is not None and frozen is not None

        original_job = add_job(
            session,
            job_type=JobType.whatsapp_dispatch_send.value,
            title="Original failed dispatch",
            parameters={"approval_id": "pending"},
        )
        original_job.status = JobStatus.succeeded.value
        original_job.result = {"delivered": 0, "failed": 1}
        approval = WhatsAppDispatchApproval(
            preview_id=preview.id,
            job_id=original_job.id,
            approved_by="test",
            preview_content_sha256=preview.content_sha256,
            approved_content_sha256=preview.content_sha256,
            delivery_count=1,
            status="failed",
            error="1 delivery failed",
            completed_at=utcnow(),
        )
        session.add(approval)
        session.flush()
        original_job.parameters = {"approval_id": str(approval.id)}
        account = service.ensure_defaults()["account"]
        delivery = WhatsAppDelivery(
            approval_id=approval.id,
            preview_delivery_id=frozen.id,
            account_id=account.id,
            recipient_type=frozen.target_type,
            recipient_name=frozen.target_name,
            target=frozen.target_jid,
            message=frozen.message,
            status_subject=f"whatsapp.status.test.{uuid.uuid4().hex}",
            status="failed",
            error="Connection Closed",
            provider_result={"status": "failed", "error": "Connection Closed"},
            completed_at=utcnow(),
        )
        session.add_all([original_job, delivery])
        session.commit()
        original_job_id = original_job.id

        monkeypatch.setattr(
            task_outbox,
            "publish_pending_tasks",
            lambda _session, limit=10: {
                "published": 0,
                "failed": 0,
                "cancelled": 0,
            },
        )
        result = retry_failed_upward_deliveries(
            batch_id,
            ActorInput(actor="retry-test"),
            session,
        )

        session.expire_all()
        persisted_original = session.get(Job, original_job_id)
        retry_job = session.get(Job, uuid.UUID(result["staged"][0]["job_id"]))
        persisted_delivery = session.get(WhatsAppDelivery, delivery.id)
        audit = session.scalar(
            select(WhatsAppActivity).where(
                WhatsAppActivity.event_type
                == "approved_delivery_retry_requested"
            )
        )
        outbox = session.scalar(
            select(TaskOutbox).where(TaskOutbox.job_id == retry_job.id)
        )

        assert persisted_original is not None
        assert persisted_original.status == JobStatus.succeeded.value
        assert persisted_original.result == {"delivered": 0, "failed": 1}
        assert retry_job is not None and retry_job.id != persisted_original.id
        assert retry_job.parameters["retry_of_job_id"] == str(persisted_original.id)
        assert retry_job.parameters["retry_delivery_ids"] == [str(delivery.id)]
        assert persisted_delivery is not None and persisted_delivery.status == "queued"
        assert audit is not None
        assert audit.details["previous_status"] == "failed"
        assert audit.details["previous_error"] == "Connection Closed"
        assert outbox is not None and outbox.args == [str(retry_job.id)]


def test_new_official_letter_revision_is_independently_eligible(tmp_path: Path) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        first = seed_letter(session, tmp_path)
        service.create_batch(direction="upward", official_letter_ids=[first.id], actor="test")
        first.status = "superseded"
        session.add(first)
        second = CrmOfficialLetter(
            complaint_case_id=first.complaint_case_id,
            reply_revision_id=first.reply_revision_id,
            template_id=first.template_id,
            signature_profile_id=first.signature_profile_id,
            letter_number="1701/PMDU/CRM",
            letter_date=first.letter_date,
            recipient_name=first.recipient_name,
            recipient_location=first.recipient_location,
            status="finalized",
            revision=2,
            supersedes_letter_id=first.id,
            complaint_number_snapshot=first.complaint_number_snapshot,
            complaint_text_snapshot=first.complaint_text_snapshot,
            reply_text_snapshot=first.reply_text_snapshot,
        )
        session.add(second)
        session.flush()
        first_artifact = session.scalar(
            select(CrmOfficialLetterArtifact).where(
                CrmOfficialLetterArtifact.official_letter_id == first.id,
                CrmOfficialLetterArtifact.kind == "complete_pdf",
            )
        )
        assert first_artifact is not None
        session.add(
            CrmOfficialLetterArtifact(
                official_letter_id=second.id,
                kind="complete_pdf",
                name="revision-2.pdf",
                path=first_artifact.path,
                content_type="application/pdf",
                size_bytes=first_artifact.size_bytes,
                sha256=first_artifact.sha256,
            )
        )
        session.commit()

        available = service.eligible_sources(direction="upward")
        assert available["total"] == 1
        assert available["items"][0]["id"] == str(second.id)
        created = service.create_batch(
            direction="upward", official_letter_ids=[second.id], actor="test"
        )
        assert created["items"][0]["official_letter_id"] == str(second.id)


def test_database_claim_constraint_closes_concurrent_precheck_race(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        letter = seed_letter(session, tmp_path)
        first = service.create_batch(
            direction="upward", official_letter_ids=[letter.id], actor="winner"
        )
        first_item = session.get(CrmDispatchItem, uuid.UUID(first["items"][0]["id"]))
        assert first_item is not None

        competing_batch = CrmDispatchBatch(
            batch_number="CRM-DSP-CONCURRENT-LOSER",
            status="resolving_routes",
            direction="upward",
            source_mode="official_letters",
            purpose="compliance_submission",
            total_items=1,
            created_by="loser",
        )
        session.add(competing_batch)
        session.flush()
        competing_item = CrmDispatchItem(
            batch_id=competing_batch.id,
            complaint_case_id=letter.complaint_case_id,
            official_letter_id=letter.id,
            complaint_number_snapshot=letter.complaint_number_snapshot,
            letter_number_snapshot=letter.letter_number,
            letter_date_snapshot=letter.letter_date,
            packet_sha256=first_item.packet_sha256,
            compliance_status="incorporated",
        )
        session.add(competing_item)
        session.flush()
        session.add(
            CrmUpwardSubmissionClaim(
                official_letter_id=letter.id,
                dispatch_item_id=competing_item.id,
                claimed_by="loser",
            )
        )

        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

        claims = session.exec(
            select(CrmUpwardSubmissionClaim).where(
                CrmUpwardSubmissionClaim.official_letter_id == letter.id,
                CrmUpwardSubmissionClaim.released_at.is_(None),
            )
        ).all()
        assert len(claims) == 1


def test_claim_backfill_audit_repair_is_dry_run_first_and_idempotent(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        service = CrmDispatchService(session, settings(tmp_path))
        letter = seed_letter(session, tmp_path)
        service.create_batch(direction="upward", official_letter_ids=[letter.id], actor="test")
        claim = session.scalar(select(CrmUpwardSubmissionClaim))
        assert claim is not None
        claim.claimed_by = "dispatch-claim-backfill"
        session.add(claim)
        session.commit()

        dry_run = audit_upward_submission_claim_backfill(session)
        assert dry_run["candidate_count"] == 1
        assert (
            session.scalar(
                select(ComplaintAuditEvent).where(
                    ComplaintAuditEvent.event_type == "upward_submission_claim_backfilled"
                )
            )
            is None
        )

        applied = audit_upward_submission_claim_backfill(session, apply=True, actor="repair-test")
        assert applied["candidate_count"] == 1
        event = session.scalar(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.event_type == "upward_submission_claim_backfilled"
            )
        )
        assert event is not None and event.actor == "repair-test"
        assert audit_upward_submission_claim_backfill(session)["candidate_count"] == 0
