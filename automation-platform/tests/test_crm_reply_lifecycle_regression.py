from __future__ import annotations

import uuid
from pathlib import Path

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401
from automation_core.config import Settings
from crm_domain.bulk_operations import CrmBulkOperationService
from crm_domain.ai_pipeline import CrmAiPipelineService
from crm_domain.case_scopes import (
    downward_dispatch_case_eligibility_clause,
    is_reply_actionable,
    is_reply_awaiting,
    is_reply_case_eligible,
    reply_actionable_clause,
    reply_awaiting_clause,
    reply_case_eligibility_clause,
)
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintReply,
    CrmBulkOperationBatch,
    CrmBulkOperationItem,
    CrmDispatchBatch,
    CrmDispatchItem,
    CrmDispatchTarget,
)
from crm_domain.registry import record_paperless_match
from crm_domain.repairs import repair_reply_import_case_states
from crm_domain.reply_workspace import ComplaintReplyWorkspaceService


class NoopHelpdeskClient:
    pass


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


def test_paperless_reconciliation_cannot_regress_a_published_case() -> None:
    db = engine()
    with Session(db) as session:
        case = ComplaintCase(
            complaint_number="104-7000001",
            state="published",
            registry_status="active",
            canonical_paperless_document_id=100,
            frappe_ticket_id="HD-1",
            frappe_sync_status="synchronized",
        )
        session.add(case)
        session.commit()

        record_paperless_match(
            session,
            complaint_case=case,
            processing_item_id=uuid.uuid4(),
            category="uploaded_pending",
            reason="Paperless still contains the active complaint.",
            document_ids=[100],
            statuses=["Pending"],
        )
        session.commit()
        session.refresh(case)

        assert case.state == "published"
        assert case.canonical_paperless_document_id == 100
        assert not session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.event_type == "paperless_match_state_transition"
            )
        ).all()


def test_reply_queue_and_batch_progress_survive_paperless_existing_state(
    tmp_path: Path,
) -> None:
    db = engine()
    cfg = settings(tmp_path)
    with Session(db) as session:
        import_batch = CrmBulkOperationBatch(
            operation_type="reply_import",
            batch_number="CRM-IMP-TEST-32",
            status="completed",
            total_items=32,
            valid_items=32,
            successful_items=32,
        )
        dispatch_batch = CrmDispatchBatch(
            batch_number="CRM-DSP-TEST-15",
            status="completed",
            direction="upward",
            total_items=15,
            successful_items=15,
        )
        session.add(import_batch)
        session.add(dispatch_batch)
        session.flush()

        for index in range(32):
            issued = index < 15
            case = ComplaintCase(
                complaint_number=f"104-{7000100 + index:07d}",
                state="existing" if index < 29 else "published",
                registry_status="active",
                canonical_paperless_document_id=2000 + index,
                frappe_ticket_id=f"HD-{index + 1}",
                frappe_sync_status="synchronized",
                frappe_reply_approval_status="Issued" if issued else None,
                frappe_reply_text_snapshot=f"Reply {index}" if issued else None,
            )
            session.add(case)
            session.flush()
            reply = ComplaintReply(
                complaint_case_id=case.id,
                reply_text=f"Reply {index}",
                source_filename="completed-replies.csv",
                source_row=index + 2,
                source_kind="frappe_helpdesk" if issued else "bulk_import",
                workspace_status="Issued" if issued else "Imported Draft",
                sync_status="synchronized" if issued else "not_synced",
                source_batch_id=import_batch.id,
            )
            item = CrmBulkOperationItem(
                batch_id=import_batch.id,
                complaint_case_id=case.id,
                complaint_number_snapshot=case.complaint_number or "",
                source_row=index + 2,
                status="imported",
            )
            session.add(reply)
            session.add(item)
            if issued:
                dispatch_item = CrmDispatchItem(
                    batch_id=dispatch_batch.id,
                    complaint_case_id=case.id,
                    complaint_number_snapshot=case.complaint_number or "",
                    packet_sha256=f"{index:064x}",
                    route_status="ready",
                    compliance_status="submitted",
                )
                session.add(dispatch_item)
                session.flush()
                session.add(
                    CrmDispatchTarget(
                        dispatch_item_id=dispatch_item.id,
                        dispatch_profile_id=uuid.uuid4(),
                        selection_source="manual",
                        business_status="delivered",
                    )
                )
        session.commit()

        workspace = ComplaintReplyWorkspaceService(session, cfg, NoopHelpdeskClient())
        actionable = workspace.list_cases(
            scope="actionable",
            source_batch_id=import_batch.id,
            page=1,
            page_size=100,
        )
        all_rows = workspace.list_cases(
            scope="all",
            source_batch_id=import_batch.id,
            page=1,
            page_size=100,
        )
        statistics = workspace.statistics()
        detail = CrmBulkOperationService(session, cfg).batch_detail(import_batch.id)

        assert actionable["total"] == 17
        assert {item["reply_status"] for item in actionable["items"]} == {"Imported Draft"}
        assert all_rows["total"] == 32
        assert statistics["reply_eligible_cases"] == 32
        assert statistics["imported_drafts"] == 17
        assert statistics["issued"] == 15
        assert detail["lifecycle_summary"] == {
            "imported": 32,
            "actionable": 17,
            "issued": 15,
            "delivered": 15,
        }


def test_reply_case_state_repair_is_dry_run_first_audited_and_idempotent(
    tmp_path: Path,
) -> None:
    db = engine()
    with Session(db) as session:
        case = ComplaintCase(
            complaint_number="104-7000999",
            state="existing",
            registry_status="active",
            frappe_ticket_id="HD-999",
        )
        parent = CrmBulkOperationBatch(
            operation_type="reply_context_export",
            batch_number="CRM-CTX-REPAIR",
            status="completed",
        )
        session.add(case)
        session.add(parent)
        session.flush()
        session.add(
            CrmBulkOperationItem(
                batch_id=parent.id,
                complaint_case_id=case.id,
                complaint_number_snapshot=case.complaint_number or "",
                status="exported",
            )
        )
        reply_import = CrmBulkOperationBatch(
            operation_type="reply_import",
            batch_number="CRM-IMP-REPAIR",
            status="completed",
            parent_batch_id=parent.id,
        )
        session.add(reply_import)
        session.flush()
        session.add(
            CrmBulkOperationItem(
                batch_id=reply_import.id,
                complaint_case_id=case.id,
                complaint_number_snapshot=case.complaint_number or "",
                status="imported",
            )
        )
        session.commit()

        dry_run = repair_reply_import_case_states(session, reply_import.id)
        session.refresh(case)
        assert dry_run["candidate_count"] == 1
        assert dry_run["mode"] == "dry_run"
        assert case.state == "existing"

        applied = repair_reply_import_case_states(
            session, reply_import.id, apply=True, actor="test-repair"
        )
        session.refresh(case)
        assert applied["candidate_count"] == 1
        assert case.state == "published"
        audit = session.exec(
            select(ComplaintAuditEvent).where(
                ComplaintAuditEvent.event_type == "reply_case_state_repaired"
            )
        ).one()
        assert audit.actor == "test-repair"
        assert audit.before_json == {"state": "existing"}
        assert audit.after_json == {"state": "published"}

        repeated = repair_reply_import_case_states(session, reply_import.id, apply=True)
        assert repeated["candidate_count"] == 0


def test_reply_lifecycle_predicates_queues_exports_and_overwrite_guards_agree(
    tmp_path: Path,
) -> None:
    db = engine()
    cfg = settings(tmp_path)
    with Session(db) as session:
        cases = {
            name: ComplaintCase(
                complaint_number=f"104-71000{index:02d}",
                state=state,
                registry_status="active",
                remarks=f"Lifecycle fixture {name}.",
                category="Others",
                frappe_ticket_id=ticket,
                frappe_reply_approval_status=reply_status,
                frappe_reply_text_snapshot=reply_text,
            )
            for index, (name, state, ticket, reply_status, reply_text) in enumerate(
                [
                    ("candidate", "candidate", None, None, None),
                    ("review", "review_required", None, None, None),
                    ("existing_bare", "existing", None, None, None),
                    ("fresh", "fresh", None, None, None),
                    ("publishing", "publishing", None, None, None),
                    ("published_awaiting", "published", None, None, None),
                    (
                        "published_approved",
                        "published",
                        None,
                        "Approved",
                        "Approved remote reply.",
                    ),
                    (
                        "existing_issued",
                        "existing",
                        "HD-LIFECYCLE",
                        "Issued",
                        "Issued remote reply.",
                    ),
                    ("existing_local", "existing", None, None, None),
                    ("rejected_ticket", "rejected", "HD-REJECTED", None, None),
                ],
                start=1,
            )
        }
        session.add_all(cases.values())
        session.flush()
        local_reply = ComplaintReply(
            complaint_case_id=cases["existing_local"].id,
            reply_text="Imported local draft.",
            source_filename="lifecycle.csv",
            source_row=2,
            source_kind="bulk_import",
            workspace_status="Imported Draft",
            sync_status="not_synced",
        )
        session.add(local_reply)
        session.commit()

        replies = {local_reply.complaint_case_id: local_reply}
        expected_eligible = {
            "fresh",
            "publishing",
            "published_awaiting",
            "published_approved",
            "existing_issued",
            "existing_local",
        }
        expected_awaiting = {"fresh", "publishing", "published_awaiting"}
        expected_actionable = expected_awaiting | {"existing_local"}

        python_eligible = {
            name
            for name, case in cases.items()
            if is_reply_case_eligible(case, has_local_reply=case.id in replies)
        }
        assert python_eligible == expected_eligible
        assert {
            name
            for name in expected_eligible
            if is_reply_awaiting(cases[name], replies.get(cases[name].id))
        } == expected_awaiting
        assert {
            name
            for name in expected_eligible
            if is_reply_actionable(cases[name], replies.get(cases[name].id))
        } == expected_actionable

        eligible_ids = set(
            session.exec(
                select(ComplaintCase.id).where(reply_case_eligibility_clause())
            ).all()
        )
        awaiting_ids = set(
            session.exec(
                select(ComplaintCase.id)
                .join(
                    ComplaintReply,
                    ComplaintReply.complaint_case_id == ComplaintCase.id,
                    isouter=True,
                )
                .where(reply_case_eligibility_clause(), reply_awaiting_clause())
            ).all()
        )
        actionable_ids = set(
            session.exec(
                select(ComplaintCase.id)
                .join(
                    ComplaintReply,
                    ComplaintReply.complaint_case_id == ComplaintCase.id,
                    isouter=True,
                )
                .where(reply_case_eligibility_clause(), reply_actionable_clause())
            ).all()
        )
        assert eligible_ids == {cases[name].id for name in expected_eligible}
        assert awaiting_ids == {cases[name].id for name in expected_awaiting}
        assert actionable_ids == {cases[name].id for name in expected_actionable}
        downward_ids = set(
            session.exec(
                select(ComplaintCase.id).where(
                    downward_dispatch_case_eligibility_clause()
                )
            ).all()
        )
        assert downward_ids == {
            cases[name].id
            for name in {
                "existing_bare",
                "fresh",
                "published_awaiting",
                "existing_local",
            }
        }

        workspace = ComplaintReplyWorkspaceService(session, cfg, NoopHelpdeskClient())
        stats = workspace.statistics()
        assert stats["reply_eligible_cases"] == 6
        assert stats["accepted_awaiting_publication"] == 2
        assert stats["awaiting_reply"] == 3
        assert stats["actionable_replies"] == 4
        assert stats["completed_replies"] == 2
        assert workspace.list_cases(scope="actionable", page_size=100)["total"] == 4
        assert workspace.list_cases(scope="all", page_size=100)["total"] == 6

        bulk = CrmBulkOperationService(session, cfg)
        awaiting_export = bulk.create_export_batch(scope="awaiting")
        actionable_export = bulk.create_export_batch(scope="actionable")
        all_export = bulk.create_export_batch(scope="all")
        assert awaiting_export["total_items"] == 3
        assert actionable_export["total_items"] == 4
        assert all_export["total_items"] == 6

        ai = CrmAiPipelineService(session, cfg)
        classification_export = ai.create_classification_export_batch(
            scope="awaiting_reply"
        )
        assert classification_export["total_items"] == 3

        validation = bulk.validate_import_batch(
            content=(
                "Complaint Number,Reply\n"
                f'{cases["published_approved"].complaint_number},"Attempted overwrite"\n'
            ).encode(),
            filename="overwrite.csv",
            parent_batch_id=uuid.UUID(all_export["id"]),
        )
        validation_items = session.exec(
            select(CrmBulkOperationItem).where(
                CrmBulkOperationItem.batch_id == uuid.UUID(validation["id"])
            )
        ).all()
        protected = next(
            item
            for item in validation_items
            if item.complaint_case_id == cases["published_approved"].id
        )
        assert protected.status == "invalid"
        assert protected.error_code == "reply_already_completed"
        assert session.exec(
            select(ComplaintReply).where(
                ComplaintReply.complaint_case_id == cases["published_approved"].id
            )
        ).first() is None
