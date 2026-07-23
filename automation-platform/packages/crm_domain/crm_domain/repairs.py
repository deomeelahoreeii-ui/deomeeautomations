from __future__ import annotations

import uuid
from typing import Any

from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    CrmBulkOperationBatch,
    CrmBulkOperationItem,
    CrmDispatchBatch,
    CrmDispatchItem,
    CrmUpwardSubmissionClaim,
)


class ReplyCaseStateRepairError(ValueError):
    pass


def repair_reply_import_case_states(
    session: Session,
    batch_id: uuid.UUID,
    *,
    apply: bool = False,
    actor: str = "reply-state-repair",
) -> dict[str, Any]:
    """Restore cases proven to have been published when their reply export ran.

    The completed parent export is the proof boundary. This deliberately avoids
    blanket-changing every `existing` case, since many are legitimate Paperless
    records that were never published by this platform.
    """

    batch = session.get(CrmBulkOperationBatch, batch_id)
    if batch is None or batch.operation_type != "reply_import":
        raise ReplyCaseStateRepairError("Select a reply-import batch")
    if batch.status not in {"completed", "completed_with_errors"}:
        raise ReplyCaseStateRepairError("The reply-import batch must be completed")
    parent = (
        session.get(CrmBulkOperationBatch, batch.parent_batch_id) if batch.parent_batch_id else None
    )
    if (
        parent is None
        or parent.operation_type not in {"reply_export", "reply_context_export"}
        or parent.status != "completed"
    ):
        raise ReplyCaseStateRepairError(
            "The import requires a completed reply export as state-repair evidence"
        )

    imported_case_ids = select(CrmBulkOperationItem.complaint_case_id).where(
        CrmBulkOperationItem.batch_id == batch.id,
        CrmBulkOperationItem.status.in_(("imported", "updated", "unchanged")),
        CrmBulkOperationItem.complaint_case_id.is_not(None),
    )
    exported_case_ids = select(CrmBulkOperationItem.complaint_case_id).where(
        CrmBulkOperationItem.batch_id == parent.id,
        CrmBulkOperationItem.status == "exported",
        CrmBulkOperationItem.complaint_case_id.is_not(None),
    )
    cases = list(
        session.exec(
            select(ComplaintCase)
            .where(
                ComplaintCase.id.in_(imported_case_ids),
                ComplaintCase.id.in_(exported_case_ids),
                ComplaintCase.registry_status == "active",
                ComplaintCase.state == "existing",
                ComplaintCase.frappe_ticket_id.is_not(None),
            )
            .order_by(ComplaintCase.complaint_number, ComplaintCase.id)
        ).all()
    )
    items = [
        {
            "case_id": str(case.id),
            "complaint_number": case.complaint_number,
            "before_state": case.state,
            "after_state": "published",
            "helpdesk_ticket_id": case.frappe_ticket_id,
            "paperless_document_id": case.canonical_paperless_document_id,
        }
        for case in cases
    ]
    if apply:
        now = utcnow()
        for case in cases:
            case.state = "published"
            case.updated_at = now
            session.add(case)
            session.add(
                ComplaintAuditEvent(
                    complaint_case_id=case.id,
                    entity_type="complaint_case",
                    entity_id=str(case.id),
                    event_type="reply_case_state_repaired",
                    actor=actor,
                    before_json={"state": "existing"},
                    after_json={"state": "published"},
                    details_json={
                        "reply_import_batch_id": str(batch.id),
                        "reply_import_batch_number": batch.batch_number,
                        "parent_export_batch_id": str(parent.id),
                        "parent_export_batch_number": parent.batch_number,
                        "proof": "case was exported while reply-workspace eligible",
                    },
                )
            )
        session.commit()
    return {
        "batch_id": str(batch.id),
        "batch_number": batch.batch_number,
        "parent_batch_id": str(parent.id),
        "mode": "apply" if apply else "dry_run",
        "candidate_count": len(items),
        "items": items,
    }


def audit_upward_submission_claim_backfill(
    session: Session,
    *,
    apply: bool = False,
    actor: str = "dispatch-claim-backfill-audit",
) -> dict[str, Any]:
    """Audit canonical claims created while reconciling legacy duplicate batches.

    Released duplicate items are audited by the migration itself. This repair
    records the complementary canonical choice and is safe to run repeatedly.
    """

    already_audited = set(
        session.exec(
            select(ComplaintAuditEvent.entity_id).where(
                ComplaintAuditEvent.event_type == "upward_submission_claim_backfilled"
            )
        ).all()
    )
    rows = [
        row
        for row in session.exec(
            select(CrmUpwardSubmissionClaim, CrmDispatchItem, CrmDispatchBatch)
            .join(
                CrmDispatchItem,
                CrmDispatchItem.id == CrmUpwardSubmissionClaim.dispatch_item_id,
            )
            .join(CrmDispatchBatch, CrmDispatchBatch.id == CrmDispatchItem.batch_id)
            .where(
                CrmUpwardSubmissionClaim.claimed_by == "dispatch-claim-backfill",
                CrmUpwardSubmissionClaim.released_at.is_(None),
            )
            .order_by(CrmDispatchBatch.created_at, CrmDispatchItem.complaint_number_snapshot)
        ).all()
        if str(row[1].id) not in already_audited
    ]
    items = [
        {
            "claim_id": str(claim.id),
            "dispatch_item_id": str(item.id),
            "official_letter_id": str(claim.official_letter_id),
            "complaint_number": item.complaint_number_snapshot,
            "batch_id": str(batch.id),
            "batch_number": batch.batch_number,
            "claim_status": claim.status,
        }
        for claim, item, batch in rows
    ]
    if apply:
        for claim, item, batch in rows:
            session.add(
                ComplaintAuditEvent(
                    complaint_case_id=item.complaint_case_id,
                    entity_type="crm_dispatch_item",
                    entity_id=str(item.id),
                    event_type="upward_submission_claim_backfilled",
                    actor=actor,
                    after_json={
                        "claim_status": claim.status,
                        "official_letter_id": str(claim.official_letter_id),
                    },
                    details_json={
                        "claim_id": str(claim.id),
                        "dispatch_batch_id": str(batch.id),
                        "dispatch_batch_number": batch.batch_number,
                        "proof": "canonical legacy dispatch selected by delivery-first backfill",
                    },
                )
            )
        session.commit()
    return {
        "mode": "apply" if apply else "dry_run",
        "candidate_count": len(items),
        "items": items,
    }
