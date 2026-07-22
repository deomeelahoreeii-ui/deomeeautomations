from __future__ import annotations

import uuid

from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.models import ComplaintDocument, ComplaintDocumentCaseLink
from whatsapp_gateway.inbound.content_duplicates import (
    _approved_relationships,
    _category_for_role,
    _item_provenance,
    _source_records,
    _unique_relationship_keys,
    normalized_content_sha256,
)
from whatsapp_gateway.inbound.content_duplicate_resolution import (
    _apply_rejected_reuse,
    _apply_reused_relationship,
    resolve_exact_duplicate_before_extraction,
)
from whatsapp_gateway.models import (
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


def resolve_exact_relationship_conflict(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    complaint_case_id: uuid.UUID,
    role: str,
    reviewed_by: str,
) -> ComplaintDocument:
    """Apply one operator-selected role to every identical capture in a case."""

    batch_item, attachment, _filename = _source_records(session, item)
    source_sha256 = (
        (batch_item.sha256 if batch_item else None)
        or (attachment.actual_sha256 if attachment else None)
        or (attachment.media_sha256 if attachment else None)
    )
    if not source_sha256:
        raise ValueError("The evidence has no durable SHA-256 identity")
    rows = list(
        session.exec(
            select(ComplaintDocument, ComplaintDocumentCaseLink)
            .join(
                ComplaintDocumentCaseLink,
                ComplaintDocumentCaseLink.complaint_document_id
                == ComplaintDocument.id,
            )
            .where(
                ComplaintDocument.source_sha256 == source_sha256,
                ComplaintDocumentCaseLink.complaint_case_id
                == complaint_case_id,
            )
            .order_by(ComplaintDocument.created_at, ComplaintDocument.id)
        ).all()
    )
    if not rows:
        raise ValueError("No identical case evidence exists to reconcile")
    canonical = rows[0][0]
    category = _category_for_role(role)
    reason = f"Canonical role set to {role.replace('_', ' ')} by {reviewed_by}."
    for document, link in rows:
        document.role = role
        document.relationship_confidence = 1.0
        document.relationship_reason = reason
        document.updated_at = utcnow()
        if document.id == canonical.id:
            document.duplicate_of_document_id = None
            document.review_state = "accepted"
        else:
            document.duplicate_of_document_id = canonical.id
            document.review_state = "duplicate"
        session.add(document)
        link.role = role
        link.confidence = 1.0
        link.reason = reason
        link.review_state = (
            "accepted" if document.id == canonical.id else "duplicate"
        )
        session.add(link)
        if document.source_processing_item_id:
            source_item = session.get(
                WhatsAppInboundProcessingItem,
                document.source_processing_item_id,
            )
            if source_item is not None:
                source_item.primary_category = category
                source_item.detected_complaint_number = item.detected_complaint_number
                source_item.updated_at = utcnow()
                session.add(source_item)

    item.content_match_kind = "exact_reused"
    item.canonical_processing_item_id = canonical.source_processing_item_id
    item.content_match_details_json = {
        "decision": "conflict_resolved",
        "reason": reason,
        "fingerprint": source_sha256,
        "canonical": {
            "processing_item_id": (
                str(canonical.source_processing_item_id)
                if canonical.source_processing_item_id
                else None
            ),
            "document_id": str(canonical.id),
            "canonical_document_id": str(canonical.id),
            "case_id": str(complaint_case_id),
            "complaint_number": item.detected_complaint_number,
            "role": role,
            **(
                _item_provenance(
                    session,
                    session.get(
                        WhatsAppInboundProcessingItem,
                        canonical.source_processing_item_id,
                    ),
                )
                if canonical.source_processing_item_id
                and session.get(
                    WhatsAppInboundProcessingItem,
                    canonical.source_processing_item_id,
                )
                else {}
            ),
        },
    }
    item.updated_at = utcnow()
    session.add(item)
    return canonical


def propagate_exact_duplicate_decision(
    session: Session,
    *,
    source_item: WhatsAppInboundProcessingItem,
) -> int:
    waiting = list(
        session.exec(
            select(WhatsAppInboundProcessingItem).where(
                WhatsAppInboundProcessingItem.canonical_processing_item_id
                == source_item.id,
                WhatsAppInboundProcessingItem.content_match_kind == "exact_pending",
            )
        ).all()
    )
    if not waiting:
        return 0
    if source_item.review_status == "approved":
        relationships = _approved_relationships(session, [source_item])
        if len(_unique_relationship_keys(relationships)) != 1:
            return 0
        for item in waiting:
            batch_item, attachment, _filename = _source_records(session, item)
            source_sha256 = (
                (batch_item.sha256 if batch_item else None)
                or (attachment.actual_sha256 if attachment else None)
                or ""
            )
            _apply_reused_relationship(
                session,
                item=item,
                relationship=relationships[0],
                match_kind="exact_reused",
                fingerprint=source_sha256,
            )
    elif source_item.review_status == "rejected":
        for item in waiting:
            batch_item, attachment, _filename = _source_records(session, item)
            source_sha256 = (
                (batch_item.sha256 if batch_item else None)
                or (attachment.actual_sha256 if attachment else None)
                or ""
            )
            _apply_rejected_reuse(
                session,
                item=item,
                source=source_item,
                source_sha256=source_sha256,
            )
    else:
        return 0

    from whatsapp_gateway.inbound.processing import (
        recalculate_processing_run,
        record_processing_event,
    )

    run_ids = {item.run_id for item in waiting}
    for run_id in run_ids:
        run = session.get(WhatsAppInboundProcessingRun, run_id)
        if run is not None:
            recalculate_processing_run(session, run)
            record_processing_event(
                session,
                run_id=run.id,
                event_type="content_duplicate_decision_propagated",
                message=(
                    f"Reused the canonical {source_item.review_status} decision for exact duplicate receipt(s)."
                ),
                details={
                    "canonical_processing_item_id": str(source_item.id),
                    "updated_items": sum(item.run_id == run_id for item in waiting),
                },
            )
    return len(waiting)


def reconcile_existing_content_duplicates(session: Session) -> dict[str, int]:
    """Backfill fingerprints and apply deterministic duplicate decisions.

    The caller owns the transaction, which makes the operation safe to run as a
    dry run by rolling the session back after inspecting the returned counts.
    No source capture, attachment, document, or audit record is deleted.
    """

    report = {
        "fingerprints_backfilled": 0,
        "document_links_backfilled": 0,
        "items_resolved": 0,
        "exact_reused": 0,
        "exact_pending": 0,
        "exact_conflict": 0,
    }
    items = list(
        session.exec(
            select(WhatsAppInboundProcessingItem).order_by(
                WhatsAppInboundProcessingItem.created_at,
                WhatsAppInboundProcessingItem.id,
            )
        ).all()
    )
    for item in items:
        if item.normalized_content_sha256 or not item.extracted_text:
            continue
        fingerprint = normalized_content_sha256(item.extracted_text)
        if fingerprint:
            item.normalized_content_sha256 = fingerprint
            session.add(item)
            report["fingerprints_backfilled"] += 1
    session.flush()

    linked_documents = list(
        session.exec(
            select(ComplaintDocument, ComplaintDocumentCaseLink)
            .join(
                ComplaintDocumentCaseLink,
                ComplaintDocumentCaseLink.complaint_document_id
                == ComplaintDocument.id,
            )
            .where(ComplaintDocument.source_sha256.is_not(None))
            .order_by(
                ComplaintDocumentCaseLink.complaint_case_id,
                ComplaintDocument.source_sha256,
                ComplaintDocument.created_at,
                ComplaintDocument.id,
            )
        ).all()
    )
    canonical_by_key: dict[tuple[uuid.UUID, str], uuid.UUID] = {}
    for document, link in linked_documents:
        if not document.source_sha256:
            continue
        key = (link.complaint_case_id, document.source_sha256)
        canonical_id = canonical_by_key.setdefault(key, document.id)
        if document.id == canonical_id or document.duplicate_of_document_id == canonical_id:
            continue
        document.duplicate_of_document_id = canonical_id
        session.add(document)
        report["document_links_backfilled"] += 1
    session.flush()

    resolvable = [
        item
        for item in items
        if item.content_match_kind is None
        and item.review_status in {"pending", "deferred"}
        and item.status in {"queued", "extracting", "extracted", "eligible", "needs_review", "deferred"}
    ]
    affected_run_ids: set[uuid.UUID] = set()
    for item in resolvable:
        if resolve_exact_duplicate_before_extraction(session, item=item):
            report["items_resolved"] += 1
            if item.content_match_kind in report:
                report[item.content_match_kind] += 1
            affected_run_ids.add(item.run_id)
            session.flush()

    from whatsapp_gateway.inbound.processing import recalculate_processing_run

    for run_id in affected_run_ids:
        run = session.get(WhatsAppInboundProcessingRun, run_id)
        if run is not None:
            recalculate_processing_run(session, run)
    session.flush()
    return report
