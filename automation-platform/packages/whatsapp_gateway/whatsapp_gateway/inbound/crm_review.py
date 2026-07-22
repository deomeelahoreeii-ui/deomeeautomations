from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import ComplaintCase, ComplaintDocument, ComplaintDocumentCaseLink
from crm_domain.registry import promote_case_observations, record_paperless_match
from whatsapp_gateway.inbound.crm_registry import persist_crm_capture
from whatsapp_gateway.inbound.content_duplicates import (
    propagate_exact_duplicate_decision,
    resolve_exact_relationship_conflict,
)
from whatsapp_gateway.inbound.processing import update_review_decision
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


@dataclass(frozen=True)
class ManualItemApproval:
    case: ComplaintCase
    document: ComplaintDocument
    case_created: bool
    role: str


MANUAL_CATEGORY_ROLES = {
    "crm_complaint": "main_complaint",
    "possible_crm_complaint": "complaint_details",
    "crm_supporting_document": "attachment",
    "crm_reply_or_report": "report",
}


def _source_records(
    session: Session, item: WhatsAppInboundProcessingItem
) -> tuple[WhatsAppInboundBatchItem | None, WhatsAppInboundAttachment | None, str]:
    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
    filename = (
        (batch_item.original_filename if batch_item else None)
        or (attachment.original_filename if attachment else None)
        or f"evidence-{item.id}"
    )
    return batch_item, attachment, filename


def _persist_approved_item(
    session: Session, item: WhatsAppInboundProcessingItem
) -> tuple[ComplaintDocument, ComplaintCase]:
    batch_item, attachment, filename = _source_records(session, item)
    persist_crm_capture(
        session,
        item=item,
        batch_item=batch_item,
        attachment=attachment,
        filename=filename,
        materialize_case=True,
    )
    document = session.exec(
        select(ComplaintDocument).where(
            ComplaintDocument.source_processing_item_id == item.id
        )
    ).one()
    case = session.exec(
        select(ComplaintCase).where(
            ComplaintCase.source_system == "crm_portal",
            ComplaintCase.complaint_number
            == normalize_complaint_number(item.detected_complaint_number),
        )
    ).one()
    promote_case_observations(session, case)
    if item.paperless_category and item.paperless_category != "not_applicable":
        record_paperless_match(
            session,
            complaint_case=case,
            processing_item_id=item.id,
            category=item.paperless_category,
            reason=item.paperless_reason or "Paperless decision retained from processing.",
            document_ids=list(item.paperless_document_ids or []),
            statuses=list(item.paperless_statuses or []),
        )
    return document, case


def decide_complaint_group(
    session: Session,
    *,
    run: WhatsAppInboundProcessingRun,
    complaint_number: str,
    decision: str,
    reviewed_by: str,
    note: str | None,
) -> ComplaintCase | None:
    normalized = normalize_complaint_number(complaint_number)
    if normalized is None:
        raise ValueError("A single valid CRM complaint number is required")
    items = list(
        session.exec(
            select(WhatsAppInboundProcessingItem)
            .where(
                WhatsAppInboundProcessingItem.run_id == run.id,
                WhatsAppInboundProcessingItem.detected_complaint_number == normalized,
            )
            .order_by(
                WhatsAppInboundProcessingItem.confidence.desc(),
                WhatsAppInboundProcessingItem.created_at,
            )
        ).all()
    )
    if not items:
        raise ValueError("Complaint group was not found in this processing run")
    if decision not in {"approved", "rejected"}:
        raise ValueError("Complaint groups can only be approved or rejected")

    existing_case = session.exec(
        select(ComplaintCase).where(
            ComplaintCase.source_system == "crm_portal",
            ComplaintCase.complaint_number == normalized,
        )
    ).first()
    existing_case_state = existing_case.state if existing_case is not None else None

    if decision == "rejected":
        for item in items:
            update_review_decision(
                session,
                item=item,
                decision="rejected",
                reviewed_by=reviewed_by,
                note=note,
                category=None,
                complaint_number=normalized,
            )
        case = session.exec(
            select(ComplaintCase).where(
                ComplaintCase.source_system == "crm_portal",
                ComplaintCase.complaint_number == normalized,
            )
        ).first()
        if case:
            links = list(
                session.exec(
                    select(ComplaintDocumentCaseLink).where(
                        ComplaintDocumentCaseLink.complaint_case_id == case.id
                    )
                ).all()
            )
            for link in links:
                link.review_state = "rejected"
                session.add(link)
            case.state = "rejected"
            case.updated_at = utcnow()
            session.add(case)
        for item in items:
            propagate_exact_duplicate_decision(session, source_item=item)
        return case

    approved: list[tuple[ComplaintDocument, ComplaintCase]] = []
    for item in items:
        update_review_decision(
            session,
            item=item,
            decision="approved",
            reviewed_by=reviewed_by,
            note=note,
            category=item.primary_category,
            complaint_number=normalized,
        )
        approved.append(_persist_approved_item(session, item))

    canonical = [pair for pair in approved if pair[0].review_state != "duplicate"]
    case = canonical[0][1] if canonical else approved[0][1]
    main_document_id = canonical[0][0].id if canonical else None
    for document, _case in approved:
        link = session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocumentCaseLink.complaint_document_id == document.id,
            )
        ).first()
        if link is None:
            continue
        if document.review_state == "duplicate":
            link.review_state = "duplicate"
            link.confidence = 1.0
            link.reason = "Exact binary duplicate; retained only in the capture audit."
            session.add(link)
            continue
        link.role = "main_complaint" if document.id == main_document_id else "complaint_details"
        link.review_state = "accepted"
        link.confidence = 1.0
        link.reason = "Complaint group approved by an operator."
        session.add(link)
    if canonical:
        case.state = "existing" if case.canonical_paperless_document_id else "fresh"
        case.version += 1
        case.updated_at = utcnow()
        session.add(case)
    elif existing_case_state is not None:
        # Approving a repeated capture completes the review item, but it must
        # not re-open, reject, or freshly approve the already-known case.
        case.state = existing_case_state
        session.add(case)
    for item in items:
        propagate_exact_duplicate_decision(session, source_item=item)
    return case


def approve_manual_item(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    complaint_number: str,
    category: str,
    reviewed_by: str,
    note: str | None,
) -> ManualItemApproval:
    normalized = normalize_complaint_number(complaint_number)
    if normalized is None:
        raise ValueError("Enter one valid complaint number, for example 104-6609317")
    role = MANUAL_CATEGORY_ROLES.get(category)
    if role is None:
        raise ValueError("Choose a valid CRM complaint document type")
    existing_case = session.exec(
        select(ComplaintCase).where(
            ComplaintCase.source_system == "crm_portal",
            ComplaintCase.complaint_number == normalized,
        )
    ).first()
    existing_case_state = existing_case.state if existing_case is not None else None
    update_review_decision(
        session,
        item=item,
        decision="approved",
        reviewed_by=reviewed_by,
        note=note,
        category=category,
        complaint_number=normalized,
    )
    document, case = _persist_approved_item(session, item)
    link = session.exec(
        select(ComplaintDocumentCaseLink).where(
            ComplaintDocumentCaseLink.complaint_case_id == case.id,
            ComplaintDocumentCaseLink.complaint_document_id == document.id,
        )
    ).first()
    resolving_content_conflict = item.content_match_kind == "exact_conflict"
    is_duplicate = document.review_state == "duplicate"
    if resolving_content_conflict:
        canonical = resolve_exact_relationship_conflict(
            session,
            item=item,
            complaint_case_id=case.id,
            role=role,
            reviewed_by=reviewed_by,
        )
        is_duplicate = document.id != canonical.id
    elif link and is_duplicate:
        link.review_state = "duplicate"
        link.confidence = 1.0
        link.reason = "Exact binary duplicate; retained only in the capture audit."
        session.add(link)
    elif link:
        link.role = role
        link.review_state = "accepted"
        link.confidence = 1.0
        link.reason = f"Linked as {role.replace('_', ' ')} by an operator during manual review."
        session.add(link)
    document.complaint_case_id = case.id
    if not is_duplicate and not resolving_content_conflict:
        document.role = role
        document.review_state = "accepted"
        document.relationship_confidence = 1.0
        document.relationship_reason = f"Linked as {role.replace('_', ' ')} by an operator during manual review."
    document.updated_at = utcnow()
    session.add(document)

    if is_duplicate and not resolving_content_conflict:
        role = "duplicate"
        if existing_case_state is not None:
            case.state = existing_case_state
    elif role == "main_complaint":
        case.state = "existing" if case.canonical_paperless_document_id else "fresh"
    elif existing_case is None or existing_case_state in {"candidate", "rejected"}:
        # An attachment can establish a case relationship, but it cannot make a
        # complaint publishable without an accepted main complaint.
        case.state = "review_required"
    elif existing_case_state is not None:
        # Reconciliation for this newly captured document may propose its own
        # state. An attachment must not regress an established complaint.
        case.state = existing_case_state
    case.version += 1
    case.updated_at = utcnow()
    session.add(case)
    propagate_exact_duplicate_decision(session, source_item=item)
    return ManualItemApproval(
        case=case,
        document=document,
        case_created=existing_case is None,
        role=role,
    )
