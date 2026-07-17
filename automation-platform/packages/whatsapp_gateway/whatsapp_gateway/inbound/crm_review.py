from __future__ import annotations

import uuid

from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import ComplaintCase, ComplaintDocument, ComplaintDocumentCaseLink
from crm_domain.registry import promote_case_observations, record_paperless_match
from whatsapp_gateway.inbound.crm_registry import persist_crm_capture
from whatsapp_gateway.inbound.processing import update_review_decision
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


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

    case = approved[0][1]
    main_document_id = approved[0][0].id
    for document, _case in approved:
        link = session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocumentCaseLink.complaint_document_id == document.id,
            )
        ).first()
        if link is None:
            continue
        link.role = "main_complaint" if document.id == main_document_id else "complaint_details"
        link.review_state = "accepted"
        link.confidence = 1.0
        link.reason = "Complaint group approved by an operator."
        session.add(link)
    case.state = "existing" if case.canonical_paperless_document_id else "fresh"
    case.version += 1
    case.updated_at = utcnow()
    session.add(case)
    return case


def approve_manual_item(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    complaint_number: str,
    category: str,
    reviewed_by: str,
    note: str | None,
) -> ComplaintCase:
    normalized = normalize_complaint_number(complaint_number)
    if normalized is None:
        raise ValueError("Enter one valid complaint number, for example 104-6609317")
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
    if link:
        existing_main = session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocumentCaseLink.role == "main_complaint",
                ComplaintDocumentCaseLink.review_state == "accepted",
            )
        ).first()
        link.role = "complaint_details" if existing_main else "main_complaint"
        link.review_state = "accepted"
        link.confidence = 1.0
        link.reason = "Ambiguous evidence approved and assigned by an operator."
        session.add(link)
    case.state = "existing" if case.canonical_paperless_document_id else "fresh"
    case.version += 1
    case.updated_at = utcnow()
    session.add(case)
    return case
