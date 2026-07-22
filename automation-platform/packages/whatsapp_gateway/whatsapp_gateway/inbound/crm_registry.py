from __future__ import annotations

from sqlalchemy import or_
from sqlmodel import Session, select

from crm_domain.models import ComplaintCase, ComplaintDocument
from crm_domain.models import ComplaintDocumentCaseLink
from automation_core.time import utcnow
from whatsapp_gateway.models import WhatsAppInboundMessage
from crm_domain.registry import (
    CapturedComplaintDocument,
    record_captured_document,
    record_paperless_match,
)
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
)


def persist_crm_capture(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    batch_item: WhatsAppInboundBatchItem | None,
    attachment: WhatsAppInboundAttachment | None,
    filename: str,
    materialize_case: bool = False,
) -> None:
    """Preserve evidence without creating a durable case before approval."""

    record_captured_document(
        session,
        CapturedComplaintDocument(
            processing_item_id=item.id,
            attachment_id=item.attachment_id,
            message_id=batch_item.message_id if batch_item else None,
            source_sha256=(batch_item.sha256 if batch_item else None)
            or (attachment.actual_sha256 if attachment else None)
            or (attachment.media_sha256 if attachment else None),
            original_filename=filename,
            mime_type=(batch_item.mime_type if batch_item else None)
            or (attachment.detected_mime_type if attachment else None)
            or (attachment.mime_type if attachment else None),
            category=item.primary_category,
            complaint_number=item.detected_complaint_number,
            classification_confidence=item.confidence,
            classification_evidence=list(item.evidence_json or []),
            extraction_method=item.extraction_method or "unknown",
            extracted_text=item.extracted_text or "",
            extraction_metadata=dict(item.extracted_metadata_json or {}),
            extraction_error=item.error,
        ),
        materialize_case=materialize_case,
    )


def persist_paperless_reconciliation(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    paperless_requested: bool,
    paperless_results: dict[str, object] | None = None,
    paperless_error: str = "",
) -> None:
    if not paperless_requested:
        return
    document = session.exec(
        select(ComplaintDocument).where(
            ComplaintDocument.source_processing_item_id == item.id
        )
    ).first()
    if document is None:
        return
    cases = list(
        session.exec(
            select(ComplaintCase)
            .join(
                ComplaintDocumentCaseLink,
                ComplaintDocumentCaseLink.complaint_case_id == ComplaintCase.id,
            )
            .where(ComplaintDocumentCaseLink.complaint_document_id == document.id)
        ).all()
    )
    results = paperless_results or {}
    for complaint_case in cases:
        number = complaint_case.complaint_number or ""
        result = results.get(number)
        category = str(getattr(result, "category", "unavailable" if paperless_error else "fresh"))
        reason = str(
            getattr(
                result,
                "reason",
                paperless_error or "No matching CRM main complaint was found in Paperless.",
            )
        )
        record_paperless_match(
            session,
            complaint_case=complaint_case,
            processing_item_id=item.id,
            category=category,
            reason=reason,
            document_ids=list(getattr(result, "matched_document_ids", []) or []),
            statuses=list(getattr(result, "matched_statuses", []) or []),
        )


def complaint_numbers_for_run(session: Session, run_id: object) -> list[str]:
    values = session.exec(
        select(WhatsAppInboundProcessingItem.detected_complaint_number).where(
            WhatsAppInboundProcessingItem.run_id == run_id,
            WhatsAppInboundProcessingItem.detected_complaint_number.is_not(None),
            or_(
                WhatsAppInboundProcessingItem.content_match_kind.is_(None),
                WhatsAppInboundProcessingItem.content_match_kind
                == "normalized_candidate",
            ),
        )
    ).all()
    # Import locally to keep the provider-neutral registry free of an inbound
    # service dependency at module import time.
    from whatsapp_gateway.inbound.spreadsheet_intake import (
        spreadsheet_numbers_for_run,
    )

    return sorted(
        {value for value in values if value}
        | spreadsheet_numbers_for_run(session, run_id)
    )


def associate_run_attachments(session: Session, run_id: object) -> int:
    """Associate numberless evidence only when the run gives a safe case signal."""

    documents = list(
        session.exec(
            select(ComplaintDocument)
            .join(
                WhatsAppInboundProcessingItem,
                WhatsAppInboundProcessingItem.id == ComplaintDocument.source_processing_item_id,
            )
            .where(WhatsAppInboundProcessingItem.run_id == run_id)
            .where(
                or_(
                    WhatsAppInboundProcessingItem.content_match_kind.is_(None),
                    WhatsAppInboundProcessingItem.content_match_kind
                    == "normalized_candidate",
                )
            )
            .order_by(ComplaintDocument.created_at)
        ).all()
    )
    links = list(
        session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_document_id.in_([item.id for item in documents])
            )
        ).all()
    ) if documents else []
    linked_by_document: dict[object, list[ComplaintDocumentCaseLink]] = {}
    for link in links:
        linked_by_document.setdefault(link.complaint_document_id, []).append(link)
    distinct_case_ids = {link.complaint_case_id for link in links}
    linked_times: list[tuple[ComplaintDocument, ComplaintDocumentCaseLink, object]] = []
    for document in documents:
        message = session.get(WhatsAppInboundMessage, document.source_message_id) if document.source_message_id else None
        for link in linked_by_document.get(document.id, []):
            if message:
                linked_times.append((document, link, message.message_timestamp))

    associated = 0
    for document in documents:
        if linked_by_document.get(document.id):
            continue
        selected_case_id = None
        confidence = 0.0
        reason = ""
        if len(distinct_case_ids) == 1:
            selected_case_id = next(iter(distinct_case_ids))
            confidence = 0.78
            reason = "Only one identified CRM complaint exists in this intake run."
        elif document.source_message_id:
            message = session.get(WhatsAppInboundMessage, document.source_message_id)
            if message:
                candidates = sorted(
                    (
                        (abs((message.message_timestamp - timestamp).total_seconds()), link.complaint_case_id)
                        for _linked_document, link, timestamp in linked_times
                    ),
                    key=lambda value: value[0],
                )
                if candidates and candidates[0][0] <= 300:
                    second_distance = candidates[1][0] if len(candidates) > 1 else float("inf")
                    if candidates[0][1] == (candidates[1][1] if len(candidates) > 1 else candidates[0][1]) or second_distance - candidates[0][0] >= 60:
                        selected_case_id = candidates[0][1]
                        confidence = 0.70
                        reason = "Nearest identified complaint message within five minutes."
        if selected_case_id is None:
            continue
        session.add(
            ComplaintDocumentCaseLink(
                complaint_document_id=document.id,
                complaint_case_id=selected_case_id,
                role="attachment",
                review_state="proposed",
                confidence=confidence,
                reason=reason,
                source_locator="inferred:whatsapp-run",
            )
        )
        document.complaint_case_id = selected_case_id
        document.role = "attachment"
        document.relationship_confidence = confidence
        document.relationship_reason = reason
        document.review_state = "proposed"
        document.updated_at = utcnow()
        session.add(document)
        associated += 1
    return associated
