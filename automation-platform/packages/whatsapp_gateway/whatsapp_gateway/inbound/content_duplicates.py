from __future__ import annotations

import hashlib
import re
import unicodedata
import uuid
from dataclasses import dataclass
from typing import Any, Iterable

from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintMatch,
)
from whatsapp_gateway.inbound.crm_registry import persist_crm_capture
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundMessage,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


AUTO_CONTENT_MATCH_KINDS = {
    "exact_reused",
    "exact_pending",
    "exact_conflict",
    "normalized_reused",
}
CONTENT_MATCH_KINDS = {*AUTO_CONTENT_MATCH_KINDS, "normalized_candidate"}
CRM_CATEGORIES = {
    "crm_complaint",
    "possible_crm_complaint",
    "crm_supporting_document",
    "crm_reply_or_report",
}


@dataclass(frozen=True)
class ApprovedRelationship:
    source_item: WhatsAppInboundProcessingItem
    document: ComplaintDocument
    case: ComplaintCase
    role: str


def normalize_evidence_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "").casefold()
    return re.sub(r"\s+", " ", normalized).strip()


def normalized_content_sha256(value: str) -> str | None:
    normalized = normalize_evidence_text(value)
    if len(normalized) < 160:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _category_for_role(role: str) -> str:
    if role == "main_complaint":
        return "crm_complaint"
    if role in {"complaint_details", "attachment"}:
        return "crm_supporting_document"
    if role in {"reply", "report"}:
        return "crm_reply_or_report"
    return "possible_crm_complaint"


def _source_records(
    session: Session,
    item: WhatsAppInboundProcessingItem,
) -> tuple[WhatsAppInboundBatchItem | None, WhatsAppInboundAttachment | None, str]:
    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
    filename = (
        (batch_item.original_filename if batch_item else None)
        or (attachment.original_filename if attachment else None)
        or f"evidence-{item.id}"
    )
    return batch_item, attachment, filename


def _item_provenance(
    session: Session,
    item: WhatsAppInboundProcessingItem,
) -> dict[str, Any]:
    batch_item, attachment, filename = _source_records(session, item)
    message = (
        session.get(WhatsAppInboundMessage, batch_item.message_id)
        if batch_item
        else None
    )
    return {
        "run_id": str(item.run_id),
        "filename": filename,
        "received_at": (
            message.message_timestamp.isoformat()
            if message and message.message_timestamp
            else None
        ),
        "contact_identity": message.remote_jid if message else None,
        "message_id": message.message_id if message else None,
        "attachment_id": str(attachment.id) if attachment else None,
    }


def _matching_processing_items(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    source_sha256: str | None = None,
    normalized_sha256: str | None = None,
) -> list[WhatsAppInboundProcessingItem]:
    query = (
        select(WhatsAppInboundProcessingItem)
        .join(
            WhatsAppInboundBatchItem,
            WhatsAppInboundBatchItem.id
            == WhatsAppInboundProcessingItem.batch_item_id,
        )
        .where(WhatsAppInboundProcessingItem.id != item.id)
    )
    if source_sha256:
        query = query.where(WhatsAppInboundBatchItem.sha256 == source_sha256)
    elif normalized_sha256:
        query = query.where(
            WhatsAppInboundProcessingItem.normalized_content_sha256
            == normalized_sha256
        )
    else:
        return []
    return list(
        session.exec(
            query.order_by(
                WhatsAppInboundProcessingItem.created_at,
                WhatsAppInboundProcessingItem.id,
            )
        ).all()
    )


def _approved_relationships(
    session: Session,
    items: Iterable[WhatsAppInboundProcessingItem],
) -> list[ApprovedRelationship]:
    relationships: list[ApprovedRelationship] = []
    for source in items:
        if source.review_status != "approved":
            continue
        document = session.exec(
            select(ComplaintDocument).where(
                ComplaintDocument.source_processing_item_id == source.id
            )
        ).first()
        if document is None:
            continue
        linked = list(
            session.exec(
                select(ComplaintDocumentCaseLink, ComplaintCase)
                .join(
                    ComplaintCase,
                    ComplaintCase.id
                    == ComplaintDocumentCaseLink.complaint_case_id,
                )
                .where(
                    ComplaintDocumentCaseLink.complaint_document_id == document.id
                )
                .order_by(ComplaintDocumentCaseLink.created_at)
            ).all()
        )
        if not linked and document.complaint_case_id:
            case = session.get(ComplaintCase, document.complaint_case_id)
            if case is not None:
                relationships.append(
                    ApprovedRelationship(
                        source_item=source,
                        document=document,
                        case=case,
                        role=document.role,
                    )
                )
        for link, case in linked:
            role = (
                document.role
                if document.role != "unclassified"
                else link.role
            )
            relationships.append(
                ApprovedRelationship(
                    source_item=source,
                    document=document,
                    case=case,
                    role=role,
                )
            )
    return relationships


def _relationship_payload(
    session: Session,
    relationship: ApprovedRelationship,
) -> dict[str, Any]:
    return {
        "processing_item_id": str(relationship.source_item.id),
        "document_id": str(relationship.document.id),
        "canonical_document_id": str(
            relationship.document.duplicate_of_document_id
            or relationship.document.id
        ),
        "case_id": str(relationship.case.id),
        "complaint_number": relationship.case.complaint_number,
        "case_state": relationship.case.state,
        "role": relationship.role,
        "category": _category_for_role(relationship.role),
        **_item_provenance(session, relationship.source_item),
    }


def _unique_relationship_keys(
    relationships: Iterable[ApprovedRelationship],
) -> set[tuple[uuid.UUID, str]]:
    return {(item.case.id, item.role) for item in relationships}


def _persist_duplicate_occurrence(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    canonical_document_id: uuid.UUID | None,
    role: str,
    case_id: uuid.UUID | None,
    match_kind: str,
) -> ComplaintDocument | None:
    if item.primary_category not in CRM_CATEGORIES:
        return None
    batch_item, attachment, filename = _source_records(session, item)
    persist_crm_capture(
        session,
        item=item,
        batch_item=batch_item,
        attachment=attachment,
        filename=filename,
        materialize_case=bool(case_id),
    )
    document = session.exec(
        select(ComplaintDocument).where(
            ComplaintDocument.source_processing_item_id == item.id
        )
    ).first()
    if document is None:
        return None
    if canonical_document_id and canonical_document_id != document.id:
        document.duplicate_of_document_id = canonical_document_id
    document.role = role if role else "unclassified"
    document.review_state = "duplicate"
    document.relationship_confidence = 1.0
    document.relationship_reason = (
        "Exact binary decision reused; retained as a receipt occurrence."
        if match_kind.startswith("exact")
        else "Normalized document content matches accepted evidence; retained as a receipt occurrence."
    )
    document.updated_at = utcnow()
    session.add(document)
    if case_id:
        link = session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_document_id == document.id,
                ComplaintDocumentCaseLink.complaint_case_id == case_id,
            )
        ).first()
        if link is not None:
            link.role = role or "unclassified"
            link.review_state = "duplicate"
            link.confidence = 1.0
            link.reason = document.relationship_reason
            session.add(link)
    if case_id and canonical_document_id:
        existing_match = session.exec(
            select(ComplaintMatch).where(
                ComplaintMatch.complaint_case_id == case_id,
                ComplaintMatch.processing_item_id == item.id,
                ComplaintMatch.proposed_decision
                == (
                    "exact_file_duplicate"
                    if match_kind.startswith("exact")
                    else "normalized_content_duplicate"
                ),
            )
        ).first()
        if existing_match is None:
            session.add(
                ComplaintMatch(
                    complaint_case_id=case_id,
                    processing_item_id=item.id,
                    matched_case_id=case_id,
                    proposed_decision=(
                        "exact_file_duplicate"
                        if match_kind.startswith("exact")
                        else "normalized_content_duplicate"
                    ),
                    final_decision="auto_reused",
                    score=1.0,
                    reason=document.relationship_reason,
                    signals_json={
                        "match_kind": match_kind,
                        "canonical_document_id": str(canonical_document_id),
                        "source_sha256": document.source_sha256,
                        "normalized_content_sha256": item.normalized_content_sha256,
                    },
                )
            )
    return document




# Public imports remain stable while implementation details stay below the
# repository's module-size guardrail.
from whatsapp_gateway.inbound.content_duplicate_resolution import (  # noqa: E402
    resolve_exact_duplicate_before_extraction,
    resolve_normalized_duplicate_after_extraction,
)
from whatsapp_gateway.inbound.content_duplicate_maintenance import (  # noqa: E402
    propagate_exact_duplicate_decision,
    reconcile_existing_content_duplicates,
    resolve_exact_relationship_conflict,
)

__all__ = [
    "AUTO_CONTENT_MATCH_KINDS",
    "CONTENT_MATCH_KINDS",
    "normalize_evidence_text",
    "normalized_content_sha256",
    "propagate_exact_duplicate_decision",
    "reconcile_existing_content_duplicates",
    "resolve_exact_duplicate_before_extraction",
    "resolve_exact_relationship_conflict",
    "resolve_normalized_duplicate_after_extraction",
]
