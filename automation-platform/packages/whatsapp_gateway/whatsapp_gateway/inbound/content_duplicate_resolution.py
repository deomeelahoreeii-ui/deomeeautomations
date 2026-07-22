from __future__ import annotations

from typing import Any

from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.models import ComplaintDocument
from whatsapp_gateway.inbound.content_duplicates import (
    CRM_CATEGORIES,
    ApprovedRelationship,
    _approved_relationships,
    _category_for_role,
    _item_provenance,
    _matching_processing_items,
    _persist_duplicate_occurrence,
    _relationship_payload,
    _source_records,
    _unique_relationship_keys,
    normalized_content_sha256,
)
from whatsapp_gateway.models import WhatsAppInboundProcessingItem


def _apply_reused_relationship(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    relationship: ApprovedRelationship,
    match_kind: str,
    fingerprint: str,
) -> None:
    source = relationship.source_item
    canonical_item_id = source.canonical_processing_item_id or source.id
    canonical_document_id = (
        relationship.document.duplicate_of_document_id or relationship.document.id
    )
    details = {
        "decision": "approved",
        "reason": (
            "The source SHA-256 exactly matches previously approved evidence."
            if match_kind == "exact_reused"
            else "The normalized document content exactly matches previously approved evidence for the same complaint."
        ),
        "fingerprint": fingerprint,
        "canonical": _relationship_payload(session, relationship),
    }
    item.primary_category = _category_for_role(relationship.role)
    item.detected_complaint_number = relationship.case.complaint_number
    item.confidence = 1.0
    item.evidence_json = [
        "human_decision_reused",
        "exact_source_sha256"
        if match_kind == "exact_reused"
        else "exact_normalized_content",
    ]
    item.extracted_text = source.extracted_text
    item.extraction_method = f"reused:{source.extraction_method or 'prior-extraction'}"
    item.normalized_content_sha256 = (
        source.normalized_content_sha256
        or normalized_content_sha256(source.extracted_text or "")
    )
    item.extracted_metadata_json = {
        **(source.extracted_metadata_json or {}),
        "content_decision_reused": True,
        "canonical_processing_item_id": str(canonical_item_id),
    }
    item.paperless_category = "not_applicable"
    item.paperless_reason = (
        "Paperless was not checked again because an accepted content decision was reused."
    )
    item.paperless_document_ids = []
    item.paperless_statuses = []
    item.status = "approved"
    item.review_status = "approved"
    item.reviewer_note = details["reason"]
    item.reviewed_by = "system:content-deduplication"
    item.reviewed_at = utcnow()
    item.finished_at = utcnow()
    item.updated_at = utcnow()
    item.content_match_kind = match_kind
    item.canonical_processing_item_id = canonical_item_id
    item.content_match_details_json = details
    session.add(item)
    _persist_duplicate_occurrence(
        session,
        item=item,
        canonical_document_id=canonical_document_id,
        role=relationship.role,
        case_id=relationship.case.id,
        match_kind=match_kind,
    )


def _apply_rejected_reuse(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    source: WhatsAppInboundProcessingItem,
    source_sha256: str,
) -> None:
    canonical_item_id = source.canonical_processing_item_id or source.id
    reason = "The source SHA-256 exactly matches evidence previously rejected by an operator."
    item.primary_category = source.primary_category
    item.detected_complaint_number = source.detected_complaint_number
    item.confidence = 1.0
    item.evidence_json = ["exact_source_sha256", "human_rejection_reused"]
    item.extracted_text = source.extracted_text
    item.extraction_method = f"reused:{source.extraction_method or 'prior-extraction'}"
    item.normalized_content_sha256 = source.normalized_content_sha256
    item.extracted_metadata_json = {
        **(source.extracted_metadata_json or {}),
        "content_decision_reused": True,
        "canonical_processing_item_id": str(canonical_item_id),
    }
    item.paperless_category = "not_applicable"
    item.paperless_reason = "Paperless was not checked because the prior rejection was reused."
    item.status = "rejected"
    item.review_status = "rejected"
    item.reviewer_note = reason
    item.reviewed_by = "system:content-deduplication"
    item.reviewed_at = utcnow()
    item.finished_at = utcnow()
    item.updated_at = utcnow()
    item.content_match_kind = "exact_reused"
    item.canonical_processing_item_id = canonical_item_id
    item.content_match_details_json = {
        "decision": "rejected",
        "reason": reason,
        "fingerprint": source_sha256,
        "canonical": {
            "processing_item_id": str(source.id),
            **_item_provenance(session, source),
        },
    }
    session.add(item)


def _apply_pending_reference(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    source: WhatsAppInboundProcessingItem,
    source_sha256: str,
) -> None:
    canonical_item_id = source.canonical_processing_item_id or source.id
    reason = "This exact binary is already represented by an unresolved review item."
    item.primary_category = source.primary_category
    item.detected_complaint_number = source.detected_complaint_number
    item.confidence = source.confidence
    item.evidence_json = ["exact_source_sha256", "shared_pending_review"]
    item.extracted_text = source.extracted_text
    item.extraction_method = f"reused:{source.extraction_method or 'prior-extraction'}"
    item.normalized_content_sha256 = source.normalized_content_sha256
    item.extracted_metadata_json = {
        **(source.extracted_metadata_json or {}),
        "content_decision_reused": True,
        "canonical_processing_item_id": str(canonical_item_id),
    }
    item.paperless_category = "not_applicable"
    item.paperless_reason = "Paperless will be determined by the canonical review item."
    item.status = "deferred"
    item.review_status = "deferred"
    item.reviewer_note = reason
    item.reviewed_by = "system:content-deduplication"
    item.reviewed_at = utcnow()
    item.finished_at = utcnow()
    item.updated_at = utcnow()
    item.content_match_kind = "exact_pending"
    item.canonical_processing_item_id = canonical_item_id
    item.content_match_details_json = {
        "decision": "pending",
        "reason": reason,
        "fingerprint": source_sha256,
        "canonical": {
            "processing_item_id": str(canonical_item_id),
            **_item_provenance(session, source),
        },
    }
    session.add(item)
    source_document = session.exec(
        select(ComplaintDocument).where(
            ComplaintDocument.source_processing_item_id == canonical_item_id
        )
    ).first()
    case_id = source_document.complaint_case_id if source_document else None
    _persist_duplicate_occurrence(
        session,
        item=item,
        canonical_document_id=(
            source_document.duplicate_of_document_id or source_document.id
            if source_document
            else None
        ),
        role=source_document.role if source_document else "unclassified",
        case_id=case_id,
        match_kind="exact_pending",
    )


def _apply_conflict(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    relationships: list[ApprovedRelationship],
    source_sha256: str,
) -> None:
    payloads = [_relationship_payload(session, value) for value in relationships]
    distinct_payloads: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for payload in payloads:
        key = (str(payload["case_id"]), str(payload["role"]))
        if key not in seen:
            seen.add(key)
            distinct_payloads.append(payload)
    case_ids = {value.case.id for value in relationships}
    case = relationships[0].case if len(case_ids) == 1 else None
    source = relationships[0].source_item
    canonical_item_id = source.canonical_processing_item_id or source.id
    reason = (
        "Exact duplicate recognized, but historical approvals disagree about its complaint relationship."
    )
    item.primary_category = "possible_crm_complaint"
    item.detected_complaint_number = case.complaint_number if case else None
    item.confidence = 1.0
    item.evidence_json = ["exact_source_sha256", "historical_relationship_conflict"]
    item.extracted_text = source.extracted_text
    item.extraction_method = f"reused:{source.extraction_method or 'prior-extraction'}"
    item.normalized_content_sha256 = source.normalized_content_sha256
    item.extracted_metadata_json = {
        **(source.extracted_metadata_json or {}),
        "content_decision_reused": True,
        "historical_relationship_conflict": True,
    }
    item.paperless_category = "not_applicable"
    item.paperless_reason = "Paperless was not checked again for an exact duplicate."
    item.status = "deferred"
    item.review_status = "deferred"
    item.reviewer_note = reason
    item.reviewed_by = "system:content-deduplication"
    item.reviewed_at = utcnow()
    item.finished_at = utcnow()
    item.updated_at = utcnow()
    item.content_match_kind = "exact_conflict"
    item.canonical_processing_item_id = canonical_item_id
    item.content_match_details_json = {
        "decision": "conflict",
        "reason": reason,
        "fingerprint": source_sha256,
        "relationships": distinct_payloads,
    }
    session.add(item)
    canonical_document = relationships[0].document
    _persist_duplicate_occurrence(
        session,
        item=item,
        canonical_document_id=(
            canonical_document.duplicate_of_document_id or canonical_document.id
        ),
        role="unclassified",
        case_id=case.id if case else None,
        match_kind="exact_conflict",
    )


def resolve_exact_duplicate_before_extraction(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
) -> bool:
    batch_item, attachment, _filename = _source_records(session, item)
    source_sha256 = (
        (batch_item.sha256 if batch_item else None)
        or (attachment.actual_sha256 if attachment else None)
        or (attachment.media_sha256 if attachment else None)
    )
    if not source_sha256:
        return False
    candidates = _matching_processing_items(
        session,
        item=item,
        source_sha256=source_sha256,
    )
    relationships = _approved_relationships(session, candidates)
    if relationships:
        if len(_unique_relationship_keys(relationships)) == 1:
            _apply_reused_relationship(
                session,
                item=item,
                relationship=relationships[0],
                match_kind="exact_reused",
                fingerprint=source_sha256,
            )
        else:
            _apply_conflict(
                session,
                item=item,
                relationships=relationships,
                source_sha256=source_sha256,
            )
        return True

    final_candidates = [
        value
        for value in candidates
        if value.review_status in {"approved", "rejected"}
    ]
    if final_candidates and all(
        value.review_status == "rejected" for value in final_candidates
    ):
        _apply_rejected_reuse(
            session,
            item=item,
            source=final_candidates[0],
            source_sha256=source_sha256,
        )
        return True

    pending = next(
        (
            value
            for value in candidates
            if (value.created_at, str(value.id))
            < (item.created_at, str(item.id))
            if value.primary_category in CRM_CATEGORIES
            and value.review_status in {"pending", "deferred"}
            and value.content_match_kind not in {"exact_conflict"}
        ),
        None,
    )
    if pending is not None:
        _apply_pending_reference(
            session,
            item=item,
            source=pending,
            source_sha256=source_sha256,
        )
        return True
    return False


def resolve_normalized_duplicate_after_extraction(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
) -> bool:
    fingerprint = normalized_content_sha256(item.extracted_text or "")
    item.normalized_content_sha256 = fingerprint
    session.add(item)
    if not fingerprint:
        return False
    candidates = _matching_processing_items(
        session,
        item=item,
        normalized_sha256=fingerprint,
    )
    relationships = _approved_relationships(session, candidates)
    if not relationships:
        return False
    matching_number = [
        value
        for value in relationships
        if item.detected_complaint_number
        and value.case.complaint_number == item.detected_complaint_number
    ]
    if matching_number and len(_unique_relationship_keys(matching_number)) == 1:
        _apply_reused_relationship(
            session,
            item=item,
            relationship=matching_number[0],
            match_kind="normalized_reused",
            fingerprint=fingerprint,
        )
        return True
    source = relationships[0].source_item
    item.content_match_kind = "normalized_candidate"
    item.canonical_processing_item_id = source.canonical_processing_item_id or source.id
    item.content_match_details_json = {
        "decision": "candidate",
        "reason": (
            "Normalized content matches prior evidence, but the complaint relationship is not unambiguous."
        ),
        "fingerprint": fingerprint,
        "relationships": [
            _relationship_payload(session, relationship)
            for relationship in relationships
        ],
    }
    item.evidence_json = list(
        dict.fromkeys([*(item.evidence_json or []), "normalized_content_match"])
    )
    session.add(item)
    return False


