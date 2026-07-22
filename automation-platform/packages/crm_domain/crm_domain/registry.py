from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.fields import extract_field_observations
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintFieldObservation,
    ComplaintMatch,
    DocumentExtraction,
)


EXTRACTOR_NAME = "deomee-crm-structured-text"
EXTRACTOR_VERSION = "1"


@dataclass(frozen=True)
class CapturedComplaintDocument:
    processing_item_id: object
    attachment_id: object | None
    message_id: object | None
    source_sha256: str | None
    original_filename: str | None
    mime_type: str | None
    category: str
    complaint_number: str | None
    classification_confidence: float
    classification_evidence: list[str]
    extraction_method: str
    extracted_text: str
    extraction_metadata: dict[str, object]
    extraction_error: str | None = None


def _normalized_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _document_role(category: str) -> str:
    return {
        "crm_complaint": "main_complaint",
        "crm_supporting_document": "complaint_details",
        "crm_reply_or_report": "report",
    }.get(category, "unclassified")


def _find_case(
    session: Session,
    *,
    complaint_number: str | None,
) -> ComplaintCase | None:
    if not complaint_number:
        return None
    return session.exec(
        select(ComplaintCase).where(
            ComplaintCase.source_system == "crm_portal",
            ComplaintCase.complaint_number == complaint_number,
        )
    ).first()


def _get_or_create_case(session: Session, complaint_number: str | None) -> ComplaintCase | None:
    case = _find_case(session, complaint_number=complaint_number)
    if case is not None or not complaint_number:
        return case
    case = ComplaintCase(
        source_system="crm_portal",
        complaint_number=complaint_number,
        state="candidate",
    )
    try:
        with session.begin_nested():
            session.add(case)
            session.flush()
    except IntegrityError:
        case = _find_case(session, complaint_number=complaint_number)
        if case is None:  # pragma: no cover
            raise
    return case


def _link_case(
    session: Session,
    *,
    document: ComplaintDocument,
    case: ComplaintCase,
    role: str,
    confidence: float,
    reason: str | None,
    source_locator: str,
) -> ComplaintDocumentCaseLink:
    exists = session.exec(
        select(ComplaintDocumentCaseLink).where(
            ComplaintDocumentCaseLink.complaint_document_id == document.id,
            ComplaintDocumentCaseLink.complaint_case_id == case.id,
            ComplaintDocumentCaseLink.source_locator == source_locator,
        )
    ).first()
    if exists is not None:
        return exists
    link = ComplaintDocumentCaseLink(
        complaint_document_id=document.id,
        complaint_case_id=case.id,
        role=role,
        review_state="proposed",
        confidence=confidence,
        reason=reason,
        source_locator=source_locator,
    )
    session.add(link)
    return link


def _canonical_field_values(
    observations: list[ComplaintFieldObservation],
) -> dict[str, str]:
    values: dict[str, str] = {}
    for observation in sorted(observations, key=lambda item: item.confidence, reverse=True):
        if observation.confidence >= 0.85:
            values.setdefault(observation.field_name, observation.normalized_value)
    return values


def _promote_safe_fields(case: ComplaintCase, values: dict[str, str]) -> None:
    """Fill blank values and safely complete a previously truncated remark."""

    for field_name in (
        "complainant_name",
        "complainant_mobile",
        "complainant_cnic",
        "complainant_address",
        "district",
        "tehsil",
        "department",
        "category",
        "sub_category",
        "remarks",
    ):
        current = getattr(case, field_name)
        candidate = values.get(field_name)
        if not current and candidate:
            setattr(case, field_name, candidate)
        elif (
            field_name == "remarks"
            and current
            and candidate
            and len(candidate) > len(current)
            and _normalized_text(candidate).casefold().startswith(
                _normalized_text(current).casefold()
            )
        ):
            # This is not a conflicting OCR guess: it is a strict continuation
            # of the exact canonical prefix already accepted by the operator.
            case.remarks = candidate
    case.updated_at = utcnow()


def promote_case_observations(
    session: Session,
    complaint_case: ComplaintCase,
) -> dict[str, str]:
    """Seed blank canonical fields after a human accepts the evidence.

    OCR and PDF observations remain immutable audit records. This promotion is
    deliberately invoked by the review workflow, never by classification
    confidence alone, and it never overwrites an operator-edited value.
    """

    observations = list(
        session.exec(
            select(ComplaintFieldObservation).where(
                ComplaintFieldObservation.complaint_case_id == complaint_case.id
            )
        ).all()
    )
    values = _canonical_field_values(observations)
    _promote_safe_fields(complaint_case, values)
    session.add(complaint_case)
    return values


def _record_binary_duplicate_signal(
    session: Session,
    *,
    document: ComplaintDocument,
    case: ComplaintCase | None,
) -> ComplaintDocument | None:
    """Return the stable canonical binary when this capture is a duplicate.

    Review workflows may revisit already-persisted documents in a different
    order from capture time.  Always elect the earliest capture (with the UUID
    as a deterministic tie-breaker) instead of treating whichever document was
    processed first in this transaction as canonical.
    """

    if case is None or not document.source_sha256:
        return None
    canonical = session.exec(
        select(ComplaintDocument)
        .join(
            ComplaintDocumentCaseLink,
            ComplaintDocumentCaseLink.complaint_document_id == ComplaintDocument.id,
        )
        .where(
            ComplaintDocumentCaseLink.complaint_case_id == case.id,
            ComplaintDocument.source_sha256 == document.source_sha256,
        )
        .order_by(ComplaintDocument.created_at, ComplaintDocument.id)
    ).first()
    if canonical is None or canonical.id == document.id:
        return None
    canonical_link = session.exec(
        select(ComplaintDocumentCaseLink).where(
            ComplaintDocumentCaseLink.complaint_document_id == canonical.id,
            ComplaintDocumentCaseLink.complaint_case_id == case.id,
        )
    ).first()
    matched_case_id = (
        canonical_link.complaint_case_id
        if canonical_link
        else canonical.complaint_case_id
    )
    existing = session.exec(
        select(ComplaintMatch).where(
            ComplaintMatch.complaint_case_id == case.id,
            ComplaintMatch.processing_item_id == document.source_processing_item_id,
            ComplaintMatch.proposed_decision == "exact_file_duplicate",
        )
    ).first()
    if existing is not None:
        return canonical
    session.add(
        ComplaintMatch(
            complaint_case_id=case.id,
            processing_item_id=document.source_processing_item_id,
            matched_case_id=matched_case_id,
            proposed_decision="exact_file_duplicate",
            score=1.0,
            reason="The captured binary SHA-256 exactly matches a previously captured document.",
            signals_json={
                "exact_source_sha256": True,
                "source_sha256": document.source_sha256,
                "matched_document_id": str(canonical.id),
            },
        )
    )
    return canonical


def record_captured_document(
    session: Session,
    captured: CapturedComplaintDocument,
    *,
    materialize_case: bool = True,
) -> tuple[ComplaintDocument, ComplaintCase | None, DocumentExtraction]:
    """Persist immutable evidence, materializing a case only after approval.

    Classification is allowed to preserve the source document and extraction,
    but it must not populate the durable complaint registry.  Review workflows
    opt into ``materialize_case`` after an explicit operator decision.
    """

    existing_document = session.exec(
        select(ComplaintDocument).where(
            ComplaintDocument.source_processing_item_id == captured.processing_item_id
        )
    ).first()
    complaint_number = normalize_complaint_number(captured.complaint_number)
    case = _get_or_create_case(session, complaint_number) if materialize_case else None

    role = _document_role(captured.category)
    relationship_reason = "; ".join(captured.classification_evidence[:8]) or None
    if existing_document is None:
        document = ComplaintDocument(
            complaint_case_id=case.id if case else None,
            source_processing_item_id=captured.processing_item_id,
            source_attachment_id=captured.attachment_id,
            source_message_id=captured.message_id,
            source_sha256=captured.source_sha256,
            original_filename=captured.original_filename,
            mime_type=captured.mime_type,
            role=role,
            relationship_confidence=captured.classification_confidence,
            relationship_reason=relationship_reason,
            review_state=(
                "proposed"
                if case and role in {"main_complaint", "complaint_details", "report"}
                else "pending"
            ),
        )
        session.add(document)
        session.flush()
    else:
        document = existing_document
        document.complaint_case_id = case.id if case else document.complaint_case_id
        document.role = role
        document.relationship_confidence = captured.classification_confidence
        document.relationship_reason = relationship_reason
        document.updated_at = utcnow()
        session.add(document)
    link = None
    if case:
        link = _link_case(
            session,
            document=document,
            case=case,
            role=role,
            confidence=captured.classification_confidence,
            reason=relationship_reason,
            source_locator="document",
        )
    duplicate_of = _record_binary_duplicate_signal(session, document=document, case=case)
    if duplicate_of is not None:
        reason = f"Exact binary duplicate of complaint document {duplicate_of.id}."
        document.duplicate_of_document_id = (
            duplicate_of.duplicate_of_document_id or duplicate_of.id
        )
        document.review_state = "duplicate"
        document.relationship_reason = reason
        document.updated_at = utcnow()
        session.add(document)
        if link is not None:
            link.review_state = "duplicate"
            link.confidence = 1.0
            link.reason = reason
            session.add(link)

    content_sha256 = hashlib.sha256(captured.extracted_text.encode("utf-8")).hexdigest()
    extraction = session.exec(
        select(DocumentExtraction).where(
            DocumentExtraction.complaint_document_id == document.id,
            DocumentExtraction.extractor_name == EXTRACTOR_NAME,
            DocumentExtraction.extractor_version == EXTRACTOR_VERSION,
            DocumentExtraction.content_sha256 == content_sha256,
        )
    ).first()
    if extraction is None:
        extraction = DocumentExtraction(
            complaint_document_id=document.id,
            extractor_name=EXTRACTOR_NAME,
            extractor_version=EXTRACTOR_VERSION,
            extraction_method=captured.extraction_method or "unknown",
            content_sha256=content_sha256,
            raw_text=captured.extracted_text,
            normalized_text=_normalized_text(captured.extracted_text),
            metadata_json=captured.extraction_metadata,
            quality_score=captured.classification_confidence,
            error=captured.extraction_error,
        )
        session.add(extraction)
        session.flush()

        observations: list[ComplaintFieldObservation] = []
        for field in extract_field_observations(captured.extracted_text):
            observation = ComplaintFieldObservation(
                complaint_case_id=case.id if case else None,
                extraction_id=extraction.id,
                field_name=field.field_name,
                raw_value=field.raw_value,
                normalized_value=field.normalized_value,
                confidence=field.confidence,
                source_locator=field.source_locator,
            )
            observations.append(observation)
            session.add(observation)
        structured_remarks = _normalized_text(
            str(captured.extraction_metadata.get("remarks_text") or "")
        )
        if role == "main_complaint" and structured_remarks:
            observation = ComplaintFieldObservation(
                complaint_case_id=case.id if case else None,
                extraction_id=extraction.id,
                field_name="remarks",
                raw_value=structured_remarks,
                normalized_value=structured_remarks,
                confidence=min(0.95, max(0.86, captured.classification_confidence)),
                source_locator="metadata:pdf-extractor:remarks_text",
            )
            observations.append(observation)
            session.add(observation)
        if (
            case
            and role == "main_complaint"
            and captured.classification_confidence >= 0.80
            and "tesseract" not in captured.extraction_method.casefold()
        ):
            _promote_safe_fields(case, _canonical_field_values(observations))
            session.add(case)

    elif case is not None:
        # The extraction was recorded while this source was only a candidate.
        # Attach its immutable observations to the newly approved case now.
        observations = list(
            session.exec(
                select(ComplaintFieldObservation).where(
                    ComplaintFieldObservation.extraction_id == extraction.id,
                    ComplaintFieldObservation.complaint_case_id.is_(None),
                )
            ).all()
        )
        for observation in observations:
            observation.complaint_case_id = case.id
            session.add(observation)
        if observations:
            _promote_safe_fields(case, _canonical_field_values(observations))
            session.add(case)

    return document, case, extraction


def record_paperless_match(
    session: Session,
    *,
    complaint_case: ComplaintCase,
    processing_item_id: object,
    category: str,
    reason: str,
    document_ids: list[int | str],
    statuses: list[str],
) -> ComplaintMatch:
    paperless_document_id = next(
        (int(value) for value in document_ids if str(value).isdigit()),
        None,
    )
    if category == "fresh":
        proposed = "fresh"
    elif category in {"submitted", "uploaded_not_relevant", "uploaded_pending"}:
        proposed = "existing"
    else:
        proposed = "review_required"
    existing = session.exec(
        select(ComplaintMatch).where(
            ComplaintMatch.complaint_case_id == complaint_case.id,
            ComplaintMatch.processing_item_id == processing_item_id,
            ComplaintMatch.paperless_document_id == paperless_document_id,
            ComplaintMatch.proposed_decision == proposed,
        )
    ).first()
    if existing is not None:
        return existing
    match = ComplaintMatch(
        complaint_case_id=complaint_case.id,
        processing_item_id=processing_item_id,
        paperless_document_id=paperless_document_id,
        proposed_decision=proposed,
        # A missing Paperless match is evidence, not an authorization to publish.
        # Only existing-document matches can be finalized automatically; a fresh
        # complaint always requires an explicit reviewer decision.
        final_decision=proposed if category in {"submitted", "uploaded_not_relevant", "uploaded_pending"} else None,
        score=1.0 if paperless_document_id is not None else 0.0,
        reason=reason,
        signals_json={
            "exact_complaint_number": bool(paperless_document_id),
            "paperless_category": category,
            "paperless_statuses": statuses,
        },
    )
    session.add(match)
    if paperless_document_id is not None:
        complaint_case.canonical_paperless_document_id = paperless_document_id
        complaint_case.state = "existing"
    elif category == "fresh":
        complaint_case.state = "review_required"
    else:
        complaint_case.state = "review_required"
    complaint_case.updated_at = utcnow()
    session.add(complaint_case)
    return match
