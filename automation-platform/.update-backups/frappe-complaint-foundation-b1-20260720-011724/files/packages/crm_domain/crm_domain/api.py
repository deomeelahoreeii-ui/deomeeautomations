from __future__ import annotations

import uuid
import hashlib

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field as PydanticField
from sqlalchemy import String, cast, func, or_
from sqlmodel import Session, select

from automation_core.database import get_session
from automation_core.celery_app import celery_app
from automation_core.time import utcnow
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintFieldObservation,
    ComplaintMatch,
    DocumentExtraction,
    PaperlessPublication,
)


router = APIRouter(prefix="/api/v1/crm/cases", tags=["crm-cases"])


class CaseReviewRequest(BaseModel):
    fields: dict[str, str | None] = PydanticField(default_factory=dict)
    accepted_document_ids: list[uuid.UUID] = PydanticField(default_factory=list)
    rejected_document_ids: list[uuid.UUID] = PydanticField(default_factory=list)


class DocumentAssociationRequest(BaseModel):
    role: str
    accepted: bool = True


class BackfillRequest(BaseModel):
    limit: int | None = PydanticField(default=None, ge=1, le=100_000)


class BatchPublicationRequest(BaseModel):
    case_ids: list[uuid.UUID] = PydanticField(min_length=1, max_length=200)


def _accepted_publication_documents(
    session: Session, case: ComplaintCase
) -> list[tuple[ComplaintDocument, ComplaintDocumentCaseLink]]:
    rows = list(
        session.exec(
            select(ComplaintDocument, ComplaintDocumentCaseLink)
            .join(
                ComplaintDocumentCaseLink,
                ComplaintDocumentCaseLink.complaint_document_id
                == ComplaintDocument.id,
            )
            .where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocumentCaseLink.review_state == "accepted",
                ComplaintDocumentCaseLink.role.in_(
                    ["main_complaint", "complaint_details", "attachment"]
                ),
            )
            .order_by(
                (ComplaintDocumentCaseLink.role == "main_complaint").desc(),
                ComplaintDocument.created_at,
            )
        ).all()
    )
    # A WhatsApp file can be captured more than once and linked under different
    # roles. Publishing identical bytes twice is unsafe: Paperless may return
    # the canonical document for the second upload and a later metadata PATCH
    # would then turn the main complaint into an attachment. Ordering above
    # makes the accepted main complaint win when hashes collide.
    unique: list[tuple[ComplaintDocument, ComplaintDocumentCaseLink]] = []
    seen_content: set[str] = set()
    for document, link in rows:
        content_key = document.source_sha256 or str(document.id)
        if content_key in seen_content:
            continue
        seen_content.add(content_key)
        unique.append((document, link))
    return unique


def _publication_blockers(session: Session, case: ComplaintCase) -> list[str]:
    blockers: list[str] = []
    normalized = normalize_complaint_number(case.complaint_number)
    if normalized is None or normalized != case.complaint_number:
        blockers.append("Confirm one valid complaint number")
    if not str(case.remarks or "").strip():
        blockers.append("Confirm the complete complaint remarks")
    if case.canonical_paperless_document_id:
        blockers.append(
            f"Already linked to Paperless #{case.canonical_paperless_document_id}"
        )
    documents = _accepted_publication_documents(session, case)
    main_documents = [
        document for document, link in documents if link.role == "main_complaint"
    ]
    if not main_documents:
        blockers.append("Accept one main complaint document")
    elif len(main_documents) > 1:
        blockers.append("Keep exactly one accepted main complaint document")
    if any(document.review_state == "duplicate" for document in main_documents):
        blockers.append("The main complaint cannot be a duplicate capture")
    return blockers


def _case_summary(
    case: ComplaintCase,
    document_count: int,
    publication_blockers: list[str] | None = None,
    publication_error: str | None = None,
) -> dict[str, object]:
    blockers = publication_blockers or []
    return {
        "id": str(case.id),
        "source_system": case.source_system,
        "complaint_number": case.complaint_number,
        "state": case.state,
        "complainant_name": case.complainant_name,
        "complainant_mobile": case.complainant_mobile,
        "complainant_cnic": case.complainant_cnic,
        "district": case.district,
        "tehsil": case.tehsil,
        "department": case.department,
        "category": case.category,
        "sub_category": case.sub_category,
        "paperless_document_id": case.canonical_paperless_document_id,
        "document_count": document_count,
        "publication_ready": case.state == "fresh" and not blockers,
        "publication_blockers": blockers,
        "publication_error": publication_error,
        "created_at": case.created_at,
        "updated_at": case.updated_at,
    }


def _latest_publication_error(session: Session, case_id: uuid.UUID) -> str | None:
    publication = session.exec(
        select(PaperlessPublication)
        .where(
            PaperlessPublication.complaint_case_id == case_id,
            PaperlessPublication.last_error.is_not(None),
        )
        .order_by(PaperlessPublication.updated_at.desc())
    ).first()
    return publication.last_error if publication else None


@router.get("")
def list_complaint_cases(
    search: str = Query(default="", max_length=120),
    state: str = Query(default="", max_length=40),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> dict[str, object]:
    filters = []
    if state:
        filters.append(ComplaintCase.state == state)
    if search.strip():
        term = f"%{search.strip()}%"
        filters.append(
            or_(
                ComplaintCase.complaint_number.ilike(term),
                ComplaintCase.complainant_name.ilike(term),
                ComplaintCase.complainant_mobile.ilike(term),
                ComplaintCase.complainant_cnic.ilike(term),
            )
        )
    total = session.exec(select(func.count()).select_from(ComplaintCase).where(*filters)).one()
    rows = session.exec(
        select(
            ComplaintCase,
            func.count(
                func.distinct(
                    func.coalesce(
                        ComplaintDocument.source_sha256,
                        cast(ComplaintDocument.id, String),
                    )
                )
            ),
        )
        .join(
            ComplaintDocumentCaseLink,
            ComplaintDocumentCaseLink.complaint_case_id == ComplaintCase.id,
            isouter=True,
        )
        .join(
            ComplaintDocument,
            ComplaintDocument.id == ComplaintDocumentCaseLink.complaint_document_id,
            isouter=True,
        )
        .where(*filters)
        .group_by(ComplaintCase.id)
        .order_by(ComplaintCase.updated_at.desc())
        .offset(offset)
        .limit(limit)
    ).all()
    return {
        "items": [
            _case_summary(
                case,
                int(document_count),
                _publication_blockers(session, case),
                _latest_publication_error(session, case.id),
            )
            for case, document_count in rows
        ],
        "total": int(total),
        "limit": limit,
        "offset": offset,
}


@router.post("/publication-batches", status_code=202)
def queue_case_publication_batch(
    payload: BatchPublicationRequest,
    session: Session = Depends(get_session),
) -> dict[str, object]:
    case_ids = list(dict.fromkeys(payload.case_ids))
    cases = [session.get(ComplaintCase, case_id) for case_id in case_ids]
    missing = [str(case_id) for case_id, case in zip(case_ids, cases) if case is None]
    if missing:
        raise HTTPException(
            status_code=404,
            detail=f"Complaint cases were not found: {', '.join(missing)}",
        )
    validation: dict[str, list[str]] = {}
    for case in cases:
        assert case is not None
        blockers = _publication_blockers(session, case)
        if case.state != "fresh":
            blockers.insert(0, "Case is not approved for publication")
        if blockers:
            validation[str(case.id)] = blockers
    if validation:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Some selected cases are not ready for Paperless publication",
                "cases": validation,
            },
        )

    staged: list[ComplaintCase] = []
    for case in cases:
        assert case is not None
        _stage_case_publications(session, case)
        staged.append(case)
    session.commit()
    tasks = [
        celery_app.send_task(
            "crm_domain.publish_complaint_case",
            args=[str(case.id)],
            queue="crm",
        )
        for case in staged
    ]
    return {
        "queued_count": len(staged),
        "case_ids": [str(case.id) for case in staged],
        "task_ids": [task.id for task in tasks],
    }


@router.get("/statistics")
def complaint_case_statistics(
    session: Session = Depends(get_session),
) -> dict[str, int]:
    cases = list(session.exec(select(ComplaintCase)).all())
    state_counts: dict[str, int] = {}
    for case in cases:
        state_counts[case.state] = state_counts.get(case.state, 0) + 1
    approved_cases = [
        case for case in cases if case.state in {"fresh", "publishing", "published"}
    ]
    awaiting_publication = [case for case in cases if case.state == "fresh"]
    ready_to_publish = sum(
        not _publication_blockers(session, case) for case in awaiting_publication
    )
    return {
        "unique_cases": len(cases),
        "needs_review": state_counts.get("candidate", 0)
        + state_counts.get("review_required", 0),
        "approved_cases": len(approved_cases),
        "awaiting_publication": len(awaiting_publication),
        "ready_to_publish": ready_to_publish,
        "blocked_from_publication": len(awaiting_publication) - ready_to_publish,
        "publishing": state_counts.get("publishing", 0),
        "published": state_counts.get("published", 0),
        "existing": state_counts.get("existing", 0),
        "rejected": state_counts.get("rejected", 0),
    }


@router.get("/{case_id}")
def read_complaint_case(
    case_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, object]:
    case = session.get(ComplaintCase, case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Complaint case not found")
    document_links = list(
        session.exec(
            select(ComplaintDocument, ComplaintDocumentCaseLink)
            .join(
                ComplaintDocumentCaseLink,
                ComplaintDocumentCaseLink.complaint_document_id == ComplaintDocument.id,
            )
            .where(ComplaintDocumentCaseLink.complaint_case_id == case.id)
            .order_by(ComplaintDocument.created_at)
        ).all()
    )
    documents = [document for document, _link in document_links]
    canonical_by_binary: dict[str, uuid.UUID] = {}
    duplicate_of: dict[uuid.UUID, uuid.UUID] = {}
    capture_counts: dict[uuid.UUID, int] = {}
    for document in documents:
        binary_key = document.source_sha256 or f"document:{document.id}"
        canonical_id = canonical_by_binary.setdefault(binary_key, document.id)
        capture_counts[canonical_id] = capture_counts.get(canonical_id, 0) + 1
        if canonical_id != document.id:
            duplicate_of[document.id] = canonical_id
    document_ids = [document.id for document in documents]
    extractions = list(
        session.exec(
            select(DocumentExtraction)
            .where(DocumentExtraction.complaint_document_id.in_(document_ids))
            .order_by(DocumentExtraction.created_at)
        ).all()
    ) if document_ids else []
    extraction_ids = [extraction.id for extraction in extractions]
    observations = list(
        session.exec(
            select(ComplaintFieldObservation)
            .where(ComplaintFieldObservation.extraction_id.in_(extraction_ids))
            .order_by(ComplaintFieldObservation.created_at)
        ).all()
    ) if extraction_ids else []
    matches = list(
        session.exec(
            select(ComplaintMatch)
            .where(ComplaintMatch.complaint_case_id == case.id)
            .order_by(ComplaintMatch.created_at.desc())
        ).all()
    )
    publications = list(
        session.exec(
            select(PaperlessPublication)
            .where(PaperlessPublication.complaint_case_id == case.id)
            .order_by(PaperlessPublication.created_at.desc())
        ).all()
    )
    return {
        **_case_summary(
            case,
            len(canonical_by_binary),
            _publication_blockers(session, case),
        ),
        "capture_count": len(documents),
        "complainant_address": case.complainant_address,
        "remarks": case.remarks,
        "documents": [
            {
                **document.model_dump(mode="json"),
                "role": link.role,
                "review_state": link.review_state,
                "relationship_confidence": link.confidence,
                "relationship_reason": link.reason,
                "source_locator": link.source_locator,
                "duplicate_of_document_id": (
                    str(duplicate_of[document.id])
                    if document.id in duplicate_of
                    else None
                ),
                "duplicate_capture_count": capture_counts.get(document.id, 0),
            }
            for document, link in document_links
        ],
        "extractions": [
            {
                **extraction.model_dump(mode="json", exclude={"raw_text", "normalized_text"}),
                "text_preview": extraction.raw_text[:2_000],
                "text_truncated": len(extraction.raw_text) > 2_000,
            }
            for extraction in extractions
        ],
        "field_observations": [item.model_dump(mode="json") for item in observations],
        "matches": [item.model_dump(mode="json") for item in matches],
        "paperless_publications": [item.model_dump(mode="json") for item in publications],
    }


@router.patch("/{case_id}/review")
def review_complaint_case(
    case_id: uuid.UUID,
    payload: CaseReviewRequest,
    session: Session = Depends(get_session),
) -> dict[str, object]:
    case = session.get(ComplaintCase, case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Complaint case not found")
    allowed = {
        "complainant_name", "complainant_mobile", "complainant_cnic",
        "complainant_address", "district", "tehsil", "department", "category",
        "sub_category", "remarks",
    }
    unknown = set(payload.fields).difference(allowed)
    if unknown:
        raise HTTPException(status_code=422, detail=f"Unsupported complaint fields: {sorted(unknown)}")
    for name, value in payload.fields.items():
        setattr(case, name, value.strip() if isinstance(value, str) else value)
    for document_id in payload.accepted_document_ids:
        document = session.get(ComplaintDocument, document_id)
        linked = session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocumentCaseLink.complaint_document_id == document_id,
            )
        ).first()
        if document and linked:
            linked.review_state = "accepted"
            document.updated_at = utcnow()
            session.add(linked)
            session.add(document)
    for document_id in payload.rejected_document_ids:
        document = session.get(ComplaintDocument, document_id)
        linked = session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocumentCaseLink.complaint_document_id == document_id,
            )
        ).first()
        if document and linked:
            linked.review_state = "rejected"
            document.updated_at = utcnow()
            session.add(linked)
            session.add(document)
    case.state = "review_required"
    case.version += 1
    case.updated_at = utcnow()
    session.add(case)
    session.commit()
    return {"id": str(case.id), "state": case.state, "version": case.version}


@router.patch("/{case_id}/documents/{document_id}")
def review_document_association(
    case_id: uuid.UUID,
    document_id: uuid.UUID,
    payload: DocumentAssociationRequest,
    session: Session = Depends(get_session),
) -> dict[str, object]:
    if payload.role not in {"main_complaint", "complaint_details", "attachment", "reply", "report"}:
        raise HTTPException(status_code=422, detail="Unsupported complaint document role")
    case = session.get(ComplaintCase, case_id)
    document = session.get(ComplaintDocument, document_id)
    if case is None or document is None:
        raise HTTPException(status_code=404, detail="Complaint case or document not found")
    link = session.exec(
        select(ComplaintDocumentCaseLink).where(
            ComplaintDocumentCaseLink.complaint_case_id == case.id,
            ComplaintDocumentCaseLink.complaint_document_id == document.id,
        )
    ).first()
    if link is None:
        link = ComplaintDocumentCaseLink(
            complaint_case_id=case.id,
            complaint_document_id=document.id,
            role=payload.role,
            review_state="accepted" if payload.accepted else "rejected",
            confidence=1.0,
            reason="Manually associated during CRM review.",
            source_locator="review:manual",
        )
    else:
        link.role = payload.role
        link.review_state = "accepted" if payload.accepted else "rejected"
        link.confidence = 1.0
        link.reason = "Confirmed during CRM review."
    document.updated_at = utcnow()
    case.state = "review_required"
    case.version += 1
    case.updated_at = utcnow()
    session.add(link)
    session.add(document)
    session.add(case)
    session.commit()
    return {
        "document_id": str(document.id),
        "role": link.role,
        "review_state": link.review_state,
    }


@router.post("/{case_id}/approve-fresh")
def approve_fresh_complaint(
    case_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, object]:
    case = session.get(ComplaintCase, case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Complaint case not found")
    if case.canonical_paperless_document_id:
        raise HTTPException(status_code=409, detail="An existing Paperless complaint is already linked")
    normalized = normalize_complaint_number(case.complaint_number)
    if normalized is None or normalized != case.complaint_number:
        raise HTTPException(status_code=409, detail="Confirm one valid complaint number first")
    if not str(case.remarks or "").strip():
        raise HTTPException(status_code=409, detail="Confirm the complete complaint remarks first")
    main = session.exec(
        select(ComplaintDocument)
        .join(ComplaintDocumentCaseLink, ComplaintDocumentCaseLink.complaint_document_id == ComplaintDocument.id)
        .where(
            ComplaintDocumentCaseLink.complaint_case_id == case.id,
            ComplaintDocumentCaseLink.role == "main_complaint",
            ComplaintDocumentCaseLink.review_state == "accepted",
        )
    ).first()
    if main is None:
        raise HTTPException(status_code=409, detail="Accept one main complaint document first")
    case.state = "fresh"
    case.version += 1
    case.updated_at = utcnow()
    matches = list(
        session.exec(
            select(ComplaintMatch).where(
                ComplaintMatch.complaint_case_id == case.id,
                ComplaintMatch.proposed_decision == "fresh",
                ComplaintMatch.final_decision.is_(None),
            )
        )
    )
    for match in matches:
        match.final_decision = "fresh"
        session.add(match)
    session.add(case)
    session.commit()
    return {"id": str(case.id), "state": case.state}


def _stage_case_publications(session: Session, case: ComplaintCase) -> None:
    document_links = _accepted_publication_documents(session, case)
    for document, link in document_links:
        content_key = document.source_sha256 or str(document.id)
        key_source = f"{case.id}:{content_key}:v{case.version}"
        key = hashlib.sha256(key_source.encode()).hexdigest()
        existing = session.exec(
            select(PaperlessPublication).where(
                PaperlessPublication.idempotency_key == key
            )
        ).first()
        if existing is None:
            session.add(
                PaperlessPublication(
                    complaint_case_id=case.id,
                    complaint_document_id=document.id,
                    idempotency_key=key,
                    intended_fields_json={
                        "case_version": case.version,
                        "role": link.role,
                    },
                )
            )
    case.state = "publishing"
    case.updated_at = utcnow()
    session.add(case)


@router.post("/{case_id}/publish", status_code=202)
def queue_case_publication(
    case_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, object]:
    case = session.get(ComplaintCase, case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Complaint case not found")
    if case.state != "fresh":
        raise HTTPException(status_code=409, detail="Approve this case as fresh before publishing")
    blockers = _publication_blockers(session, case)
    if blockers:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Case is not ready for Paperless publication",
                "blockers": blockers,
            },
        )
    _stage_case_publications(session, case)
    session.commit()
    task = celery_app.send_task("crm_domain.publish_complaint_case", args=[str(case.id)], queue="crm")
    return {"case_id": str(case.id), "state": case.state, "task_id": task.id}


@router.post("/backfill-paperless", status_code=202)
def queue_paperless_backfill(payload: BackfillRequest) -> dict[str, object]:
    task = celery_app.send_task(
        "crm_domain.backfill_paperless_cases",
        args=[payload.limit],
        queue="crm",
    )
    return {"task_id": task.id, "status": "queued"}


@router.get("/{case_id}/extractions/{extraction_id}/text")
def read_extraction_text(
    case_id: uuid.UUID,
    extraction_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, object]:
    extraction = session.exec(
        select(DocumentExtraction)
        .join(ComplaintDocument, ComplaintDocument.id == DocumentExtraction.complaint_document_id)
        .join(
            ComplaintDocumentCaseLink,
            ComplaintDocumentCaseLink.complaint_document_id == ComplaintDocument.id,
        )
        .where(
            DocumentExtraction.id == extraction_id,
            ComplaintDocumentCaseLink.complaint_case_id == case_id,
        )
    ).first()
    if extraction is None:
        raise HTTPException(status_code=404, detail="Complaint extraction not found")
    return {
        "id": str(extraction.id),
        "complaint_document_id": str(extraction.complaint_document_id),
        "content_sha256": extraction.content_sha256,
        "extraction_method": extraction.extraction_method,
        "raw_text": extraction.raw_text,
        "normalized_text": extraction.normalized_text,
    }
