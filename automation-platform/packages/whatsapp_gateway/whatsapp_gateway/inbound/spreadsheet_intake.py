from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from typing import Any

from sqlmodel import Session, select

from automation_core.time import utcnow
from crm_domain.fields import extract_mapping_observations
from crm_domain.identifiers import normalize_complaint_number
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintFieldObservation,
    CrmSpreadsheetIntakeBatch,
    CrmSpreadsheetIntakeRow,
    DocumentExtraction,
)
from crm_domain.registry import promote_case_observations, record_paperless_match
from whatsapp_gateway.models import (
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
)


PAPERLESS_EXISTING_CATEGORIES = {
    "submitted",
    "uploaded_not_relevant",
    "uploaded_pending",
}


def _row_sha256(values: dict[str, Any]) -> str:
    payload = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _source_document(
    session: Session, item: WhatsAppInboundProcessingItem
) -> ComplaintDocument | None:
    return session.exec(
        select(ComplaintDocument).where(
            ComplaintDocument.source_processing_item_id == item.id
        )
    ).first()


def _rows_for_batch(
    session: Session, batch_id: uuid.UUID
) -> list[CrmSpreadsheetIntakeRow]:
    return list(
        session.exec(
            select(CrmSpreadsheetIntakeRow)
            .where(CrmSpreadsheetIntakeRow.batch_id == batch_id)
            .order_by(
                CrmSpreadsheetIntakeRow.sheet_name,
                CrmSpreadsheetIntakeRow.row_number,
            )
        ).all()
    )


def recalculate_spreadsheet_batch(
    session: Session, batch: CrmSpreadsheetIntakeBatch
) -> CrmSpreadsheetIntakeBatch:
    rows = _rows_for_batch(session, batch.id)
    counts = Counter(row.status for row in rows)
    batch.total_rows = len(rows)
    batch.candidate_rows = sum(
        counts[value] for value in ("candidate", "fresh", "existing", "manual_review")
    )
    batch.invalid_rows = counts["invalid"]
    batch.existing_rows = counts["existing"]
    batch.fresh_rows = counts["fresh"]
    batch.manual_review_rows = counts["manual_review"]
    batch.approved_rows = counts["approved"]
    batch.rejected_rows = counts["rejected"]
    decided = batch.approved_rows + batch.rejected_rows + batch.invalid_rows
    if batch.status not in {"failed", "rejected"}:
        batch.status = "completed" if rows and decided == len(rows) else "awaiting_review"
        batch.completed_at = utcnow() if batch.status == "completed" else None
    batch.updated_at = utcnow()
    session.add(batch)
    return batch


def persist_spreadsheet_intake(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    batch_item: WhatsAppInboundBatchItem | None,
) -> CrmSpreadsheetIntakeBatch:
    """Persist row candidates without creating or linking complaint cases."""

    batch = session.exec(
        select(CrmSpreadsheetIntakeBatch).where(
            CrmSpreadsheetIntakeBatch.processing_item_id == item.id
        )
    ).first()
    document = _source_document(session, item)
    if batch is None:
        batch = CrmSpreadsheetIntakeBatch(
            processing_item_id=item.id,
            run_id=item.run_id,
            source_document_id=document.id if document else None,
            source_filename=(batch_item.original_filename if batch_item else None)
            or "Spreadsheet",
            source_sha256=(batch_item.sha256 if batch_item else None),
            status="extracting",
        )
        session.add(batch)
        session.flush()
    elif document is not None and batch.source_document_id is None:
        batch.source_document_id = document.id

    metadata = dict(item.extracted_metadata_json or {})
    source_rows = metadata.get("spreadsheet_rows") or metadata.get("complaint_rows") or []
    if not isinstance(source_rows, list):
        source_rows = []
    for source in source_rows:
        if not isinstance(source, dict):
            continue
        sheet_name = str(source.get("sheet") or "Sheet")[:180]
        row_number = int(source.get("row_number") or 0)
        if row_number <= 0:
            continue
        source_locator = f"sheet:{sheet_name}:row:{row_number}"
        values = source.get("values") if isinstance(source.get("values"), dict) else {}
        complaint_number = normalize_complaint_number(source.get("complaint_number"))
        existing = session.exec(
            select(CrmSpreadsheetIntakeRow).where(
                CrmSpreadsheetIntakeRow.batch_id == batch.id,
                CrmSpreadsheetIntakeRow.source_locator == source_locator,
            )
        ).first()
        if existing is None:
            session.add(
                CrmSpreadsheetIntakeRow(
                    batch_id=batch.id,
                    processing_item_id=item.id,
                    sheet_name=sheet_name,
                    row_number=row_number,
                    source_locator=source_locator,
                    row_sha256=_row_sha256(values),
                    values_json=values,
                    complaint_number=complaint_number,
                    status="candidate" if complaint_number else "invalid",
                    paperless_category="not_checked",
                    paperless_reason=(
                        None
                        if complaint_number
                        else "The row does not contain exactly one valid CRM complaint number."
                    ),
                )
            )
        elif existing.status not in {"approved", "rejected"}:
            existing.values_json = values
            existing.row_sha256 = _row_sha256(values)
            existing.complaint_number = complaint_number
            existing.status = "candidate" if complaint_number else "invalid"
            existing.updated_at = utcnow()
            session.add(existing)
    session.flush()
    return recalculate_spreadsheet_batch(session, batch)


def spreadsheet_numbers_for_run(session: Session, run_id: uuid.UUID) -> set[str]:
    return {
        value
        for value in session.exec(
            select(CrmSpreadsheetIntakeRow.complaint_number)
            .join(
                CrmSpreadsheetIntakeBatch,
                CrmSpreadsheetIntakeBatch.id == CrmSpreadsheetIntakeRow.batch_id,
            )
            .where(
                CrmSpreadsheetIntakeBatch.run_id == run_id,
                CrmSpreadsheetIntakeRow.complaint_number.is_not(None),
            )
        ).all()
        if value
    }


def persist_spreadsheet_paperless_reconciliation(
    session: Session,
    *,
    run_id: uuid.UUID,
    paperless_requested: bool,
    paperless_results: dict[str, object] | None = None,
    paperless_error: str = "",
) -> None:
    batches = list(
        session.exec(
            select(CrmSpreadsheetIntakeBatch).where(
                CrmSpreadsheetIntakeBatch.run_id == run_id
            )
        ).all()
    )
    results = paperless_results or {}
    for batch in batches:
        batch.status = "checking_paperless" if paperless_requested else "awaiting_review"
        session.add(batch)
        for row in _rows_for_batch(session, batch.id):
            if row.status in {"approved", "rejected", "invalid"}:
                continue
            if not paperless_requested:
                row.paperless_category = "not_checked"
                row.status = "candidate"
                row.paperless_checked_at = None
                session.add(row)
                continue
            result = results.get(row.complaint_number or "")
            category = str(
                getattr(result, "category", "unavailable" if paperless_error else "fresh")
            )
            row.paperless_category = category
            row.paperless_reason = str(
                getattr(
                    result,
                    "reason",
                    paperless_error
                    or "No matching CRM main complaint was found in Paperless.",
                )
            )
            row.paperless_document_ids = list(
                getattr(result, "matched_document_ids", []) or []
            )
            row.paperless_statuses = list(getattr(result, "matched_statuses", []) or [])
            row.paperless_checked_at = utcnow()
            if category in PAPERLESS_EXISTING_CATEGORIES:
                row.status = "existing"
            elif category == "fresh":
                row.status = "fresh"
            else:
                row.status = "manual_review"
            row.updated_at = utcnow()
            session.add(row)
        recalculate_spreadsheet_batch(session, batch)


def reject_spreadsheet_item(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    reviewed_by: str,
    note: str | None,
) -> CrmSpreadsheetIntakeBatch | None:
    batch = session.exec(
        select(CrmSpreadsheetIntakeBatch).where(
            CrmSpreadsheetIntakeBatch.processing_item_id == item.id
        )
    ).first()
    if batch is None:
        return None
    for row in _rows_for_batch(session, batch.id):
        if row.status == "approved":
            continue
        row.status = "rejected"
        row.reviewed_by = reviewed_by
        row.reviewer_note = note
        row.reviewed_at = utcnow()
        row.updated_at = utcnow()
        session.add(row)
    batch.status = "rejected"
    batch.completed_at = utcnow()
    recalculate_spreadsheet_batch(session, batch)
    batch.status = "rejected"
    session.add(batch)
    return batch


def review_spreadsheet_row(
    session: Session,
    *,
    row: CrmSpreadsheetIntakeRow,
    decision: str,
    reviewed_by: str,
    note: str | None,
) -> ComplaintCase | None:
    if decision not in {"approved", "rejected"}:
        raise ValueError("Spreadsheet rows can only be approved or rejected")
    batch = session.get(CrmSpreadsheetIntakeBatch, row.batch_id)
    if batch is None:
        raise ValueError("Spreadsheet intake batch is unavailable")
    if batch.status == "rejected":
        raise ValueError("This workbook was rejected and its row decisions are closed")
    if row.status == "approved":
        if decision != "approved":
            raise ValueError("A promoted row cannot be rejected from intake")
        return (
            session.get(ComplaintCase, row.complaint_case_id)
            if row.complaint_case_id
            else None
        )
    if row.status == "rejected":
        if decision != "rejected":
            raise ValueError("A rejected row cannot be promoted without reopening it")
        return None
    if decision == "rejected":
        row.status = "rejected"
        row.reviewer_note = note
        row.reviewed_by = reviewed_by
        row.reviewed_at = utcnow()
        row.updated_at = utcnow()
        session.add(row)
        recalculate_spreadsheet_batch(session, batch)
        return None

    complaint_number = normalize_complaint_number(row.complaint_number)
    if complaint_number is None:
        raise ValueError("A valid complaint number is required before approval")
    complaint_case = session.exec(
        select(ComplaintCase).where(
            ComplaintCase.source_system == "crm_portal",
            ComplaintCase.complaint_number == complaint_number,
        )
    ).first()
    created = complaint_case is None
    if complaint_case is None:
        complaint_case = ComplaintCase(
            source_system="crm_portal",
            complaint_number=complaint_number,
            state="review_required",
            registry_status="active",
        )
        session.add(complaint_case)
        session.flush()
    else:
        complaint_case.registry_status = "active"
        complaint_case.quarantine_reason = None

    source_item = session.get(WhatsAppInboundProcessingItem, row.processing_item_id)
    if source_item is None:
        raise ValueError("The source processing item is unavailable")
    document = _source_document(session, source_item)
    if document is None:
        raise ValueError("The immutable spreadsheet document is unavailable")
    link = session.exec(
        select(ComplaintDocumentCaseLink).where(
            ComplaintDocumentCaseLink.complaint_document_id == document.id,
            ComplaintDocumentCaseLink.complaint_case_id == complaint_case.id,
            ComplaintDocumentCaseLink.source_locator == row.source_locator,
        )
    ).first()
    if link is None:
        link = ComplaintDocumentCaseLink(
            complaint_document_id=document.id,
            complaint_case_id=complaint_case.id,
            role="source_row",
            review_state="accepted",
            confidence=1.0,
            reason="Spreadsheet row explicitly approved by an operator.",
            source_locator=row.source_locator,
        )
    else:
        link.review_state = "accepted"
        link.confidence = 1.0
        link.reason = "Spreadsheet row explicitly approved by an operator."
    session.add(link)

    extraction = session.exec(
        select(DocumentExtraction)
        .where(DocumentExtraction.complaint_document_id == document.id)
        .order_by(DocumentExtraction.created_at.desc())
    ).first()
    if extraction is not None:
        has_observations = session.exec(
            select(ComplaintFieldObservation.id).where(
                ComplaintFieldObservation.extraction_id == extraction.id,
                ComplaintFieldObservation.complaint_case_id == complaint_case.id,
                ComplaintFieldObservation.source_locator.like(f"{row.source_locator}%"),
            )
        ).first()
        if has_observations is None:
            for field in extract_mapping_observations(
                row.values_json or {}, source_locator=row.source_locator
            ):
                session.add(
                    ComplaintFieldObservation(
                        complaint_case_id=complaint_case.id,
                        extraction_id=extraction.id,
                        field_name=field.field_name,
                        raw_value=field.raw_value,
                        normalized_value=field.normalized_value,
                        confidence=field.confidence,
                        source_locator=field.source_locator,
                    )
                )
            session.flush()
            promote_case_observations(session, complaint_case)

    if row.paperless_category not in {"", "not_checked"}:
        record_paperless_match(
            session,
            complaint_case=complaint_case,
            processing_item_id=row.processing_item_id,
            category=row.paperless_category,
            reason=row.paperless_reason or "Paperless row observation retained from intake.",
            document_ids=list(row.paperless_document_ids or []),
            statuses=list(row.paperless_statuses or []),
        )
    if not complaint_case.canonical_paperless_document_id:
        complaint_case.state = "review_required"
    complaint_case.updated_at = utcnow()
    session.add(complaint_case)

    row.status = "approved"
    row.complaint_case_id = complaint_case.id
    row.reviewer_note = note
    row.reviewed_by = reviewed_by
    row.reviewed_at = utcnow()
    row.updated_at = utcnow()
    session.add(row)
    session.add(
        ComplaintAuditEvent(
            complaint_case_id=complaint_case.id,
            entity_type="crm_spreadsheet_intake_row",
            entity_id=str(row.id),
            event_type="spreadsheet_row_materialized",
            state="succeeded",
            actor=reviewed_by,
            before_json={"registry_status": "missing" if created else "quarantined"},
            after_json={"registry_status": "active", "case_state": complaint_case.state},
            details_json={
                "batch_id": str(batch.id),
                "processing_item_id": str(row.processing_item_id),
                "source_locator": row.source_locator,
                "complaint_number": complaint_number,
            },
        )
    )
    recalculate_spreadsheet_batch(session, batch)
    return complaint_case


__all__ = [
    "persist_spreadsheet_intake",
    "persist_spreadsheet_paperless_reconciliation",
    "recalculate_spreadsheet_batch",
    "reject_spreadsheet_item",
    "review_spreadsheet_row",
    "spreadsheet_numbers_for_run",
]
