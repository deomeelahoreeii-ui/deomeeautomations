from __future__ import annotations

from typing import Any

from crm_domain.models import CrmSpreadsheetIntakeBatch, CrmSpreadsheetIntakeRow


def serialize_spreadsheet_batch(batch: CrmSpreadsheetIntakeBatch) -> dict[str, Any]:
    return {
        "id": str(batch.id),
        "processing_item_id": str(batch.processing_item_id),
        "run_id": str(batch.run_id),
        "source_document_id": (
            str(batch.source_document_id) if batch.source_document_id else None
        ),
        "source_filename": batch.source_filename,
        "source_sha256": batch.source_sha256,
        "status": batch.status,
        "total_rows": batch.total_rows,
        "candidate_rows": batch.candidate_rows,
        "invalid_rows": batch.invalid_rows,
        "existing_rows": batch.existing_rows,
        "fresh_rows": batch.fresh_rows,
        "manual_review_rows": batch.manual_review_rows,
        "approved_rows": batch.approved_rows,
        "rejected_rows": batch.rejected_rows,
        "error": batch.error,
        "created_at": batch.created_at,
        "updated_at": batch.updated_at,
        "completed_at": batch.completed_at,
    }


def serialize_spreadsheet_row(row: CrmSpreadsheetIntakeRow) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "batch_id": str(row.batch_id),
        "processing_item_id": str(row.processing_item_id),
        "sheet_name": row.sheet_name,
        "row_number": row.row_number,
        "source_locator": row.source_locator,
        "row_sha256": row.row_sha256,
        "values": row.values_json or {},
        "complaint_number": row.complaint_number,
        "status": row.status,
        "paperless_category": row.paperless_category,
        "paperless_reason": row.paperless_reason,
        "paperless_document_ids": row.paperless_document_ids or [],
        "paperless_statuses": row.paperless_statuses or [],
        "paperless_checked_at": row.paperless_checked_at,
        "complaint_case_id": (
            str(row.complaint_case_id) if row.complaint_case_id else None
        ),
        "reviewer_note": row.reviewer_note,
        "reviewed_by": row.reviewed_by,
        "reviewed_at": row.reviewed_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


__all__ = ["serialize_spreadsheet_batch", "serialize_spreadsheet_row"]
