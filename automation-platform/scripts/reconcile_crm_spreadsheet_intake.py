#!/usr/bin/env python3
"""Backfill spreadsheet row ledgers and quarantine legacy row-only CRM cases.

The operation never deletes a case or mutates Paperless.  A dry run is the
default; ``--apply`` persists the reversible registry quarantine and row ledger.
"""

from __future__ import annotations

import argparse
import json

from sqlmodel import Session, select

from automation_core.database import engine
from automation_core.time import utcnow
from crm_domain.models import (
    ComplaintAuditEvent,
    ComplaintCase,
    ComplaintDocument,
    ComplaintDocumentCaseLink,
    ComplaintMatch,
    CrmDispatchItem,
    PaperlessPublication,
    CrmSpreadsheetIntakeBatch,
    CrmSpreadsheetIntakeRow,
)
from whatsapp_gateway.inbound.spreadsheet_intake import (
    PAPERLESS_EXISTING_CATEGORIES,
    persist_spreadsheet_intake,
    recalculate_spreadsheet_batch,
)
from whatsapp_gateway.models import (
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
)


QUARANTINE_REASON = "Legacy spreadsheet row was materialized before row approval."


def _is_spreadsheet_item(item: WhatsAppInboundProcessingItem) -> bool:
    metadata = item.extracted_metadata_json or {}
    return bool(
        item.primary_category == "spreadsheet"
        or metadata.get("spreadsheet_rows")
        or metadata.get("complaint_rows")
        or str(item.extraction_method or "").startswith("spreadsheet:")
    )


def _latest_paperless_match(
    session: Session, case: ComplaintCase
) -> ComplaintMatch | None:
    query = select(ComplaintMatch).where(ComplaintMatch.complaint_case_id == case.id)
    if case.canonical_paperless_document_id:
        query = query.where(
            ComplaintMatch.paperless_document_id
            == case.canonical_paperless_document_id
        )
    return session.exec(query.order_by(ComplaintMatch.created_at.desc())).first()


def _row_case(
    session: Session,
    *,
    document: ComplaintDocument | None,
    row: CrmSpreadsheetIntakeRow,
) -> ComplaintCase | None:
    if document is None:
        return None
    return session.exec(
        select(ComplaintCase)
        .join(
            ComplaintDocumentCaseLink,
            ComplaintDocumentCaseLink.complaint_case_id == ComplaintCase.id,
        )
        .where(
            ComplaintDocumentCaseLink.complaint_document_id == document.id,
            ComplaintDocumentCaseLink.source_locator == row.source_locator,
        )
    ).first()


def _has_protected_activity(session: Session, case: ComplaintCase) -> bool:
    checks = (
        select(PaperlessPublication.id).where(
            PaperlessPublication.complaint_case_id == case.id
        ),
        select(CrmDispatchItem.id).where(CrmDispatchItem.complaint_case_id == case.id),
        select(ComplaintAuditEvent.id).where(
            ComplaintAuditEvent.complaint_case_id == case.id
        ),
    )
    return any(session.exec(query).first() is not None for query in checks)


def _has_non_row_evidence(session: Session, case: ComplaintCase) -> bool:
    return (
        session.exec(
            select(ComplaintDocumentCaseLink.id).where(
                ComplaintDocumentCaseLink.complaint_case_id == case.id,
                ComplaintDocumentCaseLink.role != "source_row",
            )
        ).first()
        is not None
    )


def reconcile(session: Session) -> dict[str, int]:
    report = {
        "spreadsheet_items": 0,
        "batches_created": 0,
        "rows_backfilled": 0,
        "rows_with_paperless_observations": 0,
        "cases_quarantined": 0,
        "cases_preserved": 0,
    }
    items = list(session.exec(select(WhatsAppInboundProcessingItem)).all())
    for item in items:
        if not _is_spreadsheet_item(item):
            continue
        report["spreadsheet_items"] += 1
        batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
        existing_batch = session.exec(
            select(CrmSpreadsheetIntakeBatch).where(
                CrmSpreadsheetIntakeBatch.processing_item_id == item.id
            )
        ).first()
        before_rows = 0
        if existing_batch is not None:
            before_rows = len(
                session.exec(
                    select(CrmSpreadsheetIntakeRow.id).where(
                        CrmSpreadsheetIntakeRow.batch_id == existing_batch.id
                    )
                ).all()
            )
        batch = persist_spreadsheet_intake(
            session,
            item=item,
            batch_item=batch_item,
        )
        if existing_batch is None:
            report["batches_created"] += 1
        rows = list(
            session.exec(
                select(CrmSpreadsheetIntakeRow).where(
                    CrmSpreadsheetIntakeRow.batch_id == batch.id
                )
            ).all()
        )
        report["rows_backfilled"] += max(0, len(rows) - before_rows)
        document = session.exec(
            select(ComplaintDocument).where(
                ComplaintDocument.source_processing_item_id == item.id
            )
        ).first()
        for row in rows:
            case = _row_case(session, document=document, row=row)
            if case is None:
                continue
            match = _latest_paperless_match(session, case)
            signals = dict(match.signals_json or {}) if match else {}
            category = str(signals.get("paperless_category") or "")
            if category:
                row.paperless_category = category
                row.paperless_reason = match.reason
                row.paperless_document_ids = (
                    [match.paperless_document_id]
                    if match.paperless_document_id is not None
                    else []
                )
                row.paperless_statuses = list(
                    signals.get("paperless_statuses") or []
                )
                row.paperless_checked_at = match.created_at
                row.status = (
                    "existing"
                    if category in PAPERLESS_EXISTING_CATEGORIES
                    else "fresh" if category == "fresh" else "manual_review"
                )
                row.updated_at = utcnow()
                session.add(row)
                report["rows_with_paperless_observations"] += 1

            if _has_non_row_evidence(session, case) or _has_protected_activity(
                session, case
            ):
                report["cases_preserved"] += 1
                continue
            if case.registry_status != "quarantined":
                case.registry_status = "quarantined"
                case.quarantine_reason = QUARANTINE_REASON
                case.updated_at = utcnow()
                session.add(case)
                report["cases_quarantined"] += 1
        recalculate_spreadsheet_batch(session, batch)
    session.flush()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the repair. The default is a rolled-back dry run.",
    )
    args = parser.parse_args()
    with Session(engine) as session:
        report = reconcile(session)
        if args.apply:
            session.commit()
        else:
            session.rollback()
    print(json.dumps({"mode": "applied" if args.apply else "dry-run", **report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
