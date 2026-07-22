from __future__ import annotations

import uuid
from typing import Any, Literal

from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from automation_core.database import get_session
from crm_domain.models import CrmSpreadsheetIntakeBatch, CrmSpreadsheetIntakeRow
from whatsapp_gateway.inbound.spreadsheet_intake import (
    review_spreadsheet_row,
)
from whatsapp_gateway.inbound.spreadsheet_serialization import (
    serialize_spreadsheet_batch,
    serialize_spreadsheet_row,
)
from whatsapp_gateway.models import WhatsAppInboundProcessingRun


class ReviewSpreadsheetRowRequest(BaseModel):
    decision: Literal["approved", "rejected"]
    reviewed_by: str = Field(default="web-operator", min_length=1, max_length=120)
    note: str | None = Field(default=None, max_length=4000)


def list_spreadsheet_intake_batches(
    run_id: uuid.UUID | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status", max_length=32),
    search: str | None = Query(default=None, max_length=160),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    query = select(CrmSpreadsheetIntakeBatch)
    if run_id:
        query = query.where(CrmSpreadsheetIntakeBatch.run_id == run_id)
    if status_filter:
        query = query.where(CrmSpreadsheetIntakeBatch.status == status_filter)
    batches = list(
        session.exec(
            query.order_by(CrmSpreadsheetIntakeBatch.created_at.desc())
        ).all()
    )
    term = (search or "").strip().casefold()
    items: list[dict[str, Any]] = []
    for batch in batches:
        run = session.get(WhatsAppInboundProcessingRun, batch.run_id)
        view = serialize_spreadsheet_batch(batch)
        view["run_code"] = run.run_code if run else None
        view["review_url"] = f"/crm/intake/spreadsheets/{batch.id}"
        view["source_url"] = (
            f"/api/v1/whatsapp/inbound/processing-items/{batch.processing_item_id}/content"
        )
        if term and term not in " ".join(
            str(value or "").casefold()
            for value in (batch.source_filename, view["run_code"], batch.status)
        ):
            continue
        items.append(view)
    total = len(items)
    return {
        "items": items[offset : offset + limit],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def read_spreadsheet_intake_batch(
    batch_id: uuid.UUID,
    status_filter: str | None = Query(default=None, alias="status", max_length=32),
    paperless_category: str | None = Query(default=None, max_length=40),
    search: str | None = Query(default=None, max_length=160),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    batch = session.get(CrmSpreadsheetIntakeBatch, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Spreadsheet intake batch not found")
    query = select(CrmSpreadsheetIntakeRow).where(
        CrmSpreadsheetIntakeRow.batch_id == batch.id
    )
    if status_filter:
        query = query.where(CrmSpreadsheetIntakeRow.status == status_filter)
    if paperless_category:
        query = query.where(
            CrmSpreadsheetIntakeRow.paperless_category == paperless_category
        )
    rows = list(
        session.exec(
            query.order_by(
                CrmSpreadsheetIntakeRow.sheet_name,
                CrmSpreadsheetIntakeRow.row_number,
            )
        ).all()
    )
    term = (search or "").strip().casefold()
    items = []
    for row in rows:
        view = serialize_spreadsheet_row(row)
        if term and term not in " ".join(
            [
                str(row.complaint_number or ""),
                row.sheet_name,
                row.source_locator,
                json_value(row.values_json),
            ]
        ).casefold():
            continue
        items.append(view)
    total = len(items)
    view = serialize_spreadsheet_batch(batch)
    view["source_url"] = (
        f"/api/v1/whatsapp/inbound/processing-items/{batch.processing_item_id}/content"
    )
    return {
        "batch": view,
        "items": items[offset : offset + limit],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def json_value(value: dict[str, Any] | None) -> str:
    return " ".join(str(item) for pair in (value or {}).items() for item in pair)


def review_spreadsheet_intake_row(
    row_id: uuid.UUID,
    data: ReviewSpreadsheetRowRequest,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    row = session.get(CrmSpreadsheetIntakeRow, row_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Spreadsheet intake row not found")
    try:
        complaint_case = review_spreadsheet_row(
            session,
            row=row,
            decision=data.decision,
            reviewed_by=data.reviewed_by,
            note=data.note,
        )
        session.commit()
        session.refresh(row)
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "row": serialize_spreadsheet_row(row),
        "case": (
            {
                "id": str(complaint_case.id),
                "complaint_number": complaint_case.complaint_number,
                "state": complaint_case.state,
                "registry_status": complaint_case.registry_status,
            }
            if complaint_case
            else None
        ),
    }


__all__ = [
    "ReviewSpreadsheetRowRequest",
    "list_spreadsheet_intake_batches",
    "read_spreadsheet_intake_batch",
    "review_spreadsheet_intake_row",
]
