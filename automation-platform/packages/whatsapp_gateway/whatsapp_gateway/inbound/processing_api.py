from __future__ import annotations

import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import BackgroundTasks, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.object_storage import S3ObjectStorage
from crm_domain.identifiers import normalize_complaint_number
from whatsapp_gateway.inbound.processing import (
    complaint_group_summary,
    create_processing_run,
    processing_counts,
    recalculate_processing_run,
    serialize_processing_event,
    serialize_processing_item,
    serialize_processing_run,
    update_review_decision,
)
from whatsapp_gateway.inbound.processing_tasks import process_inbound_batch
from whatsapp_gateway.inbound.crm_review import (
    approve_manual_item,
    decide_complaint_group,
)
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingEvent,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
    WhatsAppInboundStoredObject,
)


class CreateProcessingRunRequest(BaseModel):
    batch_id: uuid.UUID
    paperless_check: bool = True


class ReviewProcessingItemRequest(BaseModel):
    decision: Literal["pending", "approved", "rejected", "deferred"]
    reviewed_by: str = Field(default="web-operator", min_length=1, max_length=100)
    note: str | None = Field(default=None, max_length=4000)
    category: str | None = Field(default=None, max_length=80)
    complaint_number: str | None = Field(default=None, max_length=40)


class ReviewComplaintGroupRequest(BaseModel):
    decision: Literal["approved", "rejected"]
    reviewed_by: str = Field(default="web-operator", min_length=1, max_length=100)
    note: str | None = Field(default=None, max_length=4000)


class BatchReviewComplaintGroupsRequest(BaseModel):
    complaint_numbers: list[str] = Field(min_length=1, max_length=200)
    reviewed_by: str = Field(default="web-operator", min_length=1, max_length=100)
    note: str | None = Field(default=None, max_length=4000)


def create_inbound_processing_run(
    data: CreateProcessingRunRequest,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    batch = session.get(WhatsAppInboundBatch, data.batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Inbound batch not found")
    existing = session.exec(
        select(WhatsAppInboundProcessingRun)
        .where(WhatsAppInboundProcessingRun.batch_id == batch.id)
        .where(WhatsAppInboundProcessingRun.status.notin_(["failed", "cancelled"]))
        .order_by(WhatsAppInboundProcessingRun.created_at.desc())
    ).first()
    if existing is not None:
        return {
            "processing_run": serialize_processing_run(session, existing),
            "task_id": None,
            "reused": True,
        }
    try:
        run = create_processing_run(
            session,
            batch=batch,
            settings=settings,
            paperless_check=data.paperless_check,
        )
        session.commit()
        session.refresh(run)
        if run.status == "queued":
            task = process_inbound_batch.delay(str(run.id))
            return {
                "processing_run": serialize_processing_run(session, run),
                "task_id": task.id,
                "reused": False,
            }
        return {
            "processing_run": serialize_processing_run(session, run),
            "task_id": None,
            "reused": False,
        }
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


def list_inbound_processing_runs(
    status_filter: str | None = Query(default=None, alias="status"),
    category: str | None = Query(default=None),
    search: str | None = Query(default=None, max_length=120),
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    query = select(WhatsAppInboundProcessingRun)
    if status_filter:
        query = query.where(WhatsAppInboundProcessingRun.status == status_filter)
    rows = list(
        session.exec(
            query.order_by(WhatsAppInboundProcessingRun.created_at.desc()).limit(limit)
        ).all()
    )
    term = (search or "").strip().casefold()
    items: list[dict[str, Any]] = []
    for run in rows:
        view = serialize_processing_run(session, run)
        if category:
            category_count = {
                "crm_complaint": run.crm_complaints,
                "possible_crm_complaint": run.possible_crm,
                "crm_supporting_document": run.supporting_documents,
                "crm_reply_or_report": run.reply_reports,
                "duplicate_in_paperless": run.duplicate_items,
                "eligible": run.eligible_items,
                "needs_review": run.review_items,
                "failed": run.failed_items,
            }.get(category, 0)
            if not category_count:
                continue
        if term and term not in " ".join(
            str(view.get(key) or "").casefold()
            for key in ("run_code", "batch_code", "contact_name", "contact_identity", "status")
        ):
            continue
        items.append(view)
    return {"items": items, "counts": processing_counts(session)}


def read_inbound_processing_run(
    run_id: uuid.UUID,
    category: str | None = Query(default=None),
    review_status: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    run = session.get(WhatsAppInboundProcessingRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Inbound processing run not found")
    query = select(WhatsAppInboundProcessingItem).where(
        WhatsAppInboundProcessingItem.run_id == run.id
    )
    if category:
        if category == "crm":
            query = query.where(
                WhatsAppInboundProcessingItem.primary_category.in_(
                    [
                        "crm_complaint",
                        "possible_crm_complaint",
                        "crm_supporting_document",
                        "crm_reply_or_report",
                    ]
                )
            )
        elif category == "duplicate_in_paperless":
            query = query.where(WhatsAppInboundProcessingItem.status == category)
        elif category == "needs_review":
            query = query.where(WhatsAppInboundProcessingItem.status == category)
        elif category == "eligible":
            query = query.where(WhatsAppInboundProcessingItem.status.in_(["eligible", "approved"]))
        else:
            query = query.where(WhatsAppInboundProcessingItem.primary_category == category)
    if review_status:
        query = query.where(WhatsAppInboundProcessingItem.review_status == review_status)
    items = list(
        session.exec(query.order_by(WhatsAppInboundProcessingItem.created_at)).all()
    )
    return {
        **serialize_processing_run(session, run),
        "complaint_groups": complaint_group_summary(session, run.id),
        "items": [serialize_processing_item(session, item) for item in items],
    }


def list_inbound_processing_events(
    run_id: uuid.UUID,
    after: datetime | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    if session.get(WhatsAppInboundProcessingRun, run_id) is None:
        raise HTTPException(status_code=404, detail="Inbound processing run not found")
    query = select(WhatsAppInboundProcessingEvent).where(
        WhatsAppInboundProcessingEvent.run_id == run_id
    )
    if after is not None:
        query = query.where(WhatsAppInboundProcessingEvent.created_at > after)
    events = session.exec(
        query.order_by(WhatsAppInboundProcessingEvent.created_at.asc()).limit(limit)
    ).all()
    return {"items": [serialize_processing_event(event) for event in events]}


def review_inbound_processing_item(
    item_id: uuid.UUID,
    data: ReviewProcessingItemRequest,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    item = session.get(WhatsAppInboundProcessingItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Processing item not found")
    try:
        approval = None
        if data.decision == "approved":
            approval = approve_manual_item(
                session,
                item=item,
                complaint_number=data.complaint_number or item.detected_complaint_number or "",
                category=data.category or item.primary_category,
                reviewed_by=data.reviewed_by,
                note=data.note,
            )
        else:
            update_review_decision(
                session,
                item=item,
                decision=data.decision,
                reviewed_by=data.reviewed_by,
                note=data.note,
                category=data.category,
                complaint_number=data.complaint_number,
            )
        session.commit()
        session.refresh(item)
        response = serialize_processing_item(session, item)
        if approval is not None:
            response["case_resolution"] = {
                "case_id": str(approval.case.id),
                "complaint_number": approval.case.complaint_number,
                "case_state": approval.case.state,
                "case_created": approval.case_created,
                "document_role": approval.role,
            }
        return response
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def review_inbound_complaint_group(
    run_id: uuid.UUID,
    complaint_number: str,
    data: ReviewComplaintGroupRequest,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    run = session.get(WhatsAppInboundProcessingRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Inbound processing run not found")
    try:
        case = decide_complaint_group(
            session,
            run=run,
            complaint_number=complaint_number,
            decision=data.decision,
            reviewed_by=data.reviewed_by,
            note=data.note,
        )
        session.commit()
        return {
            "run_id": str(run.id),
            "complaint_number": complaint_number,
            "decision": data.decision,
            "case_id": str(case.id) if case else None,
            "case_state": case.state if case else "rejected",
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def batch_approve_inbound_complaint_groups(
    run_id: uuid.UUID,
    data: BatchReviewComplaintGroupsRequest,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    """Approve an explicit, current Ready selection in one transaction."""

    run = session.get(WhatsAppInboundProcessingRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Inbound processing run not found")
    normalized = [normalize_complaint_number(value) for value in data.complaint_numbers]
    if any(value is None for value in normalized):
        raise HTTPException(status_code=422, detail="Every selected complaint number must be valid")
    numbers = list(dict.fromkeys(value for value in normalized if value))
    groups = {
        str(group["complaint_number"]): group
        for group in complaint_group_summary(session, run.id)
    }
    missing = [number for number in numbers if number not in groups]
    stale = [
        number
        for number in numbers
        if number in groups and groups[number]["review_bucket"] != "ready"
    ]
    if missing or stale:
        details = []
        if missing:
            details.append(f"not found: {', '.join(missing)}")
        if stale:
            details.append(f"no longer Ready: {', '.join(stale)}")
        raise HTTPException(
            status_code=409,
            detail="Refresh the queue before batch approval; " + "; ".join(details),
        )
    cases = []
    try:
        for number in numbers:
            case = decide_complaint_group(
                session,
                run=run,
                complaint_number=number,
                decision="approved",
                reviewed_by=data.reviewed_by,
                note=data.note,
            )
            if case is not None:
                cases.append(case)
        session.commit()
    except ValueError as exc:
        session.rollback()
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "run_id": str(run.id),
        "approved_count": len(cases),
        "complaint_numbers": numbers,
        "cases": [
            {"id": str(case.id), "complaint_number": case.complaint_number, "state": case.state}
            for case in cases
        ],
    }


def _remove_file(path: str) -> None:
    Path(path).unlink(missing_ok=True)


def preview_inbound_processing_item(
    item_id: uuid.UUID,
    background_tasks: BackgroundTasks,
    download: bool = Query(default=False),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    item = session.get(WhatsAppInboundProcessingItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Processing item not found")
    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
    filename = (
        (batch_item.original_filename if batch_item else None)
        or (attachment.original_filename if attachment else None)
        or f"attachment-{item.id}"
    )
    mime_type = (
        (batch_item.mime_type if batch_item else None)
        or (attachment.detected_mime_type if attachment else None)
        or (attachment.mime_type if attachment else None)
        or "application/octet-stream"
    )
    if item.stored_object_id:
        stored = session.get(WhatsAppInboundStoredObject, item.stored_object_id)
        if stored is None:
            raise HTTPException(status_code=409, detail="RustFS object ledger record is missing")
        handle = tempfile.NamedTemporaryFile(prefix="wa-preview-", suffix=Path(filename).suffix, delete=False)
        handle.close()
        try:
            S3ObjectStorage(settings).download_file(
                bucket=stored.bucket,
                object_key=stored.object_key,
                destination=Path(handle.name),
            )
        except Exception as exc:
            _remove_file(handle.name)
            raise HTTPException(status_code=502, detail=f"Could not read object storage: {exc}") from exc
        background_tasks.add_task(_remove_file, handle.name)
        return FileResponse(
            handle.name,
            media_type=mime_type,
            filename=filename if download else None,
            content_disposition_type="attachment" if download else "inline",
        )
    if attachment and attachment.stored_path and Path(attachment.stored_path).is_file():
        return FileResponse(
            attachment.stored_path,
            media_type=mime_type,
            filename=filename if download else None,
            content_disposition_type="attachment" if download else "inline",
        )
    raise HTTPException(status_code=409, detail="The source file is unavailable")
