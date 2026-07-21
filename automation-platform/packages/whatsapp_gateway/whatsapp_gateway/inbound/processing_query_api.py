from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from fastapi import Depends, HTTPException, Query
from sqlmodel import Session, select

from automation_core.database import get_session
from whatsapp_gateway.inbound.processing import (
    complaint_group_summary,
    processing_counts,
    serialize_processing_event,
    serialize_processing_item,
    serialize_processing_run,
)
from whatsapp_gateway.models import (
    WhatsAppInboundProcessingEvent,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


def _sort_value(value: object) -> tuple[int, object]:
    """Keep nulls together while preserving numeric and chronological ordering."""
    if value is None:
        return (0, "")
    if isinstance(value, datetime):
        return (1, value.timestamp())
    if isinstance(value, (int, float)):
        return (1, value)
    return (1, str(value).casefold())


def list_inbound_processing_runs(
    status_filter: str | None = Query(default=None, alias="status"),
    category: str | None = Query(default=None),
    search: str | None = Query(default=None, max_length=120),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="created_at", max_length=40),
    order: Literal["asc", "desc"] = Query(default="desc"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    query = select(WhatsAppInboundProcessingRun)
    if status_filter:
        query = query.where(WhatsAppInboundProcessingRun.status == status_filter)
    rows = list(session.exec(query).all())
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
            for key in (
                "run_code",
                "batch_code",
                "contact_name",
                "contact_identity",
                "status",
            )
        ):
            continue
        items.append(view)
    sort_key = {
        "created_at": "created_at",
        "started_at": "started_at",
        "status": "status",
        "contact_name": "contact_name",
        "review_items": "review_items",
        "crm_complaints": "crm_complaints",
    }.get(sort, "created_at")
    items.sort(
        key=lambda item: _sort_value(item.get(sort_key)),
        reverse=order == "desc",
    )
    total = len(items)
    return {
        "items": items[offset : offset + limit],
        "total": total,
        "limit": limit,
        "offset": offset,
        "counts": processing_counts(session),
    }


def list_inbound_complaint_groups(
    run_id: uuid.UUID,
    bucket: str | None = Query(default=None, max_length=40),
    search: str | None = Query(default=None, max_length=120),
    paperless_category: str | None = Query(default=None, max_length=40),
    minimum_confidence: float | None = Query(default=None, ge=0, le=1),
    maximum_confidence: float | None = Query(default=None, ge=0, le=1),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="complaint_number", max_length=40),
    order: Literal["asc", "desc"] = Query(default="asc"),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    run = session.get(WhatsAppInboundProcessingRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Inbound processing run not found")
    all_groups = complaint_group_summary(session, run.id)
    bucket_counts: dict[str, int] = {}
    for group in all_groups:
        group_bucket = str(group.get("review_bucket") or "unknown")
        bucket_counts[group_bucket] = bucket_counts.get(group_bucket, 0) + 1

    term = (search or "").strip().casefold()
    groups: list[dict[str, Any]] = []
    for group in all_groups:
        group_bucket = str(group.get("review_bucket") or "")
        if bucket == "decided":
            if group_bucket not in {"approved", "rejected"}:
                continue
        elif bucket and group_bucket != bucket:
            continue
        if paperless_category and group.get("paperless_category") != paperless_category:
            continue
        confidence = float(group.get("average_confidence") or 0)
        if minimum_confidence is not None and confidence < minimum_confidence:
            continue
        if maximum_confidence is not None and confidence > maximum_confidence:
            continue
        haystack = " ".join(
            [
                str(group.get("complaint_number") or ""),
                str(group.get("complainant_name") or ""),
                str(group.get("district") or ""),
                *(str(item.get("filename") or "") for item in group.get("files") or []),
            ]
        ).casefold()
        if term and term not in haystack:
            continue
        groups.append(group)

    sort_key = {
        "complaint_number": "complaint_number",
        "complainant_name": "complainant_name",
        "district": "district",
        "item_count": "item_count",
        "average_confidence": "average_confidence",
        "paperless_category": "paperless_category",
    }.get(sort, "complaint_number")
    groups.sort(
        key=lambda item: _sort_value(item.get(sort_key)),
        reverse=order == "desc",
    )
    total = len(groups)
    return {
        "run": serialize_processing_run(session, run),
        "items": groups[offset : offset + limit],
        "total": total,
        "limit": limit,
        "offset": offset,
        "bucket_counts": bucket_counts,
    }


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
        elif category in {"duplicate_in_paperless", "needs_review"}:
            query = query.where(WhatsAppInboundProcessingItem.status == category)
        elif category == "eligible":
            query = query.where(WhatsAppInboundProcessingItem.status.in_(["eligible", "approved"]))
        else:
            query = query.where(WhatsAppInboundProcessingItem.primary_category == category)
    if review_status:
        query = query.where(WhatsAppInboundProcessingItem.review_status == review_status)
    items = list(session.exec(query.order_by(WhatsAppInboundProcessingItem.created_at)).all())
    return {
        **serialize_processing_run(session, run),
        "complaint_groups": complaint_group_summary(session, run.id),
        "items": [serialize_processing_item(session, item) for item in items],
    }


def read_inbound_processing_item(
    item_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    item = session.get(WhatsAppInboundProcessingItem, item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Processing item not found")
    run = session.get(WhatsAppInboundProcessingRun, item.run_id)
    return {
        **serialize_processing_item(session, item),
        "run": serialize_processing_run(session, run) if run else None,
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
