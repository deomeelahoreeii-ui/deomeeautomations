from __future__ import annotations

import json
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func
from sqlmodel import Session, select

from automation_core.config import Settings
from automation_core.time import utcnow
from whatsapp_gateway.directory.master_contacts import resolved_contact_name
from whatsapp_gateway.inbound.processing_groups import complaint_group_summary
from whatsapp_gateway.models import (
    WhatsAppDirectoryContact,
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchItem,
    WhatsAppInboundMessage,
    WhatsAppInboundProcessingEvent,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)

PROCESSING_TERMINAL_STATUSES = {"completed", "completed_with_errors", "failed", "cancelled"}
PROCESSING_ACTIVE_STATUSES = {"queued", "extracting", "checking_paperless"}
CRM_CATEGORIES = {
    "crm_complaint",
    "possible_crm_complaint",
    "crm_supporting_document",
    "crm_reply_or_report",
}


def new_processing_code(now: datetime | None = None) -> str:
    stamp = now or utcnow()
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    stamp = stamp.astimezone(timezone.utc)
    return f"WAP-{stamp:%Y%m%d-%H%M%S}-{secrets.token_hex(3).upper()}"


def record_processing_event(
    session: Session,
    *,
    run_id: uuid.UUID,
    event_type: str,
    message: str,
    level: str = "info",
    processing_item_id: uuid.UUID | None = None,
    details: dict[str, Any] | None = None,
) -> WhatsAppInboundProcessingEvent:
    event = WhatsAppInboundProcessingEvent(
        run_id=run_id,
        processing_item_id=processing_item_id,
        level=level,
        event_type=event_type,
        message=message,
        details_json=details or {},
        created_at=utcnow(),
    )
    session.add(event)
    session.flush()
    return event


def serialize_processing_event(event: WhatsAppInboundProcessingEvent) -> dict[str, Any]:
    return {
        "id": str(event.id),
        "run_id": str(event.run_id),
        "processing_item_id": str(event.processing_item_id) if event.processing_item_id else None,
        "level": event.level,
        "event_type": event.event_type,
        "message": event.message,
        "details": event.details_json or {},
        "created_at": event.created_at,
    }


def create_processing_run(
    session: Session,
    *,
    batch: WhatsAppInboundBatch,
    settings: Settings,
    paperless_check: bool,
) -> WhatsAppInboundProcessingRun:
    if batch.status not in {"completed", "completed_with_errors"}:
        raise ValueError("Only a completed inbound batch can be processed")
    # One intake batch has one canonical review unless that attempt failed or
    # was cancelled. Reopening a completed intake must return its review, not
    # silently create a second decision history for the same evidence.
    existing = session.exec(
        select(WhatsAppInboundProcessingRun)
        .where(WhatsAppInboundProcessingRun.batch_id == batch.id)
        .where(
            WhatsAppInboundProcessingRun.status.notin_(["failed", "cancelled"])
        )
        .order_by(WhatsAppInboundProcessingRun.created_at.desc())
    ).first()
    if existing is not None:
        return existing

    batch_items = list(
        session.exec(
            select(WhatsAppInboundBatchItem)
            .where(WhatsAppInboundBatchItem.batch_id == batch.id)
            .order_by(WhatsAppInboundBatchItem.created_at)
        ).all()
    )
    if not batch_items:
        raise ValueError("The selected batch does not contain any files")
    now = utcnow()
    run = WhatsAppInboundProcessingRun(
        run_code=new_processing_code(now),
        batch_id=batch.id,
        status="queued",
        classifier_version=settings.whatsapp_processing_classifier_version,
        crm_rule_version=settings.whatsapp_processing_crm_rule_version,
        paperless_check_requested=paperless_check,
        paperless_check_status="pending" if paperless_check else "skipped",
        total_items=len(batch_items),
        configuration_json={
            "crm_complaint_prefixes": settings.crm_complaint_prefix_list,
            "crm_complaint_suffix_digits": settings.crm_complaint_suffix_digits,
            "text_limit": settings.whatsapp_processing_text_limit,
            "derived_bucket": settings.object_storage_derived_bucket,
        },
        created_at=now,
        updated_at=now,
    )
    session.add(run)
    session.flush()
    for batch_item in batch_items:
        item = WhatsAppInboundProcessingItem(
            run_id=run.id,
            batch_item_id=batch_item.id,
            attachment_id=batch_item.attachment_id,
            stored_object_id=batch_item.stored_object_id,
            status="queued",
            primary_category="unknown",
            review_status="pending",
            created_at=now,
            updated_at=now,
        )
        session.add(item)
    session.flush()
    record_processing_event(
        session,
        run_id=run.id,
        event_type="processing_run_created",
        message=f"Created processing run {run.run_code} for {len(batch_items)} file(s).",
        details={"batch_id": str(batch.id), "batch_code": batch.batch_code, "paperless_check": paperless_check},
    )
    return run


def _contact_display_name(session: Session, batch: WhatsAppInboundBatch) -> str:
    contact = session.get(WhatsAppDirectoryContact, batch.contact_id)
    if contact:
        name = resolved_contact_name(session, contact)
        if name:
            return name
    push_name = session.exec(
        select(WhatsAppInboundMessage.push_name)
        .where(WhatsAppInboundMessage.directory_contact_id == batch.contact_id)
        .where(WhatsAppInboundMessage.from_me.is_(False))
        .where(WhatsAppInboundMessage.push_name.is_not(None))
        .order_by(WhatsAppInboundMessage.message_timestamp.desc())
        .limit(1)
    ).first()
    return str(push_name or "Unnamed contact")


def serialize_processing_run(session: Session, run: WhatsAppInboundProcessingRun) -> dict[str, Any]:
    batch = session.get(WhatsAppInboundBatch, run.batch_id)
    return {
        "id": str(run.id),
        "run_code": run.run_code,
        "batch_id": str(run.batch_id),
        "batch_code": batch.batch_code if batch else "",
        "contact_id": str(batch.contact_id) if batch else None,
        "contact_name": _contact_display_name(session, batch) if batch else "Unknown contact",
        "contact_identity": batch.remote_jid if batch else None,
        "status": run.status,
        "classifier_version": run.classifier_version,
        "crm_rule_version": run.crm_rule_version,
        "paperless_check_requested": run.paperless_check_requested,
        "paperless_check_status": run.paperless_check_status,
        "paperless_error": run.paperless_error,
        "total_items": run.total_items,
        "processed_items": run.processed_items,
        "crm_complaints": run.crm_complaints,
        "possible_crm": run.possible_crm,
        "supporting_documents": run.supporting_documents,
        "reply_reports": run.reply_reports,
        "non_crm": run.non_crm,
        "unknown_items": run.unknown_items,
        "duplicate_items": run.duplicate_items,
        "eligible_items": run.eligible_items,
        "review_items": run.review_items,
        "failed_items": run.failed_items,
        "configuration": run.configuration_json or {},
        "error": run.error,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "updated_at": run.updated_at,
    }


def serialize_processing_item(
    session: Session,
    item: WhatsAppInboundProcessingItem,
) -> dict[str, Any]:
    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
    message = session.get(WhatsAppInboundMessage, batch_item.message_id) if batch_item else None
    return {
        "id": str(item.id),
        "run_id": str(item.run_id),
        "batch_item_id": str(item.batch_item_id),
        "attachment_id": str(item.attachment_id),
        "stored_object_id": str(item.stored_object_id) if item.stored_object_id else None,
        "filename": (batch_item.original_filename if batch_item else None) or (attachment.original_filename if attachment else None) or "Unnamed attachment",
        "mime_type": (batch_item.mime_type if batch_item else None) or (attachment.detected_mime_type if attachment else None) or (attachment.mime_type if attachment else None),
        "size_bytes": batch_item.size_bytes if batch_item else (attachment.actual_size if attachment else None),
        "message_timestamp": message.message_timestamp if message else (batch_item.message_timestamp if batch_item else None),
        "caption": attachment.caption if attachment else None,
        "status": item.status,
        "primary_category": item.primary_category,
        "detected_complaint_number": item.detected_complaint_number,
        "confidence": item.confidence,
        "evidence": item.evidence_json or [],
        "extracted_text": item.extracted_text or "",
        "extraction_method": item.extraction_method,
        "extracted_metadata": item.extracted_metadata_json or {},
        "paperless_category": item.paperless_category,
        "paperless_reason": item.paperless_reason,
        "paperless_document_ids": item.paperless_document_ids or [],
        "paperless_statuses": item.paperless_statuses or [],
        "review_status": item.review_status,
        "reviewer_note": item.reviewer_note,
        "reviewed_by": item.reviewed_by,
        "reviewed_at": item.reviewed_at,
        "derived_object_key": item.derived_object_key,
        "error": item.error,
        "created_at": item.created_at,
        "started_at": item.started_at,
        "finished_at": item.finished_at,
        "updated_at": item.updated_at,
    }
def recalculate_processing_run(session: Session, run: WhatsAppInboundProcessingRun) -> None:
    items = list(
        session.exec(
            select(WhatsAppInboundProcessingItem).where(WhatsAppInboundProcessingItem.run_id == run.id)
        ).all()
    )
    categories = [item.primary_category for item in items]
    run.total_items = len(items)
    run.processed_items = sum(item.status not in {"queued", "extracting"} for item in items)
    run.crm_complaints = categories.count("crm_complaint")
    run.possible_crm = categories.count("possible_crm_complaint")
    run.supporting_documents = categories.count("crm_supporting_document")
    run.reply_reports = categories.count("crm_reply_or_report")
    run.non_crm = sum(value in {"spreadsheet", "image", "general_official_document"} for value in categories)
    run.unknown_items = categories.count("unknown")
    run.duplicate_items = sum(item.status == "duplicate_in_paperless" for item in items)
    run.eligible_items = sum(item.status in {"eligible", "approved"} for item in items)
    run.review_items = sum(item.status in {"needs_review", "deferred"} or item.review_status == "deferred" for item in items)
    run.failed_items = sum(item.status in {"failed", "unsupported"} for item in items)
    run.updated_at = utcnow()
    session.add(run)


def update_review_decision(
    session: Session,
    *,
    item: WhatsAppInboundProcessingItem,
    decision: str,
    reviewed_by: str,
    note: str | None,
    category: str | None,
    complaint_number: str | None,
) -> WhatsAppInboundProcessingItem:
    if decision not in {"approved", "rejected", "deferred", "pending"}:
        raise ValueError("Unsupported review decision")
    if category:
        item.primary_category = category
    if complaint_number is not None:
        value = complaint_number.strip()
        item.detected_complaint_number = value or None
    item.review_status = decision
    item.reviewer_note = (note or "").strip() or None
    item.reviewed_by = reviewed_by.strip() or "web-operator"
    item.reviewed_at = utcnow() if decision != "pending" else None
    if decision == "approved":
        item.status = "approved"
    elif decision == "rejected":
        item.status = "rejected"
    elif decision == "deferred":
        item.status = "deferred"
    elif item.primary_category in CRM_CATEGORIES:
        item.status = "needs_review"
    else:
        item.status = "extracted"
    item.updated_at = utcnow()
    session.add(item)
    run = session.get(WhatsAppInboundProcessingRun, item.run_id)
    if run:
        recalculate_processing_run(session, run)
        record_processing_event(
            session,
            run_id=run.id,
            processing_item_id=item.id,
            event_type="review_decision_updated",
            message=f"Review decision set to {decision} for {item.detected_complaint_number or 'unidentified document'}.",
            details={
                "decision": decision,
                "category": item.primary_category,
                "complaint_number": item.detected_complaint_number,
                "reviewed_by": item.reviewed_by,
            },
        )
    return item


def classification_document(item: WhatsAppInboundProcessingItem) -> bytes:
    payload = {
        "schema_version": "whatsapp_inbound_classification_v1",
        "processing_item_id": str(item.id),
        "run_id": str(item.run_id),
        "attachment_id": str(item.attachment_id),
        "status": item.status,
        "category": item.primary_category,
        "complaint_number": item.detected_complaint_number,
        "confidence": item.confidence,
        "evidence": item.evidence_json or [],
        "extraction_method": item.extraction_method,
        "paperless": {
            "category": item.paperless_category,
            "reason": item.paperless_reason,
            "document_ids": item.paperless_document_ids or [],
            "statuses": item.paperless_statuses or [],
        },
        "review": {
            "status": item.review_status,
            "note": item.reviewer_note,
            "reviewed_by": item.reviewed_by,
            "reviewed_at": item.reviewed_at.isoformat() if item.reviewed_at else None,
        },
        "updated_at": item.updated_at.isoformat(),
    }
    return json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")


def processing_counts(session: Session) -> dict[str, int]:
    return {
        "total": int(session.exec(select(func.count()).select_from(WhatsAppInboundProcessingRun)).one()),
        "active": int(
            session.exec(
                select(func.count())
                .select_from(WhatsAppInboundProcessingRun)
                .where(WhatsAppInboundProcessingRun.status.in_(PROCESSING_ACTIVE_STATUSES))
            ).one()
        ),
        "completed": int(
            session.exec(
                select(func.count())
                .select_from(WhatsAppInboundProcessingRun)
                .where(WhatsAppInboundProcessingRun.status == "completed")
            ).one()
        ),
        "with_errors": int(
            session.exec(
                select(func.count())
                .select_from(WhatsAppInboundProcessingRun)
                .where(WhatsAppInboundProcessingRun.status.in_(["completed_with_errors", "failed"]))
            ).one()
        ),
    }
