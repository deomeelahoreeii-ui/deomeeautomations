from __future__ import annotations

import uuid

from celery import Task
from sqlmodel import Session

from automation_core.database import engine
from automation_core.time import utcnow
from whatsapp_gateway.inbound.processing import record_processing_event
from whatsapp_gateway.models import (
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


class InboundProcessingTask(Task):
    abstract = True

    def on_failure(self, exc, task_id, args, kwargs, einfo) -> None:  # type: ignore[no-untyped-def]
        run_id = str(args[0]) if args else str(kwargs.get("run_id") or "")
        try:
            value = uuid.UUID(run_id)
        except (TypeError, ValueError):
            return
        with Session(engine) as session:
            run = session.get(WhatsAppInboundProcessingRun, value)
            if run is None or run.status not in {
                "queued",
                "extracting",
                "checking_paperless",
            }:
                return
            run.status = "failed"
            run.error = str(exc)
            run.finished_at = utcnow()
            run.updated_at = utcnow()
            session.add(run)
            record_processing_event(
                session,
                run_id=run.id,
                event_type="processing_run_failed",
                message=f"Processing worker stopped unexpectedly: {exc}",
                level="error",
            )
            session.commit()


def post_classification_status(item: WhatsAppInboundProcessingItem) -> str:
    if item.primary_category == "unsupported":
        return "unsupported"
    if item.error and not item.detected_complaint_number:
        return "failed"
    if item.primary_category in {
        "possible_crm_complaint",
        "crm_supporting_document",
        "crm_reply_or_report",
        "unknown",
    }:
        return "needs_review"
    if item.primary_category == "crm_complaint":
        extraction_needs_review = bool(
            (item.extracted_metadata_json or {}).get("needs_review")
        )
        return (
            "needs_review"
            if item.confidence < 0.80 or extraction_needs_review
            else "extracted"
        )
    return "extracted"
