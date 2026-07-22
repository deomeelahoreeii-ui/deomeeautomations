from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

from celery.exceptions import SoftTimeLimitExceeded
from sqlmodel import Session, select

from automation_core.celery_app import celery_app
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.object_storage import S3ObjectStorage
from automation_core.time import utcnow
from whatsapp_gateway.inbound.processing import (
    CRM_CATEGORIES,
    recalculate_processing_run,
    record_processing_event,
)
from whatsapp_gateway.inbound.processing_classifier import classify_extracted_document, extract_document
from whatsapp_gateway.inbound.crm_registry import (
    associate_run_attachments,
    complaint_numbers_for_run,
    persist_crm_capture,
    persist_paperless_reconciliation,
)
from whatsapp_gateway.inbound.content_duplicates import (
    AUTO_CONTENT_MATCH_KINDS,
    resolve_exact_duplicate_before_extraction,
    resolve_normalized_duplicate_after_extraction,
)
from whatsapp_gateway.inbound.processing_support import (
    materialize_source as _materialize_source,
    paperless_client as _paperless_client,
    paperless_configured as _paperless_configured,
    write_derived_record as _write_derived_record,
)
from whatsapp_gateway.inbound.processing_task_lifecycle import (
    InboundProcessingTask,
    post_classification_status as _post_classification_status,
)
from whatsapp_gateway.inbound.spreadsheet_intake import (
    persist_spreadsheet_intake,
    persist_spreadsheet_paperless_reconciliation,
)
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundMessage,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
)


@celery_app.task(
    base=InboundProcessingTask,
    name="whatsapp_gateway.process_inbound_batch",
    soft_time_limit=60 * 60,
    time_limit=60 * 70,
)
def process_inbound_batch(run_id: str) -> dict[str, object]:
    run_uuid = uuid.UUID(run_id)
    settings = get_settings()
    storage = S3ObjectStorage(settings)
    with Session(engine) as session:
        run = session.get(WhatsAppInboundProcessingRun, run_uuid)
        if run is None:
            raise ValueError("Inbound processing run was not found")
        if run.status != "queued":
            return {"run_id": run_id, "status": run.status}
        now = utcnow()
        run.status = "extracting"
        run.started_at = now
        run.updated_at = now
        session.add(run)
        record_processing_event(
            session,
            run_id=run.id,
            event_type="extraction_started",
            message=f"Started CRM-first categorization for {run.total_items} file(s).",
            details={
                "classifier_version": run.classifier_version,
                "crm_rule_version": run.crm_rule_version,
            },
        )
        session.commit()

    try:
        with tempfile.TemporaryDirectory(prefix=f"wa-processing-{run_id}-") as temp_name:
            temp_root = Path(temp_name)
            with Session(engine) as session:
                item_ids = list(
                    session.exec(
                        select(WhatsAppInboundProcessingItem.id)
                        .where(WhatsAppInboundProcessingItem.run_id == run_uuid)
                        .order_by(WhatsAppInboundProcessingItem.created_at)
                    ).all()
                )

            for position, item_id in enumerate(item_ids, start=1):
                with Session(engine) as session:
                    run = session.get(WhatsAppInboundProcessingRun, run_uuid)
                    item = session.get(WhatsAppInboundProcessingItem, item_id)
                    if run is None or item is None:
                        continue
                    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
                    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
                    message = session.get(WhatsAppInboundMessage, batch_item.message_id) if batch_item else None
                    filename = (
                        (batch_item.original_filename if batch_item else None)
                        or (attachment.original_filename if attachment else None)
                        or f"attachment-{item.id}"
                    )
                    if resolve_exact_duplicate_before_extraction(
                        session,
                        item=item,
                    ):
                        recalculate_processing_run(session, run)
                        record_processing_event(
                            session,
                            run_id=run.id,
                            processing_item_id=item.id,
                            event_type="content_duplicate_resolved",
                            message=(
                                f"[{position}/{len(item_ids)}] Reused an earlier content decision for {filename}."
                            ),
                            details={
                                "content_match_kind": item.content_match_kind,
                                "canonical_processing_item_id": (
                                    str(item.canonical_processing_item_id)
                                    if item.canonical_processing_item_id
                                    else None
                                ),
                                "extraction_skipped": True,
                            },
                            level=(
                                "warning"
                                if item.content_match_kind == "exact_conflict"
                                else "info"
                            ),
                        )
                        session.commit()
                        continue
                    suffix = Path(filename).suffix
                    destination = temp_root / f"{item.id}{suffix}"
                    item.status = "extracting"
                    item.started_at = utcnow()
                    item.error = None
                    item.updated_at = utcnow()
                    session.add(item)
                    record_processing_event(
                        session,
                        run_id=run.id,
                        processing_item_id=item.id,
                        event_type="item_extraction_started",
                        message=f"[{position}/{len(item_ids)}] Extracting {filename}.",
                    )
                    session.commit()

                with Session(engine) as session:
                    run = session.get(WhatsAppInboundProcessingRun, run_uuid)
                    item = session.get(WhatsAppInboundProcessingItem, item_id)
                    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id) if item else None
                    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id) if item else None
                    message = session.get(WhatsAppInboundMessage, batch_item.message_id) if batch_item else None
                    assert run is not None and item is not None
                    filename = (
                        (batch_item.original_filename if batch_item else None)
                        or (attachment.original_filename if attachment else None)
                        or f"attachment-{item.id}"
                    )
                    try:
                        source = _materialize_source(
                            session=session,
                            item=item,
                            destination=destination,
                            settings=settings,
                            storage=storage,
                        )
                        extraction = extract_document(
                            source,
                            mime_type=(batch_item.mime_type if batch_item else None) or (attachment.detected_mime_type if attachment else None) or (attachment.mime_type if attachment else None),
                            text_limit=settings.whatsapp_processing_text_limit,
                        )
                        classification = classify_extracted_document(
                            filename=filename,
                            mime_type=(batch_item.mime_type if batch_item else None) or (attachment.detected_mime_type if attachment else None) or (attachment.mime_type if attachment else None),
                            extracted_text=extraction.text,
                            extraction_method=extraction.method,
                            caption=attachment.caption if attachment else "",
                            message_text=message.text_content if message else "",
                            prefixes=settings.crm_complaint_prefix_list,
                            suffix_digits=settings.crm_complaint_suffix_digits,
                        )
                        item.primary_category = classification.category
                        item.detected_complaint_number = classification.complaint_number
                        item.confidence = classification.confidence
                        item.evidence_json = classification.evidence
                        item.extracted_text = extraction.text
                        item.extraction_method = extraction.method
                        item.extracted_metadata_json = {
                            **extraction.metadata,
                            "all_complaint_numbers": classification.all_complaint_numbers,
                            "classification_needs_review": classification.needs_review,
                        }
                        item.error = extraction.error or None
                        item.status = _post_classification_status(item)
                        item.finished_at = utcnow()
                        item.updated_at = utcnow()
                        session.add(item)
                        content_reused = resolve_normalized_duplicate_after_extraction(
                            session,
                            item=item,
                        )
                        if not content_reused:
                            persist_crm_capture(
                                session,
                                item=item,
                                batch_item=batch_item,
                                attachment=attachment,
                                filename=filename,
                            )
                            if item.primary_category == "spreadsheet":
                                persist_spreadsheet_intake(
                                    session,
                                    item=item,
                                    batch_item=batch_item,
                                )
                        record_processing_event(
                            session,
                            run_id=run.id,
                            processing_item_id=item.id,
                            event_type="item_classified",
                            message=(
                                f"Classified {filename} as {item.primary_category}"
                                + (f" ({item.detected_complaint_number})" if item.detected_complaint_number else "")
                                + f" at {item.confidence:.0%} confidence."
                            ),
                            level="warning" if item.status == "needs_review" else "info",
                            details={
                                "category": item.primary_category,
                                "complaint_number": item.detected_complaint_number,
                                "confidence": item.confidence,
                                "extraction_method": item.extraction_method,
                                "content_match_kind": item.content_match_kind,
                            },
                        )
                    except Exception as exc:
                        item.status = "failed"
                        item.primary_category = "unknown"
                        item.error = str(exc)
                        item.finished_at = utcnow()
                        item.updated_at = utcnow()
                        session.add(item)
                        record_processing_event(
                            session,
                            run_id=run.id,
                            processing_item_id=item.id,
                            event_type="item_extraction_failed",
                            message=f"Failed to process {filename}: {exc}",
                            level="error",
                        )
                    recalculate_processing_run(session, run)
                    session.commit()

            with Session(engine) as session:
                run = session.get(WhatsAppInboundProcessingRun, run_uuid)
                assert run is not None
                associated_attachments = associate_run_attachments(session, run.id)
                if associated_attachments:
                    record_processing_event(
                        session,
                        run_id=run.id,
                        event_type="complaint_attachments_associated",
                        message=f"Proposed {associated_attachments} numberless attachment association(s).",
                    )
                complaint_numbers = complaint_numbers_for_run(session, run.id)
                if run.paperless_check_requested and complaint_numbers:
                    run.status = "checking_paperless"
                    run.paperless_check_status = "running"
                    run.updated_at = utcnow()
                    session.add(run)
                    record_processing_event(
                        session,
                        run_id=run.id,
                        event_type="paperless_check_started",
                        message=f"Checking {len(complaint_numbers)} complaint number(s) against Paperless.",
                    )
                    session.commit()

            paperless_results: dict[str, object] = {}
            paperless_error = ""
            with Session(engine) as session:
                run = session.get(WhatsAppInboundProcessingRun, run_uuid)
                assert run is not None
                should_check = run.paperless_check_requested and bool(complaint_numbers)
            if should_check:
                if not _paperless_configured(settings):
                    paperless_error = "Paperless credentials are not configured; duplicate status requires review."
                else:
                    try:
                        client = _paperless_client(settings)
                        client.connect()
                        client.prepare_complaint_lookup_index(complaint_numbers=complaint_numbers)
                        paperless_results = {
                            number: client.lookup_complaint(number)
                            for number in complaint_numbers
                        }
                    except Exception as exc:
                        paperless_error = str(exc)

            with Session(engine) as session:
                run = session.get(WhatsAppInboundProcessingRun, run_uuid)
                assert run is not None
                items = list(
                    session.exec(
                        select(WhatsAppInboundProcessingItem)
                        .where(WhatsAppInboundProcessingItem.run_id == run.id)
                        .order_by(WhatsAppInboundProcessingItem.created_at)
                    ).all()
                )
                for item in items:
                    if item.status in {"failed", "unsupported"}:
                        continue
                    if item.content_match_kind in AUTO_CONTENT_MATCH_KINDS:
                        try:
                            _write_derived_record(
                                session=session,
                                item=item,
                                settings=settings,
                                storage=storage,
                            )
                        except Exception as exc:
                            record_processing_event(
                                session,
                                run_id=run.id,
                                processing_item_id=item.id,
                                event_type="classification_record_failed",
                                message=f"Content duplicate resolved but its derived record could not be written: {exc}",
                                level="warning",
                            )
                        continue
                    complaint_number = item.detected_complaint_number
                    if complaint_number and complaint_number in paperless_results:
                        result = paperless_results[complaint_number]
                        item.paperless_category = result.category  # type: ignore[attr-defined]
                        item.paperless_reason = result.reason  # type: ignore[attr-defined]
                        item.paperless_document_ids = list(result.matched_document_ids)  # type: ignore[attr-defined]
                        item.paperless_statuses = list(result.matched_statuses)  # type: ignore[attr-defined]
                        extraction_ready = not bool(
                            (item.extracted_metadata_json or {}).get("needs_review")
                        )
                        if result.category == "fresh" and item.primary_category == "crm_complaint" and item.confidence >= 0.80 and extraction_ready:  # type: ignore[attr-defined]
                            item.status = "eligible"
                        elif result.category == "fresh":  # type: ignore[attr-defined]
                            item.status = "needs_review"
                        elif result.category in {"submitted", "uploaded_not_relevant", "uploaded_pending"}:  # type: ignore[attr-defined]
                            item.status = "duplicate_in_paperless"
                        else:
                            item.status = "needs_review"
                    elif complaint_number and run.paperless_check_requested:
                        item.paperless_category = "unavailable" if paperless_error else "fresh"
                        item.paperless_reason = paperless_error or "No matching CRM main complaint was found in Paperless."
                        extraction_ready = not bool(
                            (item.extracted_metadata_json or {}).get("needs_review")
                        )
                        item.status = "needs_review" if paperless_error or item.primary_category != "crm_complaint" or not extraction_ready else "eligible"
                    else:
                        item.paperless_category = "not_applicable" if not complaint_number else "not_checked"
                    persist_paperless_reconciliation(
                        session,
                        item=item,
                        paperless_requested=run.paperless_check_requested,
                        paperless_results=paperless_results,
                        paperless_error=paperless_error,
                    )
                    item.updated_at = utcnow()
                    session.add(item)
                    try:
                        _write_derived_record(session=session, item=item, settings=settings, storage=storage)
                    except Exception as exc:
                        record_processing_event(
                            session,
                            run_id=run.id,
                            processing_item_id=item.id,
                            event_type="classification_record_failed",
                            message=f"Classification completed but the derived RustFS record could not be written: {exc}",
                            level="warning",
                        )
                persist_spreadsheet_paperless_reconciliation(
                    session,
                    run_id=run.id,
                    paperless_requested=run.paperless_check_requested,
                    paperless_results=paperless_results,
                    paperless_error=paperless_error,
                )
                run.paperless_error = paperless_error or None
                run.paperless_check_status = (
                    "failed" if paperless_error else "completed"
                ) if run.paperless_check_requested and complaint_numbers else (
                    "skipped" if not run.paperless_check_requested else "not_applicable"
                )
                recalculate_processing_run(session, run)
                run.status = "completed_with_errors" if run.failed_items or paperless_error else "completed"
                run.finished_at = utcnow()
                run.updated_at = utcnow()
                session.add(run)
                record_processing_event(
                    session,
                    run_id=run.id,
                    event_type="processing_run_completed",
                    message=(
                        f"Processing completed: {run.crm_complaints} confirmed CRM complaint(s), "
                        f"{run.possible_crm} possible CRM complaint(s), {run.eligible_items} eligible, "
                        f"{run.duplicate_items} Paperless duplicate(s), {run.review_items} requiring review."
                    ),
                    level="warning" if run.status == "completed_with_errors" else "info",
                )
                session.commit()
                return {
                    "run_id": str(run.id),
                    "run_code": run.run_code,
                    "status": run.status,
                    "total_items": run.total_items,
                    "crm_complaints": run.crm_complaints,
                    "possible_crm": run.possible_crm,
                    "eligible_items": run.eligible_items,
                    "duplicate_items": run.duplicate_items,
                    "review_items": run.review_items,
                    "failed_items": run.failed_items,
                }
    except SoftTimeLimitExceeded:
        with Session(engine) as session:
            run = session.get(WhatsAppInboundProcessingRun, run_uuid)
            if run:
                run.status = "failed"
                run.error = "Inbound processing exceeded the 60 minute execution limit."
                run.finished_at = utcnow()
                run.updated_at = utcnow()
                session.add(run)
                record_processing_event(
                    session,
                    run_id=run.id,
                    event_type="processing_run_timed_out",
                    message=run.error,
                    level="error",
                )
                session.commit()
        raise
