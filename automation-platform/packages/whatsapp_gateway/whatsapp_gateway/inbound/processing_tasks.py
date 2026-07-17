from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

from celery import Task
from celery.exceptions import SoftTimeLimitExceeded
from sqlmodel import Session, select

from automation_core.celery_app import celery_app
from automation_core.config import Settings, get_settings
from automation_core.database import engine
from automation_core.object_storage import ObjectStorageError, S3ObjectStorage
from automation_core.time import utcnow
from crm_filters.paperless import PaperlessClient
from whatsapp_gateway.inbound.processing import (
    CRM_CATEGORIES,
    classification_document,
    recalculate_processing_run,
    record_processing_event,
)
from whatsapp_gateway.inbound.processing_classifier import (
    classify_extracted_document,
    extract_document,
)
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatchItem,
    WhatsAppInboundMessage,
    WhatsAppInboundProcessingItem,
    WhatsAppInboundProcessingRun,
    WhatsAppInboundStoredObject,
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
            if run is None or run.status not in {"queued", "extracting", "checking_paperless"}:
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


def _paperless_client(settings: Settings) -> PaperlessClient:
    return PaperlessClient(
        base_url=settings.paperless_url,
        username=settings.paperless_username,
        password=settings.paperless_password,
        token=settings.paperless_token,
        verify_ssl=settings.paperless_verify_ssl,
        ca_bundle=settings.paperless_ca_bundle,
        allow_insecure_fallback=settings.paperless_allow_insecure_fallback,
        timeout_seconds=settings.paperless_timeout_seconds,
        document_type_name=settings.paperless_document_type_complaint,
        max_pages=settings.paperless_max_pages,
    )


def _paperless_configured(settings: Settings) -> bool:
    return bool(
        settings.paperless_url
        and (
            settings.paperless_token
            or (settings.paperless_username and settings.paperless_password)
        )
    )


def _materialize_source(
    *,
    session: Session,
    item: WhatsAppInboundProcessingItem,
    destination: Path,
    settings: Settings,
    storage: S3ObjectStorage,
) -> Path:
    attachment = session.get(WhatsAppInboundAttachment, item.attachment_id)
    batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
    if attachment is None or batch_item is None:
        raise RuntimeError("The inbound attachment or batch item is unavailable")
    if item.stored_object_id:
        stored = session.get(WhatsAppInboundStoredObject, item.stored_object_id)
        if stored is None:
            raise RuntimeError("The RustFS object ledger record is unavailable")
        result = storage.download_file(
            bucket=stored.bucket,
            object_key=stored.object_key,
            destination=destination,
        )
        if result["size_bytes"] != stored.size_bytes:
            raise ObjectStorageError("Downloaded object size does not match the storage ledger")
        if stored.sha256 and result["sha256"] != stored.sha256:
            raise ObjectStorageError("Downloaded object checksum does not match the storage ledger")
        return destination
    if attachment.stored_path:
        source = Path(attachment.stored_path)
        if source.is_file():
            destination.write_bytes(source.read_bytes())
            return destination
    raise RuntimeError("The file is unavailable in RustFS and the compatibility archive")


def _write_derived_record(
    *,
    session: Session,
    item: WhatsAppInboundProcessingItem,
    settings: Settings,
    storage: S3ObjectStorage,
) -> None:
    if not settings.object_storage_enabled:
        return
    key = f"classification/{item.run_id}/{item.id}.json"
    storage.put_bytes(
        bucket=settings.object_storage_derived_bucket,
        object_key=key,
        body=classification_document(item),
        content_type="application/json",
        metadata={
            "processing-item-id": str(item.id),
            "attachment-id": str(item.attachment_id),
            "category": item.primary_category,
            "complaint-number": item.detected_complaint_number or "",
        },
    )
    item.derived_object_key = key
    session.add(item)


def _post_classification_status(item: WhatsAppInboundProcessingItem) -> str:
    if item.primary_category == "unsupported":
        return "unsupported"
    if item.error and not item.detected_complaint_number:
        return "failed"
    if item.primary_category in {"possible_crm_complaint", "crm_supporting_document", "crm_reply_or_report", "unknown"}:
        return "needs_review"
    if item.primary_category == "crm_complaint":
        return "needs_review" if item.confidence < 0.80 else "extracted"
    return "extracted"


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
                complaint_numbers = sorted(
                    {
                        value
                        for value in session.exec(
                            select(WhatsAppInboundProcessingItem.detected_complaint_number)
                            .where(WhatsAppInboundProcessingItem.run_id == run.id)
                            .where(WhatsAppInboundProcessingItem.detected_complaint_number.is_not(None))
                        ).all()
                        if value
                    }
                )
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
                    complaint_number = item.detected_complaint_number
                    if complaint_number and complaint_number in paperless_results:
                        result = paperless_results[complaint_number]
                        item.paperless_category = result.category  # type: ignore[attr-defined]
                        item.paperless_reason = result.reason  # type: ignore[attr-defined]
                        item.paperless_document_ids = list(result.matched_document_ids)  # type: ignore[attr-defined]
                        item.paperless_statuses = list(result.matched_statuses)  # type: ignore[attr-defined]
                        if result.category == "fresh" and item.primary_category == "crm_complaint" and item.confidence >= 0.80:  # type: ignore[attr-defined]
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
                        item.status = "needs_review" if paperless_error or item.primary_category != "crm_complaint" else "eligible"
                    else:
                        item.paperless_category = "not_applicable" if not complaint_number else "not_checked"
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
