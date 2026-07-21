from __future__ import annotations

import uuid
from typing import Any

from sqlmodel import Session, select

from crm_domain.fields import extract_field_observations
from crm_domain.models import ComplaintCase
from whatsapp_gateway.models import (
    WhatsAppInboundBatchItem,
    WhatsAppInboundProcessingItem,
)


def complaint_group_summary(
    session: Session,
    run_id: uuid.UUID,
) -> list[dict[str, Any]]:
    items = list(
        session.exec(
            select(WhatsAppInboundProcessingItem)
            .where(WhatsAppInboundProcessingItem.run_id == run_id)
            .where(WhatsAppInboundProcessingItem.detected_complaint_number.is_not(None))
            .order_by(
                WhatsAppInboundProcessingItem.detected_complaint_number,
                WhatsAppInboundProcessingItem.created_at,
            )
        ).all()
    )
    groups: dict[str, dict[str, Any]] = {}
    for item in items:
        number = item.detected_complaint_number
        if not number:
            continue
        group = groups.setdefault(
            number,
            {
                "complaint_number": number,
                "item_count": 0,
                "categories": {},
                "eligible_items": 0,
                "duplicate_items": 0,
                "review_items": 0,
                "approved_items": 0,
                "rejected_items": 0,
                "confidence_total": 0.0,
                "minimum_confidence": 1.0,
                "paperless_categories": {},
                "paperless_document_ids": [],
                "paperless_statuses": [],
                "paperless_reasons": [],
                "files": [],
                "complainant_name": None,
                "district": None,
            },
        )
        group["item_count"] += 1
        categories = group["categories"]
        categories[item.primary_category] = categories.get(item.primary_category, 0) + 1
        group["eligible_items"] += int(item.status in {"eligible", "approved"})
        group["duplicate_items"] += int(item.status == "duplicate_in_paperless")
        group["review_items"] += int(item.status in {"needs_review", "deferred"})
        group["approved_items"] += int(item.review_status == "approved")
        group["rejected_items"] += int(item.review_status == "rejected")
        group["confidence_total"] += float(item.confidence or 0)
        group["minimum_confidence"] = min(
            float(group["minimum_confidence"]), float(item.confidence or 0)
        )
        paperless = item.paperless_category or "not_checked"
        group["paperless_categories"][paperless] = (
            group["paperless_categories"].get(paperless, 0) + 1
        )
        for document_id in item.paperless_document_ids or []:
            if document_id not in group["paperless_document_ids"]:
                group["paperless_document_ids"].append(document_id)
        for paperless_status in item.paperless_statuses or []:
            if paperless_status not in group["paperless_statuses"]:
                group["paperless_statuses"].append(paperless_status)
        if item.paperless_reason and item.paperless_reason not in group["paperless_reasons"]:
            group["paperless_reasons"].append(item.paperless_reason)
        batch_item = session.get(WhatsAppInboundBatchItem, item.batch_item_id)
        group["files"].append(
            {
                "id": str(item.id),
                "filename": batch_item.original_filename if batch_item else "Evidence file",
                "category": item.primary_category,
                "confidence": item.confidence,
                "review_status": item.review_status,
            }
        )
        if not group["complainant_name"] or not group["district"]:
            fields = {
                observation.field_name: observation.normalized_value
                for observation in extract_field_observations(item.extracted_text or "")
            }
            group["complainant_name"] = group["complainant_name"] or fields.get("complainant_name")
            group["district"] = group["district"] or fields.get("district")

    approved_runs_by_number: dict[str, set[uuid.UUID]] = {number: set() for number in groups}
    if groups:
        approved_rows = session.exec(
            select(
                WhatsAppInboundProcessingItem.run_id,
                WhatsAppInboundProcessingItem.detected_complaint_number,
            ).where(
                WhatsAppInboundProcessingItem.review_status == "approved",
                WhatsAppInboundProcessingItem.detected_complaint_number.in_(list(groups)),
            )
        ).all()
        for approved_run_id, approved_number in approved_rows:
            if approved_number:
                approved_runs_by_number[approved_number].add(approved_run_id)

    result: list[dict[str, Any]] = []
    for number in sorted(groups):
        group = groups[number]
        count = int(group["item_count"])
        group["average_confidence"] = float(group.pop("confidence_total")) / count if count else 0.0
        paperless_priority = (
            "manual_review",
            "submitted",
            "uploaded_not_relevant",
            "uploaded_pending",
            "fresh",
            "unavailable",
            "not_checked",
            "not_applicable",
        )
        group["paperless_category"] = next(
            (
                category
                for category in paperless_priority
                if category in group["paperless_categories"]
            ),
            "not_checked",
        )
        group["paperless_match_count"] = len(group["paperless_document_ids"])
        if group["approved_items"] == count:
            group["review_bucket"] = "approved"
        elif group["rejected_items"] == count:
            group["review_bucket"] = "rejected"
        elif group["duplicate_items"]:
            group["review_bucket"] = "existing"
        elif group["review_items"] or group["categories"].get("possible_crm_complaint"):
            group["review_bucket"] = "manual_review"
        else:
            group["review_bucket"] = "ready"
        complaint_case = session.exec(
            select(ComplaintCase).where(
                ComplaintCase.source_system == "crm_portal",
                ComplaintCase.complaint_number == number,
            )
        ).first()
        group["case_id"] = str(complaint_case.id) if complaint_case else None
        group["case_state"] = complaint_case.state if complaint_case else None
        group["approved_run_count"] = len(approved_runs_by_number[number])
        group["approved_in_other_runs"] = bool(approved_runs_by_number[number].difference({run_id}))
        result.append(group)
    return result
