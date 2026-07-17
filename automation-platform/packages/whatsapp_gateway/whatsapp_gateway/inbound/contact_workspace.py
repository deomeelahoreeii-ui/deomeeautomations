from __future__ import annotations

import uuid
from collections import Counter
from pathlib import Path
from typing import Any

from fastapi import Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy import func
from sqlmodel import Session, select

from automation_core.database import get_session
from whatsapp_gateway.directory.master_contacts import resolved_contact_name
from whatsapp_gateway.inbound.common import require_contact
from whatsapp_gateway.inbound.contact_matching import (
    contact_coverage,
    contact_message_filter,
    find_matching_attachments,
)
from whatsapp_gateway.models import (
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundMessage,
)


def _evidence_item(attachment, message, category: str) -> dict[str, Any]:
    return {
        "attachment_id": str(attachment.id),
        "message_id": message.message_id,
        "timestamp": message.message_timestamp,
        "category": category,
        "filename": attachment.original_filename or f"{category}-{message.message_id}",
        "mime_type": attachment.detected_mime_type or attachment.mime_type,
        "size_bytes": attachment.actual_size or attachment.declared_size,
        "chat_scope": message.chat_scope,
        "caption": attachment.caption or message.text_content,
        "download_status": attachment.download_status,
        "downloadable": bool(attachment.stored_path),
    }


def contact_intake_workspace(
    contact_id: uuid.UUID,
    category: str | None = Query(default=None, pattern="^(image|pdf|spreadsheet)$"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        contact = require_contact(session, contact_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    matches = find_matching_attachments(
        session,
        contact_id=contact.id,
        date_from=None,
        date_to=None,
        chat_scope="direct_and_groups",
        media_types=["image", "pdf", "spreadsheet"],
    )
    category_counts = Counter(item_category for _, _, item_category in matches)
    status_counts = Counter(attachment.download_status for attachment, _, _ in matches)
    declared_bytes = sum(int(attachment.actual_size or attachment.declared_size or 0) for attachment, _, _ in matches)
    filtered = [row for row in matches if category is None or row[2] == category]
    filtered.reverse()
    offset = (page - 1) * page_size
    earliest, latest = contact_coverage(session, contact.id)
    run_count = int(
        session.exec(
            select(func.count())
            .select_from(WhatsAppInboundBatch)
            .where(WhatsAppInboundBatch.contact_id == contact.id)
        ).one()
    )
    return {
        "contact": {
            "id": str(contact.id),
            "display_name": resolved_contact_name(session, contact) or "Unnamed contact",
            "phone_jid": contact.phone_jid,
            "primary_lid_jid": contact.primary_lid_jid,
            "identity": contact.phone_jid or contact.primary_lid_jid or contact.canonical_key,
        },
        "metrics": {
            "evidence_total": len(matches),
            "run_count": run_count,
            "category_counts": dict(category_counts),
            "status_counts": dict(status_counts),
            "total_bytes": declared_bytes,
        },
        "coverage": {
            "earliest_message_at": earliest,
            "latest_message_at": latest,
            "note": "This library combines evidence captured across every intake run for this contact.",
        },
        "evidence": {
            "items": [_evidence_item(*row) for row in filtered[offset : offset + page_size]],
            "total": len(filtered),
            "page": page,
            "page_size": page_size,
            "pages": max(1, (len(filtered) + page_size - 1) // page_size),
            "category": category,
        },
    }


def download_contact_evidence(
    contact_id: uuid.UUID,
    attachment_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> FileResponse:
    try:
        contact = require_contact(session, contact_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    row = session.exec(
        select(WhatsAppInboundAttachment, WhatsAppInboundMessage)
        .join(WhatsAppInboundMessage, WhatsAppInboundMessage.id == WhatsAppInboundAttachment.message_id)
        .where(
            WhatsAppInboundAttachment.id == attachment_id,
            contact_message_filter(session, contact),
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Contact evidence was not found")
    attachment, _message = row
    if not attachment.stored_path:
        raise HTTPException(status_code=409, detail="This evidence file has not been archived locally")
    source = Path(attachment.stored_path)
    if not source.is_file():
        raise HTTPException(status_code=410, detail="The archived evidence file is missing")
    return FileResponse(
        source,
        media_type=attachment.detected_mime_type or attachment.mime_type or "application/octet-stream",
        filename=attachment.original_filename or source.name,
    )
