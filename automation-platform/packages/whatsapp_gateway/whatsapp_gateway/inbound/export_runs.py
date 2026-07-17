
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Iterable

from sqlalchemy import func
from sqlmodel import Session, select

from whatsapp_gateway.inbound.common import normalize_media_types, normalize_utc_naive, require_contact
from whatsapp_gateway.inbound.contact_matching import contact_coverage, find_matching_attachments
from whatsapp_gateway.directory.master_contacts import resolved_contact_name
from whatsapp_gateway.models import WhatsAppInboundExportItem, WhatsAppInboundExportRun

def create_export_run(
    session: Session,
    *,
    job_id: uuid.UUID,
    contact_id: uuid.UUID,
    date_from: datetime | None,
    date_to: datetime | None,
    chat_scope: str,
    media_types: Iterable[str],
) -> WhatsAppInboundExportRun:
    contact = require_contact(session, contact_id)
    normalized_types = normalize_media_types(media_types)
    matches = find_matching_attachments(
        session,
        contact_id=contact.id,
        date_from=date_from,
        date_to=date_to,
        chat_scope=chat_scope,
        media_types=normalized_types,
    )
    if not matches:
        raise ValueError("No matching inbound files were found for this contact and filter")
    earliest, latest = contact_coverage(session, contact.id)
    run = WhatsAppInboundExportRun(
        job_id=job_id,
        account_id=contact.account_id,
        contact_id=contact.id,
        contact_name=resolved_contact_name(session, contact) or contact.phone_jid or contact.canonical_key,
        date_from=normalize_utc_naive(date_from),
        date_to=normalize_utc_naive(date_to),
        chat_scope=chat_scope,
        media_types=normalized_types,
        files_matched=len(matches),
        coverage_earliest_at=earliest,
        coverage_latest_at=latest,
    )
    session.add(run)
    session.flush()
    for attachment, _message, category in matches:
        session.add(
            WhatsAppInboundExportItem(
                export_run_id=run.id,
                attachment_id=attachment.id,
                media_category=category,
            )
        )
    session.commit()
    session.refresh(run)
    return run


def serialize_run(session: Session, run: WhatsAppInboundExportRun) -> dict[str, Any]:
    counts = dict(
        session.exec(
            select(
                WhatsAppInboundExportItem.status,
                func.count(WhatsAppInboundExportItem.id),
            )
            .where(WhatsAppInboundExportItem.export_run_id == run.id)
            .group_by(WhatsAppInboundExportItem.status)
        ).all()
    )
    return {
        "id": str(run.id),
        "job_id": str(run.job_id),
        "account_id": str(run.account_id),
        "contact_id": str(run.contact_id),
        "contact_name": run.contact_name,
        "date_from": run.date_from,
        "date_to": run.date_to,
        "chat_scope": run.chat_scope,
        "media_types": run.media_types,
        "status": run.status,
        "files_matched": run.files_matched,
        "files_downloaded": run.files_downloaded,
        "files_reused": run.files_reused,
        "files_unavailable": run.files_unavailable,
        "total_bytes": run.total_bytes,
        "item_status_counts": counts,
        "coverage_earliest_at": run.coverage_earliest_at,
        "coverage_latest_at": run.coverage_latest_at,
        "error": run.error,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "updated_at": run.updated_at,
    }
