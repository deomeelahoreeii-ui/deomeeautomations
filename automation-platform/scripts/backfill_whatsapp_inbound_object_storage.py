#!/usr/bin/env python3
from __future__ import annotations

import argparse
import uuid
from collections import defaultdict
from pathlib import Path

from sqlmodel import Session, select

from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.time import utcnow
from whatsapp_gateway.inbound.batches import (
    new_batch_code,
    register_batch_item,
    store_attachment_object,
    write_batch_manifest,
)
from whatsapp_gateway.models import (
    WhatsAppDirectoryContact,
    WhatsAppInboundAttachment,
    WhatsAppInboundBatch,
    WhatsAppInboundBatchItem,
    WhatsAppInboundMessage,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Safely register and upload existing locally archived WhatsApp inbound files "
            "into the configured S3-compatible object store. The default mode is dry-run."
        )
    )
    parser.add_argument("--apply", action="store_true", help="Perform writes; otherwise only report")
    parser.add_argument("--contact-id", type=uuid.UUID, help="Limit the backfill to one contact UUID")
    parser.add_argument("--limit", type=int, default=0, help="Maximum attachments to inspect (0 means all)")
    return parser.parse_args()


def candidate_rows(session: Session, *, contact_id: uuid.UUID | None, limit: int):
    query = (
        select(WhatsAppInboundAttachment, WhatsAppInboundMessage)
        .join(WhatsAppInboundMessage, WhatsAppInboundAttachment.message_id == WhatsAppInboundMessage.id)
        .where(
            WhatsAppInboundAttachment.download_status == "archived",
            WhatsAppInboundAttachment.stored_path.is_not(None),
            WhatsAppInboundMessage.directory_contact_id.is_not(None),
        )
        .order_by(WhatsAppInboundMessage.message_timestamp, WhatsAppInboundAttachment.created_at)
    )
    if contact_id is not None:
        query = query.where(WhatsAppInboundMessage.directory_contact_id == contact_id)
    if limit > 0:
        query = query.limit(limit)
    return list(session.exec(query).all())


def main() -> int:
    args = parse_args()
    settings = get_settings()
    if args.apply and not settings.object_storage_enabled:
        raise SystemExit("OBJECT_STORAGE_ENABLED must be true before using --apply")

    with Session(engine) as session:
        rows = candidate_rows(session, contact_id=args.contact_id, limit=max(args.limit, 0))
        existing = sum(attachment.stored_object_id is not None for attachment, _ in rows)
        missing_paths = sum(
            not attachment.stored_path or not Path(attachment.stored_path).is_file()
            for attachment, _ in rows
        )
        print(f"Eligible archived attachments: {len(rows)}")
        print(f"Already linked to object storage: {existing}")
        print(f"Missing local source files: {missing_paths}")
        if not args.apply:
            print("Dry-run only. Re-run with --apply after verifying object-storage settings.")
            return 0

        grouped: dict[tuple[uuid.UUID, uuid.UUID, str], list[tuple[WhatsAppInboundAttachment, WhatsAppInboundMessage]]] = defaultdict(list)
        for attachment, message in rows:
            if message.directory_contact_id is None:
                continue
            grouped[(message.account_id, message.directory_contact_id, message.worker_key)].append(
                (attachment, message)
            )

        total_stored = total_reused = total_failed = 0
        for (account_id, contact_id, worker_key), group in grouped.items():
            contact = session.get(WhatsAppDirectoryContact, contact_id)
            now = utcnow()
            batch = WhatsAppInboundBatch(
                batch_code=new_batch_code(now, prefix="WAB-LEGACY"),
                account_id=account_id,
                contact_id=contact_id,
                worker_key=worker_key,
                provider="legacy_import",
                requested_count=len(group),
                remote_jid=(contact.phone_jid or contact.primary_lid_jid) if contact else None,
                status="storing",
                storage_backend=settings.object_storage_provider,
                raw_bucket=settings.object_storage_raw_bucket,
                manifest_bucket=settings.object_storage_manifest_bucket,
                created_at=now,
                started_at=now,
                updated_at=now,
            )
            session.add(batch)
            session.flush()

            for attachment, message in group:
                item = register_batch_item(
                    session,
                    batch_id=batch.id,
                    attachment_id=attachment.id,
                    message_id=message.id,
                )
                source_path = Path(attachment.stored_path or "")
                if not source_path.is_file():
                    if item is not None:
                        item.status = "failed"
                        item.error = f"Local archive file is missing: {source_path}"
                        item.updated_at = utcnow()
                        session.add(item)
                    total_failed += 1
                    continue
                try:
                    result = store_attachment_object(
                        session,
                        attachment=attachment,
                        message=message,
                        source_path=source_path,
                        settings=settings,
                    )
                    if result.get("reused"):
                        total_reused += 1
                    else:
                        total_stored += 1
                except Exception as exc:  # report per item and continue the migration
                    if item is not None:
                        item.status = "failed"
                        item.error = str(exc)
                        item.updated_at = utcnow()
                        session.add(item)
                    total_failed += 1

            items = list(
                session.exec(
                    select(WhatsAppInboundBatchItem).where(
                        WhatsAppInboundBatchItem.batch_id == batch.id
                    )
                ).all()
            )
            batch.files_discovered = len(items)
            batch.files_stored = sum(item.status == "stored" for item in items)
            batch.files_reused = sum(item.status == "already_stored" for item in items)
            batch.files_failed = sum(item.status == "failed" for item in items)
            batch.total_bytes = sum(
                int(item.size_bytes or 0)
                for item in items
                if item.status in {"stored", "already_stored"}
            )
            batch.status = "completed_with_errors" if batch.files_failed else "completed"
            batch.finished_at = utcnow()
            batch.updated_at = batch.finished_at
            session.add(batch)
            session.flush()
            write_batch_manifest(session, batch=batch, items=items, settings=settings)
            session.commit()
            print(
                f"{batch.batch_code}: {batch.files_stored} new, "
                f"{batch.files_reused} reused, {batch.files_failed} failed"
            )

        print(
            f"Backfill complete: {total_stored} uploaded, {total_reused} reused, "
            f"{total_failed} failed. Local archive files were retained."
        )
    return 0 if total_failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
