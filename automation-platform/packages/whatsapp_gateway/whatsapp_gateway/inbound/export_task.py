
from __future__ import annotations

import asyncio
import uuid
from pathlib import Path
from typing import Any

from sqlmodel import Session, select

from automation_core.celery_app import celery_app
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.job_service import (
    append_log,
    mark_job_failed,
    mark_job_running,
    mark_job_succeeded,
    record_artifact,
    require_job,
)
from automation_core.time import utcnow
from whatsapp_gateway.inbound.export_items import _load_export_item
from whatsapp_gateway.inbound.export_packaging import package_export
from whatsapp_gateway.inbound.task_snapshots import (
    _snapshot_account,
    _snapshot_attachment,
    _snapshot_message,
)
from whatsapp_gateway.inbound.worker_client import _open_media_client, _request_media_download
from whatsapp_gateway.models import WhatsAppAccount, WhatsAppInboundExportItem, WhatsAppInboundExportRun

@celery_app.task(
    name="whatsapp_gateway.build_inbound_export",
    soft_time_limit=60 * 45,
    time_limit=60 * 50,
)
def build_inbound_export_job(job_id: str) -> dict[str, object]:
    parsed_job_id = uuid.UUID(job_id)
    settings = get_settings()
    with Session(engine) as session:
        job = require_job(session, parsed_job_id)
        run_id = uuid.UUID(str(job.parameters["export_run_id"]))
        run = session.get(WhatsAppInboundExportRun, run_id)
        if run is None:
            raise ValueError("Inbound export run was not found")
        account = session.get(WhatsAppAccount, run.account_id)
        if account is None:
            raise ValueError("Inbound export WhatsApp account was not found")
        run.status = "running"
        run.started_at = utcnow()
        run.updated_at = utcnow()
        session.add(run)
        session.commit()
        mark_job_running(session, parsed_job_id)
        append_log(
            session,
            parsed_job_id,
            f"Preparing {run.files_matched} inbound file(s) from {run.contact_name}.",
        )
        item_ids = list(
            session.exec(
                select(WhatsAppInboundExportItem.id)
                .where(WhatsAppInboundExportItem.export_run_id == run.id)
                .order_by(WhatsAppInboundExportItem.created_at)
            ).all()
        )
        # SQLAlchemy expires ORM attributes on commit.  Never carry ORM model
        # instances outside their Session; immutable value snapshots avoid
        # DetachedInstanceError when the Celery task opens later Sessions.
        account_snapshot = _snapshot_account(account)

    downloaded = reused = unavailable = total_bytes = 0
    loop: asyncio.AbstractEventLoop | None = None
    nats_client: Any = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        nats_client = loop.run_until_complete(_open_media_client(account_snapshot))
        with Session(engine) as session:
            append_log(
                session,
                parsed_job_id,
                f"Connected to WhatsApp worker {account_snapshot.worker_key!r}; media requests will reuse one NATS connection.",
            )

        for index, item_id in enumerate(item_ids, start=1):
            with Session(engine) as session:
                item, attachment, message = _load_export_item(session, item_id)
                run = session.get(WhatsAppInboundExportRun, run_id)
                if run is None:
                    raise ValueError("Inbound export run is missing")
                existing_path = (
                    Path(attachment.stored_path) if attachment.stored_path else None
                )
                if (
                    attachment.download_status == "archived"
                    and existing_path
                    and existing_path.is_file()
                ):
                    item.status = "reused"
                    item.error = None
                    item.updated_at = utcnow()
                    reused += 1
                    total_bytes += int(
                        attachment.actual_size or existing_path.stat().st_size
                    )
                    session.add(item)
                    session.commit()
                    append_log(
                        session,
                        parsed_job_id,
                        f"[{index}/{len(item_ids)}] Reused archived {item.media_category}: "
                        f"{attachment.original_filename or message.message_id}.",
                    )
                    continue

                item.status = "downloading"
                item.error = None
                item.updated_at = utcnow()
                attachment.download_status = "downloading"
                attachment.download_attempts += 1
                attachment.last_error = None
                attachment.updated_at = utcnow()
                session.add(item)
                session.add(attachment)
                session.commit()
                append_log(
                    session,
                    parsed_job_id,
                    f"[{index}/{len(item_ids)}] Downloading {item.media_category}: "
                    f"{attachment.original_filename or message.message_id}.",
                )
                attachment_snapshot = _snapshot_attachment(attachment)
                message_snapshot = _snapshot_message(message)
                expected_category = item.media_category

            try:
                assert loop is not None and nats_client is not None
                loop.run_until_complete(
                    _request_media_download(
                        client=nats_client,
                        account=account_snapshot,
                        attachment=attachment_snapshot,
                        message=message_snapshot,
                    )
                )
                with Session(engine) as session:
                    item, attachment, _message = _load_export_item(session, item_id)
                    stored_path = (
                        Path(attachment.stored_path) if attachment.stored_path else None
                    )
                    if (
                        attachment.download_status != "archived"
                        or not stored_path
                        or not stored_path.is_file()
                    ):
                        raise RuntimeError(
                            "Worker upload completed but the archived file is missing"
                        )
                    if attachment.media_category != expected_category:
                        raise RuntimeError(
                            "Downloaded content type does not match the selected metadata "
                            f"({expected_category} expected, {attachment.media_category or 'unknown'} detected)"
                        )
                    item.status = "ready"
                    item.error = None
                    item.updated_at = utcnow()
                    session.add(item)
                    session.commit()
                    downloaded += 1
                    total_bytes += int(
                        attachment.actual_size or stored_path.stat().st_size
                    )
            except Exception as exc:
                with Session(engine) as session:
                    item, attachment, _message = _load_export_item(session, item_id)
                    item.status = "unavailable"
                    item.error = str(exc)
                    item.updated_at = utcnow()
                    # Keep correctly archived content available for a future scan even
                    # when its actual category differs from the original metadata.
                    if attachment.download_status != "archived":
                        attachment.download_status = "unavailable"
                    attachment.last_error = str(exc)
                    attachment.updated_at = utcnow()
                    session.add(item)
                    session.add(attachment)
                    session.commit()
                    append_log(
                        session,
                        parsed_job_id,
                        f"[{index}/{len(item_ids)}] Unavailable: {exc}",
                        level="warning",
                    )
                    unavailable += 1

            with Session(engine) as session:
                run = session.get(WhatsAppInboundExportRun, run_id)
                if run:
                    run.files_downloaded = downloaded
                    run.files_reused = reused
                    run.files_unavailable = unavailable
                    run.total_bytes = total_bytes
                    run.updated_at = utcnow()
                    session.add(run)
                    session.commit()

        with Session(engine) as session:
            run = session.get(WhatsAppInboundExportRun, run_id)
            if run is None:
                raise ValueError("Inbound export run disappeared")
            run.files_downloaded = downloaded
            run.files_reused = reused
            run.files_unavailable = unavailable
            run.total_bytes = total_bytes
            run.updated_at = utcnow()
            session.add(run)
            session.commit()
            append_log(session, parsed_job_id, "Building ZIP package and audit manifests.")
            output_root = settings.whatsapp_inbound_export_root / str(parsed_job_id)
            packaged = package_export(session, run=run, output_root=output_root)
            record_artifact(
                session,
                parsed_job_id,
                packaged["zip_path"],
                kind="whatsapp_inbound_export_zip",
                name=packaged["zip_path"].name,
            )
            record_artifact(
                session,
                parsed_job_id,
                packaged["manifest_csv"],
                kind="manifest_csv",
            )
            record_artifact(
                session,
                parsed_job_id,
                packaged["unavailable_csv"],
                kind="unavailable_csv",
            )
            record_artifact(
                session,
                parsed_job_id,
                packaged["summary_json"],
                kind="summary_json",
            )
            run.status = "succeeded"
            run.finished_at = utcnow()
            run.updated_at = run.finished_at
            session.add(run)
            session.commit()
            result = {
                "export_run_id": str(run.id),
                "files_matched": run.files_matched,
                "files_downloaded": downloaded,
                "files_reused": reused,
                "files_unavailable": unavailable,
                "total_bytes": total_bytes,
            }
            append_log(
                session,
                parsed_job_id,
                f"Export ready: {downloaded + reused} available, {unavailable} unavailable.",
                level="warning" if unavailable else "info",
            )
            mark_job_succeeded(session, parsed_job_id, result)
            return result
    except Exception as exc:
        with Session(engine) as session:
            run = session.get(WhatsAppInboundExportRun, run_id)
            if run:
                run.status = "failed"
                run.error = str(exc)
                run.finished_at = utcnow()
                run.updated_at = run.finished_at
                session.add(run)
                session.commit()
            append_log(session, parsed_job_id, str(exc), level="error")
            mark_job_failed(session, parsed_job_id, str(exc))
        raise
    finally:
        if loop is not None:
            if nats_client is not None:
                try:
                    loop.run_until_complete(nats_client.close())
                except Exception:
                    pass
            asyncio.set_event_loop(None)
            loop.close()
