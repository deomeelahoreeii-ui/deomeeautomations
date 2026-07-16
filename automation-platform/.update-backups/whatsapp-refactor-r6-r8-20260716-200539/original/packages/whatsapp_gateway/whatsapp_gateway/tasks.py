from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from sqlalchemy import select
from sqlmodel import Session

from automation_core.celery_app import celery_app
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.job_service import (
    append_log,
    mark_job_failed,
    mark_job_running,
    mark_job_succeeded,
    require_job,
)
from automation_core.time import utcnow
from whatsapp_gateway.preview_service import compile_antidengue_preview
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppDelivery,
    WhatsAppDispatchApproval,
    WhatsAppSettings,
)


@celery_app.task(
    name="whatsapp_gateway.compile_dispatch_preview",
    soft_time_limit=60 * 10,
    time_limit=60 * 12,
)
def compile_dispatch_preview_job(job_id: str) -> dict[str, str]:
    uuid.UUID(job_id)
    with Session(engine) as session:
        job = require_job(session, job_id)
        parameters = dict(job.parameters)
        mark_job_running(session, job_id)
        append_log(session, job_id, "Compiling immutable AntiDengue dispatch preview.")

    try:
        with Session(engine) as session:
            preview = compile_antidengue_preview(
                session,
                source_job_id=uuid.UUID(parameters["source_job_id"]),
                dispatch_profile_id=uuid.UUID(parameters["dispatch_profile_id"]),
                created_by=str(parameters.get("created_by") or "web"),
            )
            result = {"preview_id": str(preview.id), "preview_key": preview.preview_key}
        with Session(engine) as session:
            append_log(session, job_id, f"Frozen preview {result['preview_key']} is ready for review.")
            mark_job_succeeded(session, job_id, result)
        return result
    except Exception as exc:
        with Session(engine) as session:
            append_log(session, job_id, str(exc), level="error")
            mark_job_failed(session, job_id, str(exc))
        raise


async def _publish_approved_deliveries(approval_id: uuid.UUID, job_id: str) -> dict[str, int]:
    with Session(engine) as session:
        approval = session.get(WhatsAppDispatchApproval, approval_id)
        if approval is None:
            raise ValueError("Dispatch approval not found")
        approval.status = "sending"
        session.add(approval)
        session.commit()
        deliveries = session.scalars(
            select(WhatsAppDelivery)
            .where(WhatsAppDelivery.approval_id == approval.id)
            .order_by(WhatsAppDelivery.queued_at, WhatsAppDelivery.id)
        ).all()
        if not deliveries:
            raise ValueError("Approved preview has no queued deliveries")
        account = session.get(WhatsAppAccount, deliveries[0].account_id)
        if account is None or not account.enabled:
            raise ValueError("WhatsApp account is unavailable")
        gateway_settings = session.scalar(
            select(WhatsAppSettings).where(
                WhatsAppSettings.default_account_id == account.id
            )
        )
        if gateway_settings is None or not gateway_settings.live_delivery_enabled:
            raise ValueError("Live delivery is disabled in WhatsApp Settings")
        send_delay_ms = gateway_settings.send_delay_ms
        acknowledgement_timeout_seconds = gateway_settings.acknowledgement_timeout_seconds

    config = get_settings()
    client = await nats.connect(config.whatsapp_nats_url, connect_timeout=2)
    subscriptions: dict[uuid.UUID, object] = {}
    try:
        health_message = await client.request(account.health_subject, b"{}", timeout=3)
        health = json.loads(health_message.data.decode("utf-8"))
        if not health.get("ready"):
            raise RuntimeError("WhatsApp gateway is not ready")

        for delivery in deliveries:
            subscriptions[delivery.id] = await client.subscribe(delivery.status_subject)
        await client.flush()

        for delivery in deliveries:
            image_attachment = next(
                (item for item in delivery.attachments if item["mime_type"] == "image/png"),
                None,
            )
            documents = [
                {
                    "path": item["path"],
                    "filename": item["name"],
                    "mimetype": item["mime_type"],
                }
                for item in delivery.attachments
                if item is not image_attachment
            ]
            for document in documents:
                if not Path(document["path"]).is_file():
                    raise RuntimeError(f"Frozen attachment is missing: {document['filename']}")
            payload = {
                "job_id": str(delivery.id),
                "batch_id": str(approval.id),
                "target": delivery.target,
                "type": delivery.recipient_type,
                "recipient_name": delivery.recipient_name,
                "text": delivery.message,
                "documents": documents,
                "image_path": image_attachment["path"] if image_attachment else None,
                "attachment_text_mode": "separate",
                "delay_ms": send_delay_ms,
                "status_subject": delivery.status_subject,
            }
            acknowledgement = await client.jetstream().publish(
                account.command_subject,
                json.dumps(payload).encode("utf-8"),
                headers={"Nats-Msg-Id": str(delivery.id)},
            )
            with Session(engine) as session:
                stored = session.get(WhatsAppDelivery, delivery.id)
                if stored:
                    stored.queue_stream = acknowledgement.stream
                    stored.queue_sequence = acknowledgement.seq
                    session.add(stored)
                    session.commit()
            with Session(engine) as session:
                append_log(session, job_id, f"Queued frozen delivery for {delivery.recipient_name}.")

        completed = failed = 0
        for delivery in deliveries:
            subscription = subscriptions[delivery.id]
            try:
                message = await subscription.next_msg(timeout=acknowledgement_timeout_seconds)
                result = json.loads(message.data.decode("utf-8"))
                delivery_status = str(result.get("status") or "failed")
                error = result.get("error")
            except NatsTimeoutError:
                result = None
                delivery_status = "timed_out"
                error = "Gateway acknowledgement timed out"
            with Session(engine) as session:
                stored = session.get(WhatsAppDelivery, delivery.id)
                if stored:
                    stored.status = delivery_status
                    stored.error = error
                    stored.provider_result = result
                    stored.completed_at = utcnow()
                    session.add(stored)
                    session.add(WhatsAppActivity(
                        account_id=stored.account_id,
                        level="error" if delivery_status in {"failed", "timed_out"} else "info",
                        event_type="approved_delivery_completed",
                        message=f"Approved delivery {delivery_status} for {stored.recipient_name}",
                        details={"delivery_id": str(stored.id), "approval_id": str(approval_id)},
                    ))
                    session.commit()
            if delivery_status in {"delivered", "sent_pending_confirmation"}:
                completed += 1
            else:
                failed += 1

        with Session(engine) as session:
            approval = session.get(WhatsAppDispatchApproval, approval_id)
            if approval:
                approval.status = "completed" if failed == 0 else "failed"
                approval.error = None if failed == 0 else f"{failed} delivery(s) failed or timed out"
                approval.completed_at = utcnow()
                session.add(approval)
                session.commit()
        return {"delivered": completed, "failed": failed, "total": len(deliveries)}
    finally:
        await client.close()


@celery_app.task(
    name="whatsapp_gateway.send_approved_preview",
    soft_time_limit=60 * 15,
    time_limit=60 * 20,
)
def send_approved_preview_job(job_id: str) -> dict[str, int]:
    with Session(engine) as session:
        job = require_job(session, job_id)
        approval_id = uuid.UUID(str(job.parameters["approval_id"]))
        mark_job_running(session, job_id)
        append_log(session, job_id, "Revalidating and queueing the exact approved frozen payloads.")
    try:
        result = asyncio.run(_publish_approved_deliveries(approval_id, job_id))
        with Session(engine) as session:
            append_log(session, job_id, f"Dispatch completed: {result['delivered']} successful, {result['failed']} failed.")
            mark_job_succeeded(session, job_id, result)
        return result
    except Exception as exc:
        with Session(engine) as session:
            approval = session.get(WhatsAppDispatchApproval, approval_id)
            if approval:
                approval.status = "failed"
                approval.error = str(exc)
                approval.completed_at = utcnow()
                session.add(approval)
                for delivery in session.scalars(
                    select(WhatsAppDelivery).where(
                        WhatsAppDelivery.approval_id == approval.id,
                        WhatsAppDelivery.status == "queued",
                    )
                ):
                    delivery.status = "failed"
                    delivery.error = str(exc)
                    delivery.completed_at = utcnow()
                    session.add(delivery)
                session.commit()
            append_log(session, job_id, str(exc), level="error")
            mark_job_failed(session, job_id, str(exc))
        raise
