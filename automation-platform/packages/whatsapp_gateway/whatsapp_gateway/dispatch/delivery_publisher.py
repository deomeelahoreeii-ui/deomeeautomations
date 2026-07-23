from __future__ import annotations

import asyncio
import json
import math
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from sqlalchemy import select
from sqlmodel import Session

from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.job_service import append_log
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppDelivery,
    WhatsAppDispatchApproval,
    WhatsAppSettings,
)


SUCCESSFUL_DELIVERY_STATUSES = frozenset(
    {"delivered", "sent_pending_confirmation"}
)


@dataclass(frozen=True)
class DeliveryResult:
    status: str
    error: str | None
    provider_result: dict[str, Any] | None


def dispatch_attempt_message_id(delivery_id: uuid.UUID, dispatch_job_id: str) -> str:
    """Return the stable broker identity for one logical send attempt.

    Celery may redeliver the same job, so the value must remain stable within a
    job.  An explicit operator retry uses a new Job row and therefore receives a
    new broker identity instead of being suppressed by JetStream de-duplication.
    """

    return f"whatsapp-delivery:{delivery_id}:attempt:{dispatch_job_id}"


def batch_acknowledgement_timeout_seconds(
    *,
    per_delivery_timeout_seconds: int,
    send_delay_ms: int,
    delivery_count: int,
) -> int:
    """Bound the batch wait while accounting for intentional serial throttling."""

    throttle_seconds = math.ceil(
        max(0, delivery_count - 1) * max(0, send_delay_ms) / 1000
    )
    return per_delivery_timeout_seconds + throttle_seconds


async def collect_delivery_results(
    subscriptions: dict[uuid.UUID, Any],
    *,
    acknowledgement_timeout_seconds: int,
    attempt_message_ids: dict[uuid.UUID, str] | None = None,
) -> dict[uuid.UUID, DeliveryResult]:
    """Await every delivery concurrently under one acknowledgement window."""

    async def receive(delivery_id: uuid.UUID, subscription: Any) -> tuple[uuid.UUID, DeliveryResult]:
        deadline = time.monotonic() + acknowledgement_timeout_seconds
        expected_attempt_id = (attempt_message_ids or {}).get(delivery_id)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                message = await subscription.next_msg(timeout=remaining)
            except NatsTimeoutError:
                break
            result = json.loads(message.data.decode("utf-8"))
            if (
                expected_attempt_id
                and result.get("dispatchAttemptId") != expected_attempt_id
            ):
                # A late status from an older attempt must never complete a new
                # operator-authorized retry.
                continue
            return delivery_id, DeliveryResult(
                status=str(result.get("status") or "failed"),
                error=result.get("error"),
                provider_result=result,
            )
        return delivery_id, DeliveryResult(
            status="timed_out",
            error=(
                "Gateway acknowledgement timed out; delivery outcome is "
                "ambiguous and requires reconciliation before retry"
            ),
            provider_result=None,
        )

    pairs = await asyncio.gather(
        *(receive(delivery_id, subscription) for delivery_id, subscription in subscriptions.items())
    )
    return dict(pairs)


def _resolve_attachments(delivery: WhatsAppDelivery) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
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
            raise RuntimeError(
                f"Frozen attachment is missing: {document['filename']}"
            )
    if image_attachment and not Path(image_attachment["path"]).is_file():
        raise RuntimeError(
            f"Frozen attachment is missing: {image_attachment['name']}"
        )
    return image_attachment, documents


async def publish_frozen_deliveries(
    approval_id: uuid.UUID,
    dispatch_job_id: str,
    *,
    delivery_ids: Sequence[uuid.UUID] | None = None,
) -> dict[str, int]:
    """Publish an initial approval or one explicitly selected retry attempt."""

    with Session(engine) as session:
        approval = session.get(WhatsAppDispatchApproval, approval_id)
        if approval is None:
            raise ValueError("Dispatch approval not found")
        approval.status = "sending"
        session.add(approval)
        session.commit()

        statement = select(WhatsAppDelivery).where(
            WhatsAppDelivery.approval_id == approval.id,
            WhatsAppDelivery.status == "queued",
        )
        if delivery_ids is not None:
            statement = statement.where(WhatsAppDelivery.id.in_(delivery_ids))
        deliveries = list(
            session.scalars(
                statement.order_by(WhatsAppDelivery.queued_at, WhatsAppDelivery.id)
            ).all()
        )
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
        acknowledgement_timeout_seconds = (
            gateway_settings.acknowledgement_timeout_seconds
        )

    config = get_settings()
    client = await nats.connect(config.whatsapp_nats_url, connect_timeout=2)
    subscriptions: dict[uuid.UUID, Any] = {}
    try:
        health_message = await client.request(account.health_subject, b"{}", timeout=3)
        health = json.loads(health_message.data.decode("utf-8"))
        if not health.get("ready"):
            raise RuntimeError("WhatsApp gateway is not ready")

        for delivery in deliveries:
            subscriptions[delivery.id] = await client.subscribe(
                delivery.status_subject
            )
        await client.flush()

        jetstream = client.jetstream()
        attempt_message_ids: dict[uuid.UUID, str] = {}
        for delivery in deliveries:
            image_attachment, documents = _resolve_attachments(delivery)
            attempt_id = dispatch_attempt_message_id(delivery.id, dispatch_job_id)
            attempt_message_ids[delivery.id] = attempt_id
            payload = {
                "job_id": str(delivery.id),
                "dispatch_attempt_id": attempt_id,
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
            acknowledgement = await jetstream.publish(
                account.command_subject,
                json.dumps(payload).encode("utf-8"),
                headers={"Nats-Msg-Id": attempt_id},
            )
            with Session(engine) as session:
                stored = session.get(WhatsAppDelivery, delivery.id)
                if stored:
                    stored.queue_stream = acknowledgement.stream
                    stored.queue_sequence = acknowledgement.seq
                    stored.provider_result = {
                        "dispatchAttemptId": attempt_id,
                        "queueAccepted": True,
                        "queueStream": acknowledgement.stream,
                        "queueSequence": acknowledgement.seq,
                    }
                    session.add(stored)
                    session.commit()
                append_log(
                    session,
                    dispatch_job_id,
                    f"Queued frozen delivery for {delivery.recipient_name}.",
                )

        batch_timeout_seconds = batch_acknowledgement_timeout_seconds(
            per_delivery_timeout_seconds=acknowledgement_timeout_seconds,
            send_delay_ms=send_delay_ms,
            delivery_count=len(deliveries),
        )
        results = await collect_delivery_results(
            subscriptions,
            acknowledgement_timeout_seconds=batch_timeout_seconds,
            attempt_message_ids=attempt_message_ids,
        )
        completed = failed = 0
        for delivery in deliveries:
            outcome = results[delivery.id]
            with Session(engine) as session:
                stored = session.get(WhatsAppDelivery, delivery.id)
                if stored:
                    stored.status = outcome.status
                    stored.error = outcome.error
                    stored.provider_result = outcome.provider_result
                    stored.completed_at = utcnow()
                    session.add(stored)
                    session.add(
                        WhatsAppActivity(
                            account_id=stored.account_id,
                            level=(
                                "error"
                                if outcome.status in {"failed", "timed_out"}
                                else "info"
                            ),
                            event_type="approved_delivery_completed",
                            message=(
                                f"Approved delivery {outcome.status} for "
                                f"{stored.recipient_name}"
                            ),
                            details={
                                "delivery_id": str(stored.id),
                                "approval_id": str(approval_id),
                                "dispatch_job_id": dispatch_job_id,
                                "attempt_message_id": dispatch_attempt_message_id(
                                    stored.id, dispatch_job_id
                                ),
                                "error": outcome.error,
                            },
                        )
                    )
                    session.commit()
            if outcome.status in SUCCESSFUL_DELIVERY_STATUSES:
                completed += 1
            else:
                failed += 1

        with Session(engine) as session:
            stored_approval = session.get(WhatsAppDispatchApproval, approval_id)
            if stored_approval:
                stored_approval.status = "completed" if failed == 0 else "failed"
                stored_approval.error = (
                    None if failed == 0 else f"{failed} delivery(s) failed or timed out"
                )
                stored_approval.completed_at = utcnow()
                session.add(stored_approval)
                session.commit()
        return {"delivered": completed, "failed": failed, "total": len(deliveries)}
    finally:
        await client.close()
