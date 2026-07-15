from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import nats

from .contacts import Contact
from .history import AttemptStore, file_sha256


@dataclass(frozen=True)
class PublishSummary:
    batch_id: str
    requested: int
    queued: int
    delivered: int
    failed: int
    rejected: int
    timed_out: int
    unqueued: int
    rejected_contacts: tuple[Contact, ...]


async def publish_contacts(
    contacts: list[Contact],
    *,
    group_jid: str,
    nats_url: str,
    subject: str,
    delay_ms: int,
    wait_timeout: int,
    attempt_store: AttemptStore,
    source_file: Path,
    health_subject: str = "whatsapp.worker.health",
    worker_ready_timeout: int = 120,
    progress: Callable[[str], None] | None = None,
) -> PublishSummary:
    if not group_jid.endswith("@g.us"):
        raise ValueError("group JID must end with @g.us")

    batch_id = uuid.uuid4().hex
    source_digest = file_sha256(source_file)
    status_subject = f"whatsapp.status.group-import.{batch_id}"
    report = progress or (lambda _message: None)
    async def suppress_nats_connect_error(_error: Exception) -> None:
        return

    try:
        connection = await nats.connect(
            nats_url,
            connect_timeout=5,
            allow_reconnect=False,
            max_reconnect_attempts=1,
            reconnect_time_wait=0.1,
            error_cb=suppress_nats_connect_error,
        )
    except Exception as exc:
        raise RuntimeError(
            f"NATS is not available at {nats_url}. Start the NATS service and retry."
        ) from exc
    ready_deadline = asyncio.get_running_loop().time() + worker_ready_timeout
    report(
        f"Waiting up to {worker_ready_timeout} seconds for a connected WhatsApp worker..."
    )
    last_health_error: Exception | None = None
    while True:
        try:
            response = await connection.request(health_subject, b"health", timeout=3)
            health = json.loads(response.data.decode("utf-8"))
            if health.get("ready"):
                report("WhatsApp worker is connected and ready.")
                break
            last_health_error = RuntimeError("Worker responded with ready=false")
        except Exception as exc:
            last_health_error = exc

        remaining_ready = int(ready_deadline - asyncio.get_running_loop().time())
        if remaining_ready <= 0:
            await connection.close()
            raise RuntimeError(
                "WhatsApp worker did not become ready. Open its service log and wait "
                "for 'NATS consumer is listening', then retry."
            ) from last_health_error
        report(
            f"WhatsApp worker not ready yet; {remaining_ready} seconds remaining..."
        )
        await asyncio.sleep(min(5, remaining_ready))
    statuses: asyncio.Queue[dict] = asyncio.Queue()

    async def handle_status(message):
        await statuses.put(json.loads(message.data.decode("utf-8")))

    subscription = await connection.subscribe(status_subject, cb=handle_status)
    jetstream = connection.jetstream()
    try:
        queued = delivered = failed = rejected = timed_out = 0
        rejected_contacts: list[Contact] = []
        deadline = asyncio.get_running_loop().time() + wait_timeout
        for attempt_number, contact in enumerate(contacts, start=1):
            job_id = str(uuid.uuid4())
            reserved = attempt_store.reserve_attempt(
                source_file=source_file,
                source_digest=source_digest,
                group_jid=group_jid,
                contact=contact,
                batch_id=batch_id,
                job_id=job_id,
            )
            if not reserved:
                continue
            report(
                f"Processing {attempt_number}/{len(contacts)}: "
                f"Excel row {contact.source_row}, {contact.phone}"
            )
            payload = {
                "batch_id": batch_id,
                "delay_ms": 0,
                "job_id": job_id,
                "operation": "add_group_participants",
                "participants": [contact.jid],
                "recipient_name": f"Excel row {contact.source_row}",
                "status_subject": status_subject,
                "target": group_jid,
                "type": "group",
            }
            try:
                await jetstream.publish(subject, json.dumps(payload).encode("utf-8"))
            except Exception as exc:
                attempt_store.update_attempt(
                    source_file=source_file,
                    group_jid=group_jid,
                    phone=contact.phone,
                    status="publish_failed",
                    error_message=str(exc),
                )
                raise
            queued += 1
            attempt_store.update_attempt(
                source_file=source_file,
                group_jid=group_jid,
                phone=contact.phone,
                status="queued",
            )
            await connection.flush()
            report(f"  Queued; waiting for WhatsApp result (job {job_id})...")

            if wait_timeout > 0:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    timed_out += 1
                    attempt_store.update_attempt(
                        source_file=source_file,
                        group_jid=group_jid,
                        phone=contact.phone,
                        status="timed_out",
                        error_message="Completion timeout expired",
                    )
                    break
                try:
                    status = await asyncio.wait_for(statuses.get(), timeout=remaining)
                except asyncio.CancelledError:
                    attempt_store.update_attempt(
                        source_file=source_file,
                        group_jid=group_jid,
                        phone=contact.phone,
                        status="interrupted",
                        error_message="Operator stopped the import while awaiting completion",
                    )
                    raise
                except TimeoutError:
                    timed_out += 1
                    attempt_store.update_attempt(
                        source_file=source_file,
                        group_jid=group_jid,
                        phone=contact.phone,
                        status="timed_out",
                        error_message="Completion status was not received",
                    )
                    break
                if status.get("jobId") != job_id:
                    failed += 1
                    attempt_store.update_attempt(
                        source_file=source_file,
                        group_jid=group_jid,
                        phone=contact.phone,
                        status="failed",
                        error_message="Received a mismatched completion status",
                    )
                    break
                if status.get("status") == "delivered":
                    delivered += 1
                    attempt_store.update_attempt(
                        source_file=source_file,
                        group_jid=group_jid,
                        phone=contact.phone,
                        status="added_or_already_member",
                    )
                    report("  Added successfully or already a member.")
                elif status.get("status") == "failed":
                    error_code = str(status.get("errorCode") or "")
                    error_text = str(status.get("error") or "")
                    if not error_code and "status: 403" in error_text:
                        error_code = "403"
                    if error_code == "403":
                        rejected += 1
                        rejected_contacts.append(contact)
                        attempt_store.update_attempt(
                            source_file=source_file,
                            group_jid=group_jid,
                            phone=contact.phone,
                            status="privacy_rejected",
                            error_code=error_code,
                            error_message=error_text,
                        )
                        report("  Not added: participant privacy restriction (403). Continuing.")
                        if attempt_number < len(contacts):
                            await visible_safety_delay(delay_ms, report)
                        continue
                    failed += 1
                    attempt_store.update_attempt(
                        source_file=source_file,
                        group_jid=group_jid,
                        phone=contact.phone,
                        status="failed",
                        error_code=error_code,
                        error_message=error_text,
                    )
                    report(f"  Failed: {error_text or error_code or 'unknown worker error'}. Halting.")
                    break
                else:
                    failed += 1
                    attempt_store.update_attempt(
                        source_file=source_file,
                        group_jid=group_jid,
                        phone=contact.phone,
                        status="failed",
                        error_message=f"Unexpected completion status: {status.get('status')}",
                    )
                    break
                if attempt_number < len(contacts):
                    await visible_safety_delay(delay_ms, report)
        return PublishSummary(
            batch_id=batch_id,
            requested=len(contacts),
            queued=queued,
            delivered=delivered,
            failed=failed,
            rejected=rejected,
            timed_out=timed_out,
            unqueued=len(contacts) - queued,
            rejected_contacts=tuple(rejected_contacts),
        )
    finally:
        await subscription.unsubscribe()
        await connection.drain()


async def visible_safety_delay(
    delay_ms: int,
    report: Callable[[str], None],
) -> None:
    remaining = max(0, delay_ms // 1000)
    if remaining <= 0:
        return
    report(f"  Safety wait: {remaining} seconds before the next contact.")
    while remaining > 0:
        step = min(5, remaining)
        await asyncio.sleep(step)
        remaining -= step
        if remaining > 0:
            report(f"  Safety wait: {remaining} seconds remaining...")
    report("  Safety wait complete; continuing.")
