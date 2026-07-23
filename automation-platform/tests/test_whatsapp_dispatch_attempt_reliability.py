from __future__ import annotations

import asyncio
import json
import uuid

from whatsapp_gateway.dispatch.delivery_publisher import (
    batch_acknowledgement_timeout_seconds,
    collect_delivery_results,
    dispatch_attempt_message_id,
)


class _Message:
    def __init__(self, payload: dict[str, object]) -> None:
        self.data = json.dumps(payload).encode("utf-8")


def test_explicit_retry_has_new_broker_identity_but_task_redelivery_does_not() -> None:
    delivery_id = uuid.uuid4()
    first_job_id = str(uuid.uuid4())
    retry_job_id = str(uuid.uuid4())

    first = dispatch_attempt_message_id(delivery_id, first_job_id)

    assert dispatch_attempt_message_id(delivery_id, first_job_id) == first
    assert dispatch_attempt_message_id(delivery_id, retry_job_id) != first


def test_batch_deadline_accounts_for_serial_send_throttling_once() -> None:
    assert batch_acknowledgement_timeout_seconds(
        per_delivery_timeout_seconds=45,
        send_delay_ms=1_500,
        delivery_count=22,
    ) == 77


def test_acknowledgements_are_awaited_concurrently() -> None:
    delivery_ids = [uuid.uuid4(), uuid.uuid4()]
    all_started = asyncio.Event()
    started: set[uuid.UUID] = set()

    class Subscription:
        def __init__(self, delivery_id: uuid.UUID) -> None:
            self.delivery_id = delivery_id

        async def next_msg(self, *, timeout: float) -> _Message:
            started.add(self.delivery_id)
            if len(started) == len(delivery_ids):
                all_started.set()
            await asyncio.wait_for(all_started.wait(), timeout=0.2)
            return _Message({"status": "delivered"})

    async def run() -> dict[uuid.UUID, object]:
        return await collect_delivery_results(
            {
                delivery_id: Subscription(delivery_id)
                for delivery_id in delivery_ids
            },
            acknowledgement_timeout_seconds=1,
        )

    results = asyncio.run(run())

    assert set(results) == set(delivery_ids)
    assert {item.status for item in results.values()} == {"delivered"}


def test_late_status_from_old_attempt_cannot_complete_retry() -> None:
    delivery_id = uuid.uuid4()
    current_attempt = dispatch_attempt_message_id(delivery_id, "retry-job")

    class Subscription:
        def __init__(self) -> None:
            self.messages = [
                _Message(
                    {
                        "status": "failed",
                        "dispatchAttemptId": dispatch_attempt_message_id(
                            delivery_id, "old-job"
                        ),
                    }
                ),
                _Message(
                    {
                        "status": "delivered",
                        "dispatchAttemptId": current_attempt,
                    }
                ),
            ]

        async def next_msg(self, *, timeout: float) -> _Message:
            return self.messages.pop(0)

    result = asyncio.run(
        collect_delivery_results(
            {delivery_id: Subscription()},
            acknowledgement_timeout_seconds=1,
            attempt_message_ids={delivery_id: current_attempt},
        )
    )

    assert result[delivery_id].status == "delivered"
    assert result[delivery_id].provider_result == {
        "status": "delivered",
        "dispatchAttemptId": current_attempt,
    }
