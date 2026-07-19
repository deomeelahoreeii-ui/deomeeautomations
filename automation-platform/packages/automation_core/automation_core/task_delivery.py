"""Shared idempotency outcomes for durable broker task consumers."""

from __future__ import annotations

import logging
from typing import Any

from automation_core.database_identity import database_identity


def discarded_missing_job_delivery(
    job_id: str,
    *,
    task_name: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Turn a broker delivery whose durable job was deleted into a safe no-op.

    RabbitMQ delivery and PostgreSQL deletion cannot be one atomic transaction.
    A durable message may consequently outlive its job after database restoration,
    test cleanup, or an operator-approved hard deletion.  Consumers must treat
    that tombstone exactly like any other duplicate delivery rather than retrying
    work that no longer has authoritative state.
    """
    identity = database_identity()
    logger.warning(
        "Discarding orphaned durable task delivery task=%s job_id=%s "
        "worker_database=%s (%s)",
        task_name,
        job_id,
        identity["fingerprint"],
        identity["display"],
    )
    return {
        "discarded": True,
        "discard_reason": "owning_job_missing",
        "job_id": job_id,
        "task_name": task_name,
        "worker_database": identity["fingerprint"],
    }
