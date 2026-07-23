from __future__ import annotations

import uuid

from whatsapp_gateway.dispatch.delivery_publisher import publish_frozen_deliveries
from whatsapp_gateway.dispatch.source_reconciliation import (
    reconcile_source_after_terminal_delivery,
)


async def _publish_selected_approved_deliveries(
    approval_id: uuid.UUID, job_id: str, delivery_ids: list[uuid.UUID]
) -> dict[str, int]:
    return await publish_frozen_deliveries(
        approval_id,
        job_id,
        delivery_ids=delivery_ids,
    )


_publish_selected_approved_deliveries = reconcile_source_after_terminal_delivery(
    _publish_selected_approved_deliveries
)
