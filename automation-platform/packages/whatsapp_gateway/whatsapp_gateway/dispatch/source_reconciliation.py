"""Reconcile source-domain state after a WhatsApp approval reaches a terminal state."""

from __future__ import annotations

import uuid
import logging
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any, ParamSpec, TypeVar

from sqlmodel import Session

from automation_core.config import Settings
from automation_core.database import engine
from whatsapp_gateway.models import WhatsAppDispatchApproval, WhatsAppDispatchPreview


logger = logging.getLogger(__name__)
P = ParamSpec("P")
R = TypeVar("R")


def reconcile_approval_source(approval_id: uuid.UUID) -> dict[str, Any]:
    """Route delivery completion back to the domain that created the preview.

    The source reference is immutable on a frozen preview, which makes this safe
    to call after the original send and after any retry or repair operation.
    """

    with Session(engine) as session:
        approval = session.get(WhatsAppDispatchApproval, approval_id)
        preview = (
            session.get(WhatsAppDispatchPreview, approval.preview_id)
            if approval is not None
            else None
        )
        if preview is None:
            return {"source_kind": "unknown", "reconciled": False}
        if preview.source_kind != "crm_dispatch_batch" or preview.source_reference_id is None:
            return {"source_kind": preview.source_kind, "reconciled": False}

        # Import lazily to preserve the WhatsApp persistence boundary during
        # worker startup. CRM dispatch already depends on WhatsApp models.
        from crm_domain.dispatch import CrmDispatchService

        result = CrmDispatchService(session, Settings()).refresh(
            preview.source_reference_id
        )
        return {
            "source_kind": preview.source_kind,
            "source_reference_id": str(preview.source_reference_id),
            "reconciled": True,
            "batch_status": result["batch"]["status"],
            "paperless": result.get("reconciliation") or {},
        }


def reconcile_source_after_terminal_delivery(
    publisher: Callable[P, Awaitable[R]],
) -> Callable[P, Awaitable[R]]:
    """Decorate a delivery publisher without changing its stable task contract."""

    @wraps(publisher)
    async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:
        result = await publisher(*args, **kwargs)
        approval_id = kwargs.get("approval_id") or (args[0] if args else None)
        if approval_id is None:
            return result
        try:
            reconcile_approval_source(uuid.UUID(str(approval_id)))
        except Exception:
            # Delivery remains authoritative and successful. The durable CRM sync
            # record and batch Reconcile action provide the retry path.
            logger.exception(
                "dispatch.source_reconciliation.failed",
                extra={"context": {"approval_id": str(approval_id)}},
            )
        return result

    return wrapped


__all__ = [
    "reconcile_approval_source",
    "reconcile_source_after_terminal_delivery",
]
