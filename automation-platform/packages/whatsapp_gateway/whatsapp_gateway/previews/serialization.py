from __future__ import annotations

import re
import uuid
from typing import Any

from sqlalchemy import select
from sqlmodel import Session

from whatsapp_gateway.models import (
    WhatsAppDispatchApproval, WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
)

def _digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")
def _delivery_dict(
    session: Session,
    delivery: WhatsAppDispatchPreviewDelivery,
) -> dict[str, Any]:
    artifact_ids = [uuid.UUID(value) for value in delivery.attachment_ids]
    artifacts = (
        session.scalars(
            select(WhatsAppDispatchPreviewArtifact).where(
                WhatsAppDispatchPreviewArtifact.id.in_(artifact_ids)
            )
        ).all()
        if artifact_ids
        else []
    )
    artifacts_by_id = {str(item.id): item for item in artifacts}
    attachments = []
    for artifact_id in delivery.attachment_ids:
        artifact = artifacts_by_id.get(artifact_id)
        if artifact:
            attachments.append(
                {
                    "id": str(artifact.id),
                    "name": artifact.name,
                    "mime_type": artifact.mime_type,
                    "size_bytes": artifact.size_bytes,
                    "checksum_sha256": artifact.checksum_sha256,
                    "status": artifact.status,
                }
            )
    return {
        "id": str(delivery.id),
        "preview_id": str(delivery.preview_id),
        "sequence": delivery.sequence,
        "source_route_key": delivery.source_route_key,
        "target_type": delivery.target_type,
        "target_name": delivery.target_name,
        "target_jid": delivery.target_jid,
        "wing_name": delivery.wing_name,
        "route_kind": delivery.route_kind,
        "route_scope": delivery.route_scope,
        "message": delivery.message,
        "attachments": attachments,
        "routing_snapshot": delivery.routing_snapshot,
        "issues": delivery.issues,
        "status": delivery.status,
        "idempotency_key": delivery.idempotency_key,
        "created_at": delivery.created_at,
    }
def _artifact_dict(artifact: WhatsAppDispatchPreviewArtifact) -> dict[str, Any]:
    return {
        "id": str(artifact.id),
        "preview_id": str(artifact.preview_id),
        "artifact_id": artifact.artifact_id,
        "role": artifact.role,
        "name": artifact.name,
        "mime_type": artifact.mime_type,
        "size_bytes": artifact.size_bytes,
        "checksum_sha256": artifact.checksum_sha256,
        "status": artifact.status,
        "issues": artifact.issues,
        "created_at": artifact.created_at,
    }
def _with_approval(session: Session, item: dict[str, Any]) -> dict[str, Any]:
    approval = session.scalar(
        select(WhatsAppDispatchApproval).where(
            WhatsAppDispatchApproval.preview_id == uuid.UUID(item["id"])
        )
    )
    return {
        **item,
        "approval_status": approval.status if approval else None,
        "approved": approval is not None,
    }
