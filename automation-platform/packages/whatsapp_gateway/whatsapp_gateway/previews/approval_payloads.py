from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any, Iterable

from fastapi import HTTPException
from sqlmodel import Session

from automation_core.storage_catalog import ensure_stored_path
from whatsapp_gateway.models import (
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
)


def excluded_delivery_snapshot(
    deliveries: Iterable[WhatsAppDispatchPreviewDelivery],
) -> list[dict[str, Any]]:
    return [
        {
            "id": str(item.id),
            "sequence": item.sequence,
            "target_name": item.target_name,
            "target_jid": item.target_jid,
            "status": item.status,
            "issues": [
                {
                    "code": issue.get("code"),
                    "severity": issue.get("severity"),
                    "message": issue.get("message"),
                }
                for issue in list(item.issues or [])
            ],
        }
        for item in deliveries
    ]


def resolve_delivery_attachments(
    session: Session,
    deliveries: Iterable[WhatsAppDispatchPreviewDelivery],
    artifacts: Iterable[WhatsAppDispatchPreviewArtifact],
) -> dict[uuid.UUID, list[dict[str, Any]]]:
    artifacts_by_id = {str(item.id): item for item in artifacts}
    resolved: dict[uuid.UUID, list[dict[str, Any]]] = {}
    for delivery in deliveries:
        attachments: list[dict[str, Any]] = []
        for artifact_id in list(delivery.attachment_ids or []):
            artifact = artifacts_by_id.get(str(artifact_id))
            if artifact is None or artifact.status == "blocked":
                raise HTTPException(
                    status_code=409,
                    detail="A frozen delivery attachment is invalid",
                )
            try:
                path = ensure_stored_path(
                    session,
                    stored_object_id=artifact.stored_object_id,
                    local_path=Path(artifact.path_snapshot),
                    name=artifact.name,
                    expected_sha256=artifact.checksum_sha256,
                )
            except Exception as exc:
                raise HTTPException(
                    status_code=409,
                    detail=f"Frozen attachment is unavailable: {artifact.name}: {exc}",
                ) from exc
            artifact.path_snapshot = str(path)
            session.add(artifact)
            attachments.append({
                "preview_artifact_id": str(artifact.id),
                "path": str(path),
                "name": artifact.name,
                "mime_type": artifact.mime_type,
                "checksum_sha256": artifact.checksum_sha256,
            })
        resolved[delivery.id] = attachments
    return resolved


def approved_subset_sha256(
    deliveries: Iterable[WhatsAppDispatchPreviewDelivery],
    delivery_attachments: dict[uuid.UUID, list[dict[str, Any]]],
) -> str:
    payload = [
        {
            "id": str(delivery.id),
            "sequence": delivery.sequence,
            "target": delivery.target_jid,
            "message": delivery.message,
            "attachments": [
                item["checksum_sha256"]
                for item in delivery_attachments[delivery.id]
            ],
            "idempotency_key": delivery.idempotency_key,
        }
        for delivery in deliveries
    ]
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


__all__ = [
    "approved_subset_sha256",
    "excluded_delivery_snapshot",
    "resolve_delivery_attachments",
]
