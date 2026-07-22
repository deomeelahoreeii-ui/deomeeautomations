from __future__ import annotations

from typing import Any

from whatsapp_gateway.models import WhatsAppInboundBatchItem


def serialize_batch_item(item: WhatsAppInboundBatchItem) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "batch_id": str(item.batch_id),
        "attachment_id": str(item.attachment_id),
        "message_id": str(item.message_id),
        "stored_object_id": str(item.stored_object_id) if item.stored_object_id else None,
        "status": item.status,
        "original_filename": item.original_filename,
        "message_timestamp": item.message_timestamp,
        "mime_type": item.mime_type,
        "sha256": item.sha256,
        "size_bytes": item.size_bytes,
        "object_key": item.object_key,
        "error": item.error,
        "created_at": item.created_at,
        "stored_at": item.stored_at,
        "updated_at": item.updated_at,
    }


__all__ = ["serialize_batch_item"]
