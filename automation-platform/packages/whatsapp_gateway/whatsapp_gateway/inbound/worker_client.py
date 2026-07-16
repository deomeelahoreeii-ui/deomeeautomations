
from __future__ import annotations

import json
from typing import Any

import nats

from automation_core.config import get_settings
from whatsapp_gateway.inbound.task_snapshots import (
    _AccountSnapshot,
    _AttachmentSnapshot,
    _MessageSnapshot,
)

async def _open_media_client(account: _AccountSnapshot):
    settings = get_settings()
    client = await nats.connect(
        settings.whatsapp_nats_url,
        connect_timeout=3,
        max_reconnect_attempts=10,
        reconnect_time_wait=1,
    )
    try:
        health_message = await client.request(account.health_subject, b"{}", timeout=5)
        health = json.loads(health_message.data.decode("utf-8"))
        if not health.get("ready"):
            raise RuntimeError(
                f"WhatsApp worker {account.worker_key!r} is not ready: "
                f"{health.get('status') or 'unknown status'}"
            )
        return client
    except Exception:
        await client.close()
        raise


async def _request_media_download(
    *,
    client: Any,
    account: _AccountSnapshot,
    attachment: _AttachmentSnapshot,
    message: _MessageSnapshot,
) -> dict[str, Any]:
    settings = get_settings()
    payload = {
        "action": "download_attachment",
        "workerId": account.worker_key,
        "attachmentId": str(attachment.id),
        "remoteJid": message.remote_jid,
        "messageId": message.message_id,
        "originalFilename": attachment.original_filename,
        "declaredMimeType": attachment.mime_type,
    }
    subject_base = (
        settings.whatsapp_web_history_subject
        if message.ingestion_source == "web_history"
        else settings.whatsapp_inbound_media_subject
    )
    response = await client.request(
        f"{subject_base}.{account.worker_key}",
        json.dumps(payload).encode("utf-8"),
        timeout=settings.whatsapp_inbound_media_timeout_seconds,
    )
    result = json.loads(response.data.decode("utf-8"))
    if result.get("error"):
        raise RuntimeError(str(result["error"]))
    if not result.get("uploaded"):
        raise RuntimeError("WhatsApp worker did not confirm media upload")
    return result
