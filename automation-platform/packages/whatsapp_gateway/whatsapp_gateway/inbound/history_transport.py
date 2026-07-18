from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import nats
from fastapi import HTTPException

from automation_core.config import Settings


def provider(settings: Settings) -> str:
    value = str(settings.whatsapp_inbound_history_provider or "wwebjs").strip().lower()
    if value not in {"wwebjs", "baileys"}:
        raise HTTPException(
            status_code=500,
            detail="WHATSAPP_INBOUND_HISTORY_PROVIDER must be wwebjs or baileys",
        )
    return value


def history_subject(settings: Settings, worker_key: str, provider_name: str) -> str:
    base = (
        settings.whatsapp_web_history_subject
        if provider_name == "wwebjs"
        else settings.whatsapp_inbound_history_subject
    )
    return f"{base}.{worker_key}"


def iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def bridge_operator_message(result: dict[str, Any]) -> str:
    if result.get("historyReady") or result.get("ready"):
        return "Managed WhatsApp Web is ready."
    bridge_status = str(result.get("status") or "unavailable").strip().lower()
    messages = {
        "starting": "Managed WhatsApp Web is starting. Retry shortly.",
        "initializing": "Managed WhatsApp Web is starting. Retry shortly.",
        "recovering": "Managed WhatsApp Web is recovering its browser page. Retry shortly.",
        "qr": "Managed WhatsApp Web needs pairing. Scan the QR shown by the bridge process.",
        "authenticated": "Managed WhatsApp Web is authenticated and finishing startup.",
        "failed": "Managed WhatsApp Web could not start. Restart the managed bridge and check its server log.",
        "disconnected": "Managed WhatsApp Web disconnected. Restart the managed bridge.",
        "auth_failure": "Managed WhatsApp Web authentication failed. Refresh the browser snapshot and pair it again.",
    }
    return messages.get(bridge_status, "Managed WhatsApp Web is unavailable.")


async def request_nats(
    *, settings: Settings, subject: str, payload: dict[str, Any],
    timeout: float | None = None,
) -> dict[str, Any]:
    client = await nats.connect(settings.whatsapp_nats_url, connect_timeout=2)
    try:
        message = await client.request(
            subject, json.dumps(payload).encode("utf-8"),
            timeout=timeout or settings.whatsapp_inbound_history_timeout_seconds,
        )
        return json.loads(message.data.decode("utf-8"))
    finally:
        await client.close()


__all__ = [
    "bridge_operator_message", "history_subject", "iso_utc", "provider", "request_nats",
]
