from __future__ import annotations

from collections import Counter
from typing import Any, Literal

import requests
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlmodel import Session, col, select

from antidengue_automation.models import AntiDengueScheduleEvent
from antidengue_automation.notifications import NOTIFICATION_EVENT_TYPES
from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.notifications import ntfy_headers, ntfy_health, ntfy_topic_url

router = APIRouter(prefix="/api/v1/notifications", tags=["notifications"])


class NotificationTestInput(BaseModel):
    channel: Literal["antidengue"] = "antidengue"
    message: str = Field(default="Automation Platform notification test", min_length=1, max_length=500)


def _channel_registry(settings: Settings) -> list[dict[str, Any]]:
    return [
        {
            "key": "antidengue",
            "name": "AntiDengue operations",
            "enabled": settings.ntfy_enabled and settings.antidengue_ntfy_enabled,
            "topic": settings.antidengue_ntfy_topic,
            "event_types": sorted(NOTIFICATION_EVENT_TYPES),
            "publisher": "AntiDengue scheduler",
        }
    ]


def _delivery_items(session: Session) -> list[dict[str, Any]]:
    events = session.exec(
        select(AntiDengueScheduleEvent)
        .where(AntiDengueScheduleEvent.event_type.in_(sorted(NOTIFICATION_EVENT_TYPES)))
        .order_by(col(AntiDengueScheduleEvent.created_at).desc(), col(AntiDengueScheduleEvent.id).desc())
    ).all()
    items: list[dict[str, Any]] = []
    for event in events:
        details = dict(event.details or {})
        delivery = details.get("ntfy_delivery")
        if not isinstance(delivery, dict):
            continue
        items.append(
            {
                "id": str(event.id),
                "module_key": "antidengue",
                "event_type": event.event_type,
                "message": event.message,
                "status": str(delivery.get("status") or "pending"),
                "attempts": int(delivery.get("attempts") or 0),
                "last_error": delivery.get("last_error"),
                "next_attempt_at": delivery.get("next_attempt_at"),
                "sent_at": delivery.get("sent_at"),
                "execution_id": str(event.execution_id),
                "execution_code": details.get("execution_code") or delivery.get("execution_code"),
                "created_at": event.created_at,
            }
        )
    return items


@router.get("/overview")
def notification_overview(
    settings: Settings = Depends(get_settings),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    deliveries = _delivery_items(session)
    counts = Counter(str(item["status"]) for item in deliveries)
    last_sent = next((item for item in deliveries if item["status"] == "sent"), None)
    health = ntfy_health(settings)
    return {
        "transport": {
            **health,
            "publish_url": settings.ntfy_publish_url,
            "token_configured": bool(settings.ntfy_token),
            "timeout_seconds": settings.ntfy_timeout_seconds,
        },
        "channels": _channel_registry(settings),
        "deliveries": {
            "total": len(deliveries),
            "statuses": dict(sorted(counts.items())),
            "last_sent_at": last_sent["sent_at"] if last_sent else None,
        },
        "available_modes": ["local", "lan", "tailscale", "cloudflare"],
    }


@router.get("/deliveries")
def notification_deliveries(
    status: str = "",
    event_type: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    items = _delivery_items(session)
    if status:
        items = [item for item in items if item["status"] == status]
    if event_type:
        items = [item for item in items if item["event_type"] == event_type]
    total = len(items)
    start = (page - 1) * page_size
    return {
        "items": items[start : start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/test")
def send_test_notification(
    data: NotificationTestInput,
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    if not settings.ntfy_enabled:
        raise HTTPException(status_code=409, detail="The shared ntfy transport is disabled")
    channel = next(item for item in _channel_registry(settings) if item["key"] == data.channel)
    if not channel["enabled"] or not str(channel["topic"]).strip():
        raise HTTPException(status_code=409, detail=f"The {data.channel} ntfy channel is disabled")
    try:
        response = requests.post(
            ntfy_topic_url(settings.ntfy_publish_url, str(channel["topic"])),
            data=data.message.encode("utf-8"),
            headers={
                "Title": "Automation Platform test",
                "Tags": "test_tube,white_check_mark",
                **ntfy_headers(settings),
            },
            timeout=settings.ntfy_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise HTTPException(status_code=503, detail=f"ntfy test delivery failed: {exc}") from exc
    return {
        "sent": True,
        "id": payload.get("id"),
        "topic": payload.get("topic") or channel["topic"],
        "channel": data.channel,
    }
