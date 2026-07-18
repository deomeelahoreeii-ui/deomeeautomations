from __future__ import annotations

from datetime import UTC, datetime, timedelta

import requests
from sqlmodel import Session, col, select

from antidengue_automation.models import AntiDengueScheduleEvent, AntiDengueScheduleExecution
from automation_core.config import get_settings
from automation_core.notifications import ntfy_headers, ntfy_topic_url, ntfy_transport_enabled
from automation_core.time import utcnow

NOTIFICATION_EVENT_TYPES = {
    "antidengue.execution.started",
    "antidengue.report.downloaded",
    "antidengue.messages.sent",
}

EVENT_PRESENTATION = {
    "antidengue.execution.started": ("AntiDengue run started", "runner"),
    "antidengue.report.downloaded": ("AntiDengue report ready", "inbox_tray"),
    "antidengue.messages.sent": ("AntiDengue messages sent", "white_check_mark"),
}


def _eligible(details: dict, now) -> bool:
    delivery = details.get("ntfy_delivery")
    if not delivery:
        return False
    if delivery.get("status") == "sent":
        return False
    next_attempt = delivery.get("next_attempt_at")
    if not next_attempt:
        return True
    parsed = datetime.fromisoformat(str(next_attempt))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC) <= now


def publish_pending_ntfy(session: Session, limit: int = 25) -> dict[str, int]:
    """Best-effort durable ntfy delivery, isolated from execution transitions."""
    settings = get_settings()
    if (
        not ntfy_transport_enabled(settings)
        or not settings.antidengue_ntfy_enabled
        or not settings.antidengue_ntfy_topic.strip()
    ):
        return {"sent": 0, "failed": 0}

    now = utcnow()
    events = session.exec(
        select(AntiDengueScheduleEvent)
        .where(AntiDengueScheduleEvent.event_type.in_(sorted(NOTIFICATION_EVENT_TYPES)))
        .order_by(col(AntiDengueScheduleEvent.created_at).desc())
        .limit(500)
    ).all()
    pending = list(reversed([event for event in events if _eligible(dict(event.details or {}), now)][:limit]))
    sent = failed = 0
    url = ntfy_topic_url(settings.ntfy_publish_url, settings.antidengue_ntfy_topic)
    for event in pending:
        details = dict(event.details or {})
        previous = dict(details.get("ntfy_delivery") or {})
        attempts = int(previous.get("attempts") or 0) + 1
        execution = session.get(AntiDengueScheduleExecution, event.execution_id)
        title, tag = EVENT_PRESENTATION[event.event_type]
        headers = {
            "Title": title,
            "Tags": tag,
            "X-AntiDengue-Event-ID": str(event.id),
            **ntfy_headers(settings),
        }
        try:
            response = requests.post(
                url,
                data=event.message.encode("utf-8"),
                headers=headers,
                timeout=settings.ntfy_timeout_seconds,
            )
            response.raise_for_status()
            details["ntfy_delivery"] = {
                "status": "sent",
                "attempts": attempts,
                "sent_at": str(utcnow()),
                "execution_code": execution.execution_code if execution else None,
            }
            sent += 1
        except requests.RequestException as exc:
            delay_seconds = min(3600, 30 * (2 ** min(attempts - 1, 7)))
            details["ntfy_delivery"] = {
                "status": "retrying",
                "attempts": attempts,
                "last_error": str(exc)[:500],
                "next_attempt_at": str(utcnow() + timedelta(seconds=delay_seconds)),
            }
            failed += 1
        event.details = details
        session.add(event)
        session.commit()
    return {"sent": sent, "failed": failed}
