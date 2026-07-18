from __future__ import annotations

from typing import Any
from urllib.parse import quote

import requests

from automation_core.config import Settings, get_settings


def ntfy_topic_url(base_url: str, topic: str) -> str:
    """Build a topic URL without allowing a topic to alter the URL path."""
    normalized_topic = topic.strip()
    if not normalized_topic:
        raise ValueError("ntfy topic cannot be empty")
    return f"{base_url.rstrip('/')}/{quote(normalized_topic, safe='')}"


def ntfy_transport_enabled(settings: Settings | None = None) -> bool:
    current = settings or get_settings()
    return current.ntfy_enabled


def ntfy_headers(settings: Settings | None = None) -> dict[str, str]:
    current = settings or get_settings()
    if not current.ntfy_token:
        return {}
    return {"Authorization": f"Bearer {current.ntfy_token}"}


def ntfy_health(settings: Settings | None = None) -> dict[str, Any]:
    """Return a secret-free snapshot of the configured ntfy origin."""
    current = settings or get_settings()
    configured = current.ntfy_enabled
    result: dict[str, Any] = {
        "enabled": configured,
        "reachable": False,
        "exposure_mode": current.ntfy_exposure_mode,
        "public_base_url": current.ntfy_public_base_url,
    }
    if not configured:
        return result
    try:
        response = requests.get(
            f"{current.ntfy_publish_url}/v1/health",
            timeout=current.ntfy_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        result["reachable"] = payload.get("healthy") is True
    except (requests.RequestException, ValueError) as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"[:300]
    return result
