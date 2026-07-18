from __future__ import annotations

from types import SimpleNamespace

import pytest

from automation_core.config import Settings
from automation_core.notifications import ntfy_health, ntfy_topic_url


def test_ntfy_topic_url_encodes_topic_as_one_path_segment() -> None:
    assert ntfy_topic_url("http://ntfy:80/", "operations alerts") == (
        "http://ntfy:80/operations%20alerts"
    )
    assert ntfy_topic_url("http://ntfy:80", "a/b") == "http://ntfy:80/a%2Fb"


def test_ntfy_exposure_mode_is_validated() -> None:
    assert Settings(ntfy_exposure_mode="tailscale").ntfy_exposure_mode == "tailscale"
    with pytest.raises(ValueError, match="ntfy_exposure_mode"):
        Settings(ntfy_exposure_mode="public")


def test_ntfy_port_is_validated() -> None:
    assert Settings(ntfy_port=2586).ntfy_port == 2586
    with pytest.raises(ValueError, match="ntfy_port"):
        Settings(ntfy_port=70000)


def test_disabled_ntfy_health_does_not_make_request(monkeypatch) -> None:
    monkeypatch.setattr(
        "automation_core.notifications.requests.get",
        lambda *args, **kwargs: pytest.fail("disabled transport must not make a request"),
    )
    settings = SimpleNamespace(
        ntfy_enabled=False,
        ntfy_exposure_mode="local",
        ntfy_public_base_url="http://localhost:2586",
    )
    assert ntfy_health(settings) == {
        "enabled": False,
        "reachable": False,
        "exposure_mode": "local",
        "public_base_url": "http://localhost:2586",
    }


def test_ntfy_health_uses_private_publish_origin(monkeypatch) -> None:
    calls = []

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"healthy": True}

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr("automation_core.notifications.requests.get", fake_get)
    settings = SimpleNamespace(
        ntfy_enabled=True,
        ntfy_publish_url="http://ntfy:80",
        ntfy_public_base_url="https://notify.example.com",
        ntfy_exposure_mode="cloudflare",
        ntfy_timeout_seconds=3.0,
    )
    health = ntfy_health(settings)
    assert health["reachable"] is True
    assert health["public_base_url"] == "https://notify.example.com"
    assert calls == [("http://ntfy:80/v1/health", {"timeout": 3.0})]
