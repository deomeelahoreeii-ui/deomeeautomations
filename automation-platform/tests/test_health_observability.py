from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from automation_api.main import app


def test_liveness_does_not_depend_on_external_services() -> None:
    with TestClient(app) as client:
        response = client.get("/livez", headers={"x-request-id": "health-check-1"})

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["x-request-id"] == "health-check-1"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"


def test_readiness_checks_database_without_exposing_its_address() -> None:
    with TestClient(app) as client:
        response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["database"] == "reachable"
    assert "sqlite:" not in response.text
    assert "postgresql:" not in response.text


def test_readiness_returns_503_when_database_is_unavailable() -> None:
    with TestClient(app) as client:
        with patch("automation_api.main.engine.connect", side_effect=RuntimeError("offline")):
            response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "status": "not_ready",
        "database": "unavailable",
    }
