from fastapi.testclient import TestClient

from automation_api.main import app


def test_antidengue_execution_preflight_allows_idempotency_header() -> None:
    with TestClient(app) as client:
        response = client.options(
            "/api/v1/antidengue/executions",
            headers={
                "Origin": "http://localhost:4321",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type,idempotency-key",
            },
        )

    assert response.status_code == 200
    assert response.text == "OK"
    assert response.headers["access-control-allow-origin"] == "http://localhost:4321"
    allowed_headers = {
        value.strip().lower()
        for value in response.headers["access-control-allow-headers"].split(",")
    }
    assert {"content-type", "idempotency-key"} <= allowed_headers
