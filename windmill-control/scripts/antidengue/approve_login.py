import json
import os
import urllib.request


def _post(path: str, payload: dict) -> dict:
    base_url = os.environ.get("CONTROL_API_BASE_URL", "http://127.0.0.1:8787").rstrip("/")
    token = os.environ.get("CONTROL_API_TOKEN", "")
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def main(note: str = "") -> dict:
    return _post("/portal-login/approve", {"note": note})
