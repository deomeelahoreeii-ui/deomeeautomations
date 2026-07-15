import json
import os
import urllib.request


def _request(path: str, payload: dict) -> dict:
    base_url = os.environ.get("CONTROL_API_BASE_URL", "http://127.0.0.1:8787").rstrip("/")
    token = os.environ.get("CONTROL_API_TOKEN", "")
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def main(
    login_mode: str = "auto",
    dry_run: bool = False,
    command: str = "portal",
    file_path: str = "",
) -> dict:
    return _request(
        "/antidengue/run",
        {
            "login_mode": login_mode,
            "dry_run": dry_run,
            "command": command,
            "file_path": file_path,
        },
    )
