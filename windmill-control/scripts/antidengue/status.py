import json
import os
import urllib.request


def _get(path: str) -> dict:
    base_url = os.environ.get("CONTROL_API_BASE_URL", "http://127.0.0.1:8787").rstrip("/")
    request = urllib.request.Request(f"{base_url}{path}", method="GET")
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> dict:
    return _get("/antidengue/status")


if __name__ == "__main__":
    print(json.dumps(main(), indent=2))
