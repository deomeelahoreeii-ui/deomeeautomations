from __future__ import annotations

import hashlib
from pathlib import Path
from urllib.parse import urlsplit

from automation_core.config import get_settings


def database_identity(database_url: str | None = None) -> dict[str, str]:
    raw = database_url or get_settings().database_url
    if raw.startswith("sqlite:///"):
        path = Path(raw.removeprefix("sqlite:///")).absolute()
        descriptor = f"sqlite:{path}"
        display = descriptor
    else:
        parsed = urlsplit(raw.replace("+psycopg", ""))
        host = parsed.hostname or "local"
        port = parsed.port or 5432
        database = (parsed.path or "/").lstrip("/") or "default"
        descriptor = f"{parsed.scheme}:{host}:{port}/{database}"
        display = descriptor
    return {
        "display": display,
        "fingerprint": hashlib.sha256(descriptor.encode("utf-8")).hexdigest()[:16],
    }
