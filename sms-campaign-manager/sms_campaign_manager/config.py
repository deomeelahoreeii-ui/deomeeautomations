from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class GatewayConfig:
    base_url: str
    username: str
    password: str
    device_id: str
    database: Path
    device_max_age_seconds: int
    device_online_timeout_seconds: int

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        load_dotenv()
        base_url = os.getenv("SMS_GATE_BASE_URL", "").strip().rstrip("/")
        username = os.getenv("SMS_GATE_USERNAME", "").strip()
        password = os.getenv("SMS_GATE_PASSWORD", "")
        device_id = os.getenv("SMS_GATE_DEVICE_ID", "").strip()
        database = Path(os.getenv("SMS_GATE_DATABASE", "data/sms-campaigns.sqlite3")).expanduser()
        device_max_age_seconds = int(os.getenv("SMS_GATE_DEVICE_MAX_AGE_SECONDS", "120"))
        device_online_timeout_seconds = int(os.getenv("SMS_GATE_DEVICE_ONLINE_TIMEOUT_SECONDS", "900"))
        missing = [
            name
            for name, value in (
                ("SMS_GATE_BASE_URL", base_url),
                ("SMS_GATE_USERNAME", username),
                ("SMS_GATE_PASSWORD", password),
                ("SMS_GATE_DEVICE_ID", device_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"Missing gateway configuration: {', '.join(missing)}")
        if not base_url.startswith(("https://", "http://")):
            raise ValueError("SMS_GATE_BASE_URL must start with https:// or http://")
        if "/api/" in base_url:
            raise ValueError(
                "SMS_GATE_BASE_URL must stop at /api for a private server; "
                "do not include /3rdparty/v1"
            )
        return cls(base_url, username, password, device_id, database, device_max_age_seconds, device_online_timeout_seconds)
