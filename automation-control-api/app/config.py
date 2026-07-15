from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


API_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = API_ROOT.parent
load_dotenv(API_ROOT / ".env")


@dataclass(frozen=True)
class Settings:
    api_root: Path = API_ROOT
    repo_root: Path = Path(os.getenv("DEOMEE_AUTOMATIONS_ROOT", str(REPO_ROOT)))
    data_dir: Path = Path(os.getenv("CONTROL_API_DATA_DIR", str(API_ROOT / "data")))
    host: str = os.getenv("CONTROL_API_HOST", "127.0.0.1")
    port: int = int(os.getenv("CONTROL_API_PORT", "8787"))
    api_token: str = os.getenv("CONTROL_API_TOKEN", "").strip()

    @property
    def antidengue_dir(self) -> Path:
        return self.repo_root / "antidengue"

    @property
    def antidengue_python(self) -> Path:
        return self.antidengue_dir / ".venv" / "bin" / "python"

    @property
    def nats_dir(self) -> Path:
        return self.repo_root / "nats-server"

    @property
    def whatsappbot_dir(self) -> Path:
        return self.repo_root / "whatsappbot"


settings = Settings()
