from __future__ import annotations

import json
from pathlib import Path
from typing import Any


SETTINGS_FILENAME = ".complaints_manager.json"


def settings_path(project_root: Path) -> Path:
    return project_root / SETTINGS_FILENAME


def load_app_settings(project_root: Path) -> dict[str, Any]:
    path = settings_path(project_root)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def save_app_settings(project_root: Path, data: dict[str, Any]) -> None:
    settings_path(project_root).write_text(
        json.dumps(data, indent=2, sort_keys=True),
        encoding="utf-8",
    )

