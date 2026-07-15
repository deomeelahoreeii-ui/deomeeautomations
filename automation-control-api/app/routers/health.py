from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter

from app.config import settings


router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, object]:
    return {
        "status": "ok",
        "time": datetime.now().isoformat(timespec="seconds"),
        "auth_enabled": bool(settings.api_token),
        "repo_root": str(settings.repo_root),
        "data_dir": str(settings.data_dir),
    }
