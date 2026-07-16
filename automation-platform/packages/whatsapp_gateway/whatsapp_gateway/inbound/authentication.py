from __future__ import annotations

import hmac

from fastapi import Depends, Header, HTTPException, status

from automation_core.config import Settings, get_settings


def verify_worker_token(
    x_whatsapp_worker_token: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> None:
    expected = settings.whatsapp_inbound_ingest_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WHATSAPP_INBOUND_INGEST_TOKEN is not configured",
        )
    if not x_whatsapp_worker_token or not hmac.compare_digest(
        x_whatsapp_worker_token, expected
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid worker token",
        )
