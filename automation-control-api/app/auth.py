from __future__ import annotations

from fastapi import Header, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings


bearer_scheme = HTTPBearer(auto_error=False)


def require_api_token(
    credentials: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
    x_api_token: str | None = Header(default=None, alias="X-API-Token"),
) -> None:
    expected = settings.api_token
    if not expected:
        return

    supplied = ""
    if credentials and credentials.scheme.lower() == "bearer":
        supplied = credentials.credentials
    if not supplied and x_api_token:
        supplied = x_api_token

    if supplied != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API token.",
        )
