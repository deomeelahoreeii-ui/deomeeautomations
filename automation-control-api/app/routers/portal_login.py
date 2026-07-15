from __future__ import annotations

from fastapi import APIRouter, Depends

from app.adapters.antidengue import (
    PortalApprovalRequest,
    portal_login_status,
    write_portal_login_action,
)
from app.auth import require_api_token


router = APIRouter(prefix="/portal-login", tags=["portal-login"])


@router.get("/status")
def status() -> dict[str, object]:
    return portal_login_status()


@router.post("/approve", dependencies=[Depends(require_api_token)])
def approve(request: PortalApprovalRequest | None = None) -> dict[str, object]:
    note = request.note if request else ""
    return write_portal_login_action("approve", note=note)


@router.post("/deny", dependencies=[Depends(require_api_token)])
def deny(request: PortalApprovalRequest | None = None) -> dict[str, object]:
    note = request.note if request else ""
    return write_portal_login_action("deny", note=note)
