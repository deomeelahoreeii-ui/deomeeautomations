from __future__ import annotations

from fastapi import APIRouter

from whatsapp_gateway.api_common import *

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])

@router.get("/overview")
async def overview(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    health = await refresh_health(session, account)
    since = utcnow() - timedelta(hours=24)
    count = lambda model, *filters: session.scalar(
        select(func.count()).select_from(model).where(*filters)
    ) or 0
    return {
        "account": account_dict(account),
        "health": health,
        "settings": settings_dict(gateway_settings),
        "counts": {
            "groups": count(WhatsAppGroup, WhatsAppGroup.enabled.is_(True)),
            "queued": count(WhatsAppDelivery, WhatsAppDelivery.status == "queued"),
            "delivered_24h": count(
                WhatsAppDelivery,
                WhatsAppDelivery.status.in_(["delivered", "sent_pending_confirmation"]),
                WhatsAppDelivery.queued_at >= since,
            ),
            "failed_24h": count(
                WhatsAppDelivery,
                WhatsAppDelivery.status.in_(["failed", "timed_out"]),
                WhatsAppDelivery.queued_at >= since,
            ),
            "recipients": count(Officer, Officer.active.is_(True))
            + count(SchoolHead, SchoolHead.active.is_(True)),
        },
    }

@router.get("/connection")
async def connection(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    health = await refresh_health(session, account)
    return {
        "account": account_dict(account),
        "health": health,
        "settings": settings_dict(gateway_settings),
    }

@router.get("/connection/qr")
def connection_qr() -> FileResponse:
    path = get_settings().whatsapp_qr_image_path
    if not path.exists():
        raise HTTPException(status_code=404, detail="No WhatsApp login QR is pending")
    return FileResponse(path, media_type="image/png", filename="whatsapp-login-qr.png")
