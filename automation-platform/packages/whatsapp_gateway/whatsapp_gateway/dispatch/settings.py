from __future__ import annotations

from fastapi import APIRouter

from whatsapp_gateway.api_common import *

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])

@router.get("/settings")
def read_settings(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    return {"account": account_dict(account), "settings": settings_dict(gateway_settings)}

@router.put("/settings")
def update_settings(
    data: SettingsInput, session: Session = Depends(get_session)
) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    for key, value in data.model_dump().items():
        setattr(gateway_settings, key, value)
    gateway_settings.updated_at = utcnow()
    session.add(gateway_settings)
    activity(
        session,
        account,
        "settings_updated",
        "Updated WhatsApp safety and delivery settings",
        details=data.model_dump(),
    )
    session.commit()
    return settings_dict(gateway_settings)

@router.get("/activity")
def activity_log(
    level: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters = [WhatsAppActivity.level == level] if level else []
    total = session.scalar(
        select(func.count()).select_from(WhatsAppActivity).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppActivity)
        .where(*filters)
        .order_by(WhatsAppActivity.created_at.desc())
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            {
                "id": item.id,
                "level": item.level,
                "event_type": item.event_type,
                "message": item.message,
                "details": item.details,
                "created_at": item.created_at,
            }
            for item in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }
