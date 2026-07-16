from __future__ import annotations

from fastapi import APIRouter

from whatsapp_gateway.api_common import *

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])

@router.get("/templates")
def templates(session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    ensure_defaults(session)
    return [
        template_dict(
            item,
            session.get(WhatsAppApplication, item.application_id).name
            if item.application_id
            else None,
            session.get(WhatsAppReportType, item.report_type_id).name
            if item.report_type_id
            else None,
            session.get(WhatsAppRecipientScope, item.recipient_scope_id).name
            if item.recipient_scope_id
            else None,
        )
        for item in session.scalars(
            select(WhatsAppTemplate).order_by(WhatsAppTemplate.name)
        )
    ]

@router.post("/templates", status_code=status.HTTP_201_CREATED)
def save_template(
    data: TemplateInput, session: Session = Depends(get_session)
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppTemplate, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="WhatsApp template not found")
    duplicate = session.scalar(
        select(WhatsAppTemplate).where(
            WhatsAppTemplate.key == data.key,
            WhatsAppTemplate.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This template key already exists")
    application = (
        session.get(WhatsAppApplication, data.application_id)
        if data.application_id
        else None
    )
    report_type = (
        session.get(WhatsAppReportType, data.report_type_id)
        if data.report_type_id
        else None
    )
    recipient_scope = (
        session.get(WhatsAppRecipientScope, data.recipient_scope_id)
        if data.recipient_scope_id
        else None
    )
    if data.application_id and (application is None or not application.enabled):
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    if data.report_type_id and (
        report_type is None or report_type.application_id != data.application_id
    ):
        raise HTTPException(status_code=422, detail="Report type does not belong to the template module")
    if data.category == "report" and (
        application is None
        or report_type is None
        or data.recipient_channel == "any"
    ):
        raise HTTPException(
            status_code=422,
            detail="Report templates require a module, report type and Individual or Group channel",
        )
    if recipient_scope and (
        application is None
        or recipient_scope.application_id != application.id
        or recipient_scope.channel != data.recipient_channel
        or not recipient_scope.enabled
    ):
        raise HTTPException(status_code=422, detail="Recipient scope does not match the template channel and module")
    if data.recipient_scope_id and data.recipient_channel == "any":
        raise HTTPException(status_code=422, detail="A scoped template requires an Individual or Group channel")
    if item is None:
        item = WhatsAppTemplate(key=data.key, name=data.name, body=data.body)
    item.key = data.key
    item.application_id = data.application_id
    item.report_type_id = data.report_type_id
    item.recipient_scope_id = data.recipient_scope_id
    item.recipient_channel = data.recipient_channel
    item.name = data.name
    item.category = data.category
    item.body = data.body
    item.enabled = data.enabled
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "template_saved", f"Saved WhatsApp template {item.name}")
    session.commit()
    return template_dict(
        item,
        application.name if application else None,
        report_type.name if report_type else None,
        recipient_scope.name if recipient_scope else None,
    )

@router.delete("/templates/{template_id}/hard")
def hard_delete_template(
    template_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppTemplate, template_id)
    if item is None:
        raise HTTPException(status_code=404, detail="WhatsApp template not found")
    profiles = session.scalars(
        select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.template_id == item.id
        )
    ).all()
    for profile in profiles:
        profile.template_id = None
        profile.enabled = False
        profile.version += 1
        profile.updated_at = utcnow()
        session.add(profile)
    name = item.name
    session.delete(item)
    activity(
        session,
        account,
        "template_deleted",
        f"Permanently deleted template {name}; {len(profiles)} dependent profiles were disabled",
    )
    session.commit()
    return {"id": str(template_id), "deleted": True, "disabled_profiles": len(profiles)}
