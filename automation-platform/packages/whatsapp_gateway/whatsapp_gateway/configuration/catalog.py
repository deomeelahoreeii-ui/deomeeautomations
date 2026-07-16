from __future__ import annotations

from fastapi import APIRouter

from whatsapp_gateway.api_common import *

router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])

from whatsapp_gateway.configuration.audiences import audiences

from whatsapp_gateway.dispatch.templates import templates

@router.get("/configuration/options")
def configuration_options(session: Session = Depends(get_session)) -> dict[str, Any]:
    ensure_defaults(session)
    applications = session.scalars(
        select(WhatsAppApplication).order_by(WhatsAppApplication.name)
    ).all()
    report_types = session.execute(
        select(WhatsAppReportType, WhatsAppApplication)
        .join(WhatsAppApplication, WhatsAppApplication.id == WhatsAppReportType.application_id)
        .where(WhatsAppReportType.enabled.is_(True))
        .order_by(WhatsAppApplication.name, WhatsAppReportType.name)
    ).all()
    accounts = session.scalars(
        select(WhatsAppAccount)
        .where(WhatsAppAccount.enabled.is_(True))
        .order_by(WhatsAppAccount.name)
    ).all()
    audiences = session.execute(
        select(WhatsAppAudience, WhatsAppApplication)
        .join(WhatsAppApplication, WhatsAppApplication.id == WhatsAppAudience.application_id)
        .where(WhatsAppAudience.enabled.is_(True))
        .order_by(WhatsAppApplication.name, WhatsAppAudience.name)
    ).all()
    templates = session.scalars(
        select(WhatsAppTemplate)
        .where(WhatsAppTemplate.enabled.is_(True))
        .order_by(WhatsAppTemplate.name)
    ).all()
    wings = session.scalars(
        select(Wing).where(Wing.active.is_(True)).order_by(Wing.name)
    ).all()
    districts = session.scalars(
        select(District).where(District.active.is_(True)).order_by(District.name)
    ).all()
    tehsils = session.scalars(
        select(Tehsil).where(Tehsil.active.is_(True)).order_by(Tehsil.name)
    ).all()
    markazes = session.scalars(
        select(Markaz).where(Markaz.active.is_(True)).order_by(Markaz.name)
    ).all()
    recipient_scopes = session.execute(
        select(WhatsAppRecipientScope, WhatsAppApplication)
        .join(
            WhatsAppApplication,
            WhatsAppApplication.id == WhatsAppRecipientScope.application_id,
        )
        .where(WhatsAppRecipientScope.enabled.is_(True))
        .order_by(
            WhatsAppApplication.name,
            WhatsAppRecipientScope.channel,
            WhatsAppRecipientScope.hierarchy_level,
            WhatsAppRecipientScope.name,
        )
    ).all()
    return {
        "applications": [
            {
                "id": str(item.id),
                "key": item.key,
                "name": item.name,
                "description": item.description,
                "enabled": item.enabled,
            }
            for item in applications
        ],
        "report_types": [
            report_type_dict(item, application)
            for item, application in report_types
        ],
        "audiences": [
            {
                "id": str(item.id),
                "application_id": str(item.application_id),
                "application_key": application.key,
                "name": item.name,
            }
            for item, application in audiences
        ],
        "accounts": [account_dict(item) for item in accounts],
        "templates": [
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
            for item in templates
        ],
        "recipient_scopes": [
            recipient_scope_dict(
                item,
                application,
                session.get(WhatsAppRecipientScope, item.parent_id)
                if item.parent_id
                else None,
            )
            for item, application in recipient_scopes
        ],
        "wings": [
            {
                "id": str(item.id),
                "name": item.name,
                "code": item.code,
                "district_id": str(item.district_id),
            }
            for item in wings
        ],
        "route_areas": [
            *[
                {
                    "scope_key": "district",
                    "value": str(item.id),
                    "label": item.name,
                    "district_id": str(item.id),
                }
                for item in districts
            ],
            *[
                {
                    "scope_key": "wing",
                    "value": str(item.id),
                    "label": item.name,
                    "wing_id": str(item.id),
                    "district_id": str(item.district_id),
                }
                for item in wings
            ],
            *[
                {
                    "scope_key": "tehsil",
                    "value": str(item.id),
                    "label": item.name,
                    "district_id": str(item.district_id),
                }
                for item in tehsils
            ],
            *[
                {
                    "scope_key": "markaz",
                    "value": str(item.id),
                    "label": item.name,
                    "wing_id": str(item.wing_id),
                    "tehsil_id": str(item.tehsil_id),
                }
                for item in markazes
            ],
        ],
    }

@router.get("/report-types")
def report_types(
    application_id: uuid.UUID | None = None,
    session: Session = Depends(get_session),
) -> list[dict[str, Any]]:
    ensure_defaults(session)
    filters = (
        [WhatsAppReportType.application_id == application_id]
        if application_id
        else []
    )
    records = session.execute(
        select(WhatsAppReportType, WhatsAppApplication)
        .join(WhatsAppApplication, WhatsAppApplication.id == WhatsAppReportType.application_id)
        .where(*filters)
        .order_by(WhatsAppApplication.name, WhatsAppReportType.name)
    ).all()
    result = []
    for item, application in records:
        data = report_type_dict(item, application)
        individual_templates = session.scalar(
            select(func.count()).select_from(WhatsAppTemplate).where(
                WhatsAppTemplate.report_type_id == item.id,
                WhatsAppTemplate.category == "report",
                WhatsAppTemplate.recipient_channel == "individual",
                WhatsAppTemplate.enabled.is_(True),
            )
        ) or 0
        group_templates = session.scalar(
            select(func.count()).select_from(WhatsAppTemplate).where(
                WhatsAppTemplate.report_type_id == item.id,
                WhatsAppTemplate.category == "report",
                WhatsAppTemplate.recipient_channel == "group",
                WhatsAppTemplate.enabled.is_(True),
            )
        ) or 0
        data["individual_template_count"] = individual_templates
        data["group_template_count"] = group_templates
        data["template_count"] = individual_templates + group_templates
        result.append(data)
    return result

@router.post("/report-types", status_code=status.HTTP_201_CREATED)
def save_report_type(
    data: ReportTypeInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    application = session.get(WhatsAppApplication, data.application_id)
    if application is None or not application.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    item = session.get(WhatsAppReportType, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="Report type not found")
    duplicate = session.scalar(
        select(WhatsAppReportType).where(
            WhatsAppReportType.application_id == data.application_id,
            WhatsAppReportType.key == data.key,
            WhatsAppReportType.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This report key already exists in the module")
    if item is None:
        item = WhatsAppReportType(
            application_id=data.application_id,
            key=data.key,
            name=data.name,
        )
    elif item.application_id != data.application_id:
        raise HTTPException(status_code=409, detail="A report type cannot move between modules")
    item.key = data.key
    item.name = data.name
    item.description = data.description
    item.artifact_kind = data.artifact_kind
    item.enabled = data.enabled
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "report_type_saved", f"Saved report type {application.name} / {item.name}")
    session.commit()
    return report_type_dict(item, application)

@router.delete("/report-types/{report_type_id}/hard")
def hard_delete_report_type(
    report_type_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppReportType, report_type_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Report type not found")
    paths: set[Path] = set()
    profiles = session.scalars(
        select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.report_type_id == item.id
        )
    ).all()
    for profile in profiles:
        paths.update(_delete_profile_records(session, profile))
    templates = session.scalars(
        select(WhatsAppTemplate).where(WhatsAppTemplate.report_type_id == item.id)
    ).all()
    for template in templates:
        session.delete(template)
    activity(session, account, "report_type_deleted", f"Permanently deleted report type {item.name}")
    session.delete(item)
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"id": str(report_type_id), "deleted": True}

@router.get("/recipient-scopes")
def recipient_scopes(
    application_id: uuid.UUID | None = None,
    channel: Literal["individual", "group"] | None = None,
    session: Session = Depends(get_session),
) -> list[dict[str, Any]]:
    ensure_defaults(session)
    filters: list[Any] = []
    if application_id:
        filters.append(WhatsAppRecipientScope.application_id == application_id)
    if channel:
        filters.append(WhatsAppRecipientScope.channel == channel)
    records = session.execute(
        select(WhatsAppRecipientScope, WhatsAppApplication)
        .join(
            WhatsAppApplication,
            WhatsAppApplication.id == WhatsAppRecipientScope.application_id,
        )
        .where(*filters)
        .order_by(
            WhatsAppApplication.name,
            WhatsAppRecipientScope.channel,
            WhatsAppRecipientScope.hierarchy_level,
            WhatsAppRecipientScope.name,
        )
    ).all()
    return [
        recipient_scope_dict(
            item,
            application,
            session.get(WhatsAppRecipientScope, item.parent_id)
            if item.parent_id
            else None,
        )
        for item, application in records
    ]

@router.post("/recipient-scopes", status_code=status.HTTP_201_CREATED)
def save_recipient_scope(
    data: RecipientScopeInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    application = session.get(WhatsAppApplication, data.application_id)
    if application is None or not application.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    item = session.get(WhatsAppRecipientScope, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="Recipient scope not found")
    duplicate = session.scalar(
        select(WhatsAppRecipientScope).where(
            WhatsAppRecipientScope.application_id == data.application_id,
            WhatsAppRecipientScope.channel == data.channel,
            WhatsAppRecipientScope.key == data.key,
            WhatsAppRecipientScope.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This recipient scope key already exists")
    parent = session.get(WhatsAppRecipientScope, data.parent_id) if data.parent_id else None
    if parent and (
        parent.application_id != data.application_id
        or parent.channel != data.channel
        or parent.id == data.id
        or parent.hierarchy_level >= data.hierarchy_level
    ):
        raise HTTPException(
            status_code=422,
            detail="Parent scope must be a higher-level scope in the same module and channel",
        )
    if item is None:
        item = WhatsAppRecipientScope(
            application_id=data.application_id,
            channel=data.channel,
            key=data.key,
            name=data.name,
        )
    elif item.application_id != data.application_id or item.channel != data.channel:
        raise HTTPException(status_code=409, detail="A recipient scope cannot move between modules or channels")
    item.parent_id = data.parent_id
    item.name = data.name
    item.hierarchy_level = data.hierarchy_level
    item.description = data.description
    item.enabled = data.enabled
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "recipient_scope_saved", f"Saved {application.name} / {item.name}")
    session.commit()
    return recipient_scope_dict(item, application, parent)

@router.delete("/recipient-scopes/{scope_id}/hard")
def hard_delete_recipient_scope(
    scope_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    root = session.get(WhatsAppRecipientScope, scope_id)
    if root is None:
        raise HTTPException(status_code=404, detail="Recipient scope not found")
    paths: set[Path] = set()

    def remove_scope(item: WhatsAppRecipientScope) -> None:
        children = session.scalars(
            select(WhatsAppRecipientScope).where(
                WhatsAppRecipientScope.parent_id == item.id
            )
        ).all()
        for child in children:
            remove_scope(child)
        profiles = session.scalars(
            select(WhatsAppDispatchProfile).where(
                WhatsAppDispatchProfile.recipient_scope_id == item.id
            )
        ).all()
        for profile in profiles:
            paths.update(_delete_profile_records(session, profile))
        templates = session.scalars(
            select(WhatsAppTemplate).where(
                WhatsAppTemplate.recipient_scope_id == item.id
            )
        ).all()
        for template in templates:
            session.delete(template)
        session.delete(item)
        session.flush()

    name = root.name
    remove_scope(root)
    activity(session, account, "recipient_scope_deleted", f"Permanently deleted recipient scope {name}")
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"id": str(scope_id), "deleted": True}
