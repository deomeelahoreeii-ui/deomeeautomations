from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import nats
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import FileResponse
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from pydantic import BaseModel, Field as PydanticField
from sqlalchemy import func, or_, select
from sqlmodel import Session

from automation_core.config import get_settings
from automation_core.database import get_session
from automation_core.time import utcnow
from master_data.models import District, Markaz, Officer, School, SchoolHead, Tehsil, Wing
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppActivity,
    WhatsAppApplication,
    WhatsAppAudience,
    WhatsAppAudienceMember,
    WhatsAppContactLink,
    WhatsAppDelivery,
    WhatsAppDirectoryContact,
    WhatsAppDirectoryGroup,
    WhatsAppDispatchPreview,
    WhatsAppDispatchApproval,
    WhatsAppGroup,
    WhatsAppGroupMember,
    WhatsAppIdentityAlias,
    WhatsAppDispatchProfile,
    WhatsAppRecipientScope,
    WhatsAppReportType,
    WhatsAppSettings,
    WhatsAppTemplate,
)
from whatsapp_gateway.preview_service import (
    cleanup_unreferenced_preview_files,
    delete_preview_records,
)
from whatsapp_gateway.configuration.catalog_serialization import report_type_dict
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.configuration.deletion import _delete_profile_records
from whatsapp_gateway.gateway.activity import activity
from whatsapp_gateway.schemas import ReportTypeInput

router = APIRouter()


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
