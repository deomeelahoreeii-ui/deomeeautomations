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
from whatsapp_gateway.configuration.catalog_serialization import recipient_scope_dict, report_type_dict
from whatsapp_gateway.configuration.defaults import ensure_defaults
from whatsapp_gateway.dispatch.template_serialization import template_dict
from whatsapp_gateway.gateway.serialization import account_dict

router = APIRouter()


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
