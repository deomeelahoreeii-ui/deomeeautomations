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
DEFAULT_APPLICATIONS = {
    "antidengue": (
        "AntiDengue",
        "Dengue activity reports, officer summaries and escalation workflows.",
    ),
    "crm": (
        "CRM",
        "Filtered workbooks, PDFs and CRM operational notifications.",
    ),
    "pmdu": (
        "PMDU",
        "Performance monitoring reports and PMDU notifications.",
    ),
}


DEFAULT_REPORT_TYPES = {
    "antidengue": [
        ("school_activity", "School activity report", "School-level activity evidence."),
        ("officer_summary", "Officer summary", "Officer coverage and follow-up summary."),
        ("wing_summary", "Wing summary", "Consolidated report for a wing audience."),
        (
            "hotspot_distance_activity",
            "Hotspot distance review",
            "Activities selected by published distance and submission-time review rules.",
        ),
        (
            "simple_activity_timing",
            "Simple activity timing review",
            "Before/after activity pairs below the published minimum time interval.",
        ),
    ],
    "crm": [
        ("filtered_workbook", "Filtered workbook", "CRM workbook filtered for an audience."),
        ("filtered_pdf", "Filtered PDF", "CRM PDF filtered for an audience."),
    ],
    "pmdu": [
        ("performance_report", "Performance report", "PMDU performance report for an audience."),
    ],
}


DEFAULT_RECIPIENT_SCOPES = {
    "antidengue": [
        ("individual", "deo", "DEO", 0, "District education officer."),
        ("individual", "ddeo", "DDEO", 1, "Tehsil-level deputy district education officer."),
        ("individual", "aeo", "AEO", 2, "Markaz-level assistant education officer."),
        ("individual", "school_head", "School head", 3, "Head teacher or school focal person."),
        ("individual", "other", "Other individual", 9, "Custom individual recipient role."),
        ("group", "district", "District group", 0, "District-wide WhatsApp group."),
        ("group", "wing", "Wing group", 1, "Wing-specific WhatsApp group."),
        ("group", "tehsil", "Tehsil group", 2, "Tehsil-level WhatsApp group."),
        ("group", "markaz", "Markaz group", 3, "Markaz-level WhatsApp group."),
        ("group", "other", "Other group", 9, "Custom group recipient scope."),
    ],
    "crm": [
        ("individual", "officer", "Officer", 0, "Individual CRM recipient."),
        ("individual", "other", "Other individual", 9, "Custom individual recipient role."),
        ("group", "district", "District group", 0, "District CRM audience."),
        ("group", "wing", "Wing group", 1, "Wing CRM audience."),
        ("group", "tehsil", "Tehsil group", 2, "Tehsil CRM audience."),
        ("group", "other", "Other group", 9, "Custom group recipient scope."),
    ],
    "pmdu": [
        ("individual", "officer", "Officer", 0, "Individual PMDU recipient."),
        ("individual", "other", "Other individual", 9, "Custom individual recipient role."),
        ("group", "district", "District group", 0, "District PMDU audience."),
        ("group", "wing", "Wing group", 1, "Wing PMDU audience."),
        ("group", "tehsil", "Tehsil group", 2, "Tehsil PMDU audience."),
        ("group", "other", "Other group", 9, "Custom group recipient scope."),
    ],
}


def ensure_defaults(session: Session) -> tuple[WhatsAppAccount, WhatsAppSettings]:
    config = get_settings()
    account = session.scalar(
        select(WhatsAppAccount).where(WhatsAppAccount.worker_key == "default")
    )
    if account is None:
        account = WhatsAppAccount(
            name="Primary WhatsApp",
            worker_key="default",
            command_subject=config.whatsapp_command_subject,
            health_subject=config.whatsapp_health_subject,
        )
        session.add(account)
        session.flush()
    applications: dict[str, WhatsAppApplication] = {}
    for key, (name, description) in DEFAULT_APPLICATIONS.items():
        application = session.scalar(
            select(WhatsAppApplication).where(WhatsAppApplication.key == key)
        )
        if application is None:
            application = WhatsAppApplication(
                key=key,
                name=name,
                description=description,
            )
            session.add(application)
            session.flush()
        applications[key] = application
    # Configuration endpoints are commonly loaded in parallel by the UI. Lock
    # the stable application rows before seeding child report types/scopes so
    # two requests cannot both observe a missing default and race the unique
    # (application_id, key) constraint.
    session.flush()
    for application in sorted(applications.values(), key=lambda item: str(item.id)):
        session.execute(
            select(WhatsAppApplication.id)
            .where(WhatsAppApplication.id == application.id)
            .with_for_update()
        )
    for application_key, definitions in DEFAULT_REPORT_TYPES.items():
        application = applications[application_key]
        for key, name, description in definitions:
            existing = session.scalar(
                select(WhatsAppReportType).where(
                    WhatsAppReportType.application_id == application.id,
                    WhatsAppReportType.key == key,
                )
            )
            if existing is None:
                session.add(
                    WhatsAppReportType(
                        application_id=application.id,
                        key=key,
                        name=name,
                        description=description,
                    )
                )
    session.flush()
    for application_key, definitions in DEFAULT_RECIPIENT_SCOPES.items():
        application = applications[application_key]
        for channel, key, name, hierarchy_level, description in definitions:
            existing = session.scalar(
                select(WhatsAppRecipientScope).where(
                    WhatsAppRecipientScope.application_id == application.id,
                    WhatsAppRecipientScope.channel == channel,
                    WhatsAppRecipientScope.key == key,
                )
            )
            if existing is None:
                session.add(
                    WhatsAppRecipientScope(
                        application_id=application.id,
                        channel=channel,
                        key=key,
                        name=name,
                        hierarchy_level=hierarchy_level,
                        description=description,
                    )
                )
    settings = session.scalar(
        select(WhatsAppSettings).where(
            WhatsAppSettings.default_account_id == account.id
        )
    )
    if settings is None:
        settings = WhatsAppSettings(default_account_id=account.id)
        session.add(settings)
    if session.scalar(select(func.count()).select_from(WhatsAppTemplate)) == 0:
        session.add(
            WhatsAppTemplate(
                key="test_message",
                name="Connection test",
                category="system",
                body="WhatsApp gateway test from Deomee Automation Platform.",
            )
        )
    session.commit()
    session.refresh(account)
    session.refresh(settings)
    return account, settings
