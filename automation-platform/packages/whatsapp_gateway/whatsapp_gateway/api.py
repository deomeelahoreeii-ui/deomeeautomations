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


router = APIRouter(prefix="/api/v1/whatsapp", tags=["whatsapp"])


def _delete_profile_records(
    session: Session,
    profile: WhatsAppDispatchProfile,
) -> set[Path]:
    paths: set[Path] = set()
    previews = session.scalars(
        select(WhatsAppDispatchPreview).where(
            WhatsAppDispatchPreview.dispatch_profile_id == profile.id
        )
    ).all()
    for preview in previews:
        if session.scalar(
            select(WhatsAppDispatchApproval.id).where(
                WhatsAppDispatchApproval.preview_id == preview.id
            )
        ):
            raise HTTPException(
                status_code=409,
                detail="This setup has an approved dispatch and must be retained for audit",
            )
        paths.update(delete_preview_records(session, preview))
    session.delete(profile)
    session.flush()
    return paths


def _delete_audience_records(
    session: Session,
    audience: WhatsAppAudience,
) -> set[Path]:
    paths: set[Path] = set()
    profiles = session.scalars(
        select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.audience_id == audience.id
        )
    ).all()
    for profile in profiles:
        paths.update(_delete_profile_records(session, profile))
    members = session.scalars(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience.id
        )
    ).all()
    for member in members:
        session.delete(member)
    session.flush()
    session.delete(audience)
    session.flush()
    return paths


def _delete_guided_setup_records(
    session: Session,
    profile: WhatsAppDispatchProfile,
) -> tuple[set[Path], bool, bool]:
    """Delete wizard-owned configuration while preserving shared resources."""
    audience = session.get(WhatsAppAudience, profile.audience_id)
    template = session.get(WhatsAppTemplate, profile.template_id) if profile.template_id else None
    audience_deleted = False
    template_deleted = False
    profile_count = session.scalar(
        select(func.count())
        .select_from(WhatsAppDispatchProfile)
        .where(WhatsAppDispatchProfile.audience_id == profile.audience_id)
    ) or 0
    if profile.owns_audience and audience is not None and profile_count == 1:
        paths = _delete_audience_records(session, audience)
        audience_deleted = True
    else:
        paths = _delete_profile_records(session, profile)

    if profile.owns_template and template is not None:
        remaining_template_profiles = session.scalar(
            select(func.count())
            .select_from(WhatsAppDispatchProfile)
            .where(WhatsAppDispatchProfile.template_id == template.id)
        ) or 0
        if remaining_template_profiles == 0:
            session.delete(template)
            session.flush()
            template_deleted = True
    return paths, audience_deleted, template_deleted

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


class TestMessageInput(BaseModel):
    target: str = PydanticField(min_length=10, max_length=80)
    recipient_name: str = PydanticField(default="Test recipient", max_length=200)
    message: str = PydanticField(min_length=1, max_length=3500)


class GroupInput(BaseModel):
    id: uuid.UUID | None = None
    directory_group_id: uuid.UUID | None = None
    wing_id: uuid.UUID
    name: str = PydanticField(min_length=1, max_length=200)
    jid: str = PydanticField(min_length=10, max_length=100)
    purpose: str = PydanticField(default="reports", max_length=80)
    notes: str = PydanticField(default="", max_length=1000)
    enabled: bool = True


class TemplateInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID | None = None
    report_type_id: uuid.UUID | None = None
    recipient_scope_id: uuid.UUID | None = None
    recipient_channel: Literal["individual", "group", "any"] = "any"
    key: str = PydanticField(min_length=1, max_length=100)
    name: str = PydanticField(min_length=1, max_length=200)
    category: str = PydanticField(default="report", max_length=80)
    body: str = PydanticField(min_length=1, max_length=5000)
    enabled: bool = True


class SettingsInput(BaseModel):
    send_delay_ms: int = PydanticField(ge=0, le=60_000)
    acknowledgement_timeout_seconds: int = PydanticField(ge=5, le=180)
    max_retries: int = PydanticField(ge=1, le=10)
    require_preview: bool
    live_delivery_enabled: bool


class ReportTypeInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    key: str = PydanticField(min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$")
    name: str = PydanticField(min_length=2, max_length=200)
    description: str = PydanticField(default="", max_length=1000)
    artifact_kind: str = PydanticField(default="document", max_length=80)
    enabled: bool = True


class RecipientScopeInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    parent_id: uuid.UUID | None = None
    channel: Literal["individual", "group"]
    key: str = PydanticField(min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$")
    name: str = PydanticField(min_length=2, max_length=200)
    hierarchy_level: int = PydanticField(default=0, ge=0, le=99)
    description: str = PydanticField(default="", max_length=1000)
    enabled: bool = True


class AudienceInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    key: str | None = PydanticField(
        default=None, min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$"
    )
    name: str = PydanticField(min_length=2, max_length=200)
    description: str = PydanticField(default="", max_length=1000)
    enabled: bool = True


class AudienceMemberInput(BaseModel):
    target_type: Literal["group", "contact"]
    target_id: uuid.UUID
    wing_id: uuid.UUID | None = None
    route_scope_key: str = PydanticField(default="", max_length=100)
    route_scope_value: str = PydanticField(default="", max_length=100)
    route_scope_label: str = PydanticField(default="", max_length=200)


class DispatchProfileInput(BaseModel):
    id: uuid.UUID | None = None
    application_id: uuid.UUID
    key: str = PydanticField(min_length=2, max_length=100, pattern=r"^[a-z0-9_]+$")
    name: str = PydanticField(min_length=2, max_length=200)
    report_type_id: uuid.UUID
    audience_id: uuid.UUID
    account_id: uuid.UUID
    template_id: uuid.UUID | None = None
    recipient_scope_id: uuid.UUID | None = None
    recipient_channel: Literal["individual", "group"] = "group"
    wing_id: uuid.UUID | None = None
    delivery_mode: Literal["groups", "individuals"] = "groups"
    require_approval: bool = True
    fallback_policy: Literal["none", "same_scope"] = "none"
    max_retries: int = PydanticField(default=5, ge=0, le=10)
    messages_per_minute: int = PydanticField(default=20, ge=1, le=120)
    message_style: Literal["summary", "detailed"] = "summary"
    attachment_mode: Literal["none", "image", "excel", "image_excel"] = "image_excel"
    image_content: Literal["summary", "details"] = "details"
    enabled: bool = True
    notes: str = PydanticField(default="", max_length=2000)


class ReportingRouteSetupInput(BaseModel):
    """One guided transaction for a complete group reporting route."""

    profile_id: uuid.UUID | None = None
    application_id: uuid.UUID | None = None
    report_type_id: uuid.UUID | None = None
    wing_id: uuid.UUID | None = None
    recipient_scope_id: uuid.UUID | None = None
    account_id: uuid.UUID | None = None
    directory_group_id: uuid.UUID
    route_scope_value: str = PydanticField(min_length=1, max_length=100)
    route_scope_label: str = PydanticField(min_length=1, max_length=200)
    template_id: uuid.UUID | None = None
    create_recommended_template: bool = True
    audience_name: str = PydanticField(default="", max_length=200)
    profile_name: str = PydanticField(default="", max_length=200)
    template_name: str = PydanticField(default="", max_length=200)
    template_body: str = PydanticField(default="{{report_body}}", min_length=1, max_length=5000)
    require_approval: bool = True
    max_retries: int = PydanticField(default=5, ge=0, le=10)
    messages_per_minute: int = PydanticField(default=20, ge=1, le=120)
    message_style: Literal["summary", "detailed"] = "summary"
    attachment_mode: Literal["none", "image", "excel", "image_excel"] = "image_excel"
    image_content: Literal["summary", "details"] = "details"


def gateway_datetime(value: Any, fallback: datetime | None = None) -> datetime:
    if isinstance(value, datetime):
        return value
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            pass
    return fallback or utcnow()


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


def activity(
    session: Session,
    account: WhatsAppAccount | None,
    event_type: str,
    message: str,
    *,
    level: str = "info",
    details: dict[str, Any] | None = None,
) -> None:
    session.add(
        WhatsAppActivity(
            account_id=account.id if account else None,
            event_type=event_type,
            level=level,
            message=message,
            details=details,
        )
    )


async def worker_health(account: WhatsAppAccount) -> dict[str, Any]:
    config = get_settings()
    client = None
    try:
        client = await nats.connect(
            config.whatsapp_nats_url,
            connect_timeout=1,
            max_reconnect_attempts=0,
        )
        message = await client.request(account.health_subject, b"{}", timeout=2)
        result = json.loads(message.data.decode("utf-8"))
        identity_store = result.get("identityStore") or {}
        return {
            "reachable": True,
            "ready": bool(result.get("ready")),
            "status": result.get("status") or "unknown",
            "checked_at": result.get("checkedAt"),
            "worker_key": result.get("workerId") or account.worker_key,
            "whatsapp_connected": bool(result.get("whatsappConnected")),
            "nats_consumer_ready": bool(result.get("natsConsumerReady")),
            "reconnect_scheduled": bool(result.get("reconnectScheduled")),
            "last_connection_status": result.get("lastConnectionStatus"),
            "known_identities": int(identity_store.get("identities") or 0),
            "target_policy": result.get("targetPolicy"),
        }
    except (NoRespondersError, NatsTimeoutError) as exc:
        return {"reachable": False, "ready": False, "status": "unavailable", "error": str(exc)}
    except Exception as exc:
        return {"reachable": False, "ready": False, "status": "error", "error": str(exc)}
    finally:
        if client is not None:
            await client.close()


async def gateway_request(
    subject: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: float = 20,
) -> dict[str, Any]:
    config = get_settings()
    client = None
    try:
        client = await nats.connect(
            config.whatsapp_nats_url,
            connect_timeout=2,
            max_reconnect_attempts=0,
        )
        message = await client.request(
            subject,
            json.dumps(payload or {}).encode("utf-8"),
            timeout=timeout,
        )
        result = json.loads(message.data.decode("utf-8"))
        if result.get("error"):
            raise HTTPException(status_code=502, detail=str(result["error"]))
        return result
    except HTTPException:
        raise
    except (NoRespondersError, NatsTimeoutError) as exc:
        raise HTTPException(
            status_code=503,
            detail="WhatsApp gateway directory service is unavailable",
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    finally:
        if client is not None:
            await client.close()


async def refresh_health(session: Session, account: WhatsAppAccount) -> dict[str, Any]:
    health = await worker_health(account)
    account.status = str(health.get("status") or "unknown")
    account.connected = bool(health.get("whatsapp_connected"))
    account.qr_available = get_settings().whatsapp_qr_image_path.exists()
    account.last_seen_at = utcnow() if health.get("reachable") else account.last_seen_at
    account.last_error = str(health.get("error") or "") or None
    account.updated_at = utcnow()
    session.add(account)
    session.commit()
    return health


def account_dict(account: WhatsAppAccount) -> dict[str, Any]:
    return {
        "id": str(account.id),
        "name": account.name,
        "worker_key": account.worker_key,
        "enabled": account.enabled,
        "command_subject": account.command_subject,
        "health_subject": account.health_subject,
        "status": account.status,
        "connected": account.connected,
        "qr_available": account.qr_available,
        "last_seen_at": account.last_seen_at,
        "last_error": account.last_error,
    }


def settings_dict(settings: WhatsAppSettings) -> dict[str, Any]:
    return {
        "send_delay_ms": settings.send_delay_ms,
        "acknowledgement_timeout_seconds": settings.acknowledgement_timeout_seconds,
        "max_retries": settings.max_retries,
        "require_preview": settings.require_preview,
        "live_delivery_enabled": settings.live_delivery_enabled,
    }


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


def directory_group_dict(
    item: WhatsAppDirectoryGroup,
    configured: WhatsAppGroup | None = None,
    wing: Wing | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "jid": item.jid,
        "name": item.name,
        "description": item.description,
        "owner_jid": item.owner_jid,
        "participant_count": item.participant_count,
        "available": item.available,
        "discovered_at": item.discovered_at,
        "last_seen_at": item.last_seen_at,
        "last_synced_at": item.last_synced_at,
        "configured_id": str(configured.id) if configured else None,
        "wing_id": str(configured.wing_id) if configured else None,
        "wing_name": wing.name if wing else None,
        "purpose": configured.purpose if configured else None,
        "dispatch_enabled": configured.enabled if configured else False,
    }


def upsert_directory_contact(
    session: Session,
    account: WhatsAppAccount,
    source: dict[str, Any],
    synced_at: datetime,
) -> WhatsAppDirectoryContact | None:
    phone_jid = str(source.get("phoneJid") or source.get("phone_jid") or "").strip() or None
    primary_lid = str(
        source.get("primaryLidJid") or source.get("primary_lid_jid") or ""
    ).strip() or None
    canonical_key = phone_jid or primary_lid
    if not canonical_key:
        return None
    item = session.scalar(
        select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account.id,
            WhatsAppDirectoryContact.canonical_key == canonical_key,
        )
    )
    if item is None and primary_lid:
        alias = session.scalar(
            select(WhatsAppIdentityAlias).where(
                WhatsAppIdentityAlias.account_id == account.id,
                WhatsAppIdentityAlias.lid_jid == primary_lid,
            )
        )
        item = session.get(WhatsAppDirectoryContact, alias.contact_id) if alias else None
    if item is None:
        item = WhatsAppDirectoryContact(
            account_id=account.id,
            canonical_key=canonical_key,
            first_seen_at=gateway_datetime(source.get("firstSeenAt"), synced_at),
        )
    token = source.get("token") or {}
    item.canonical_key = phone_jid or item.canonical_key
    item.phone_jid = phone_jid or item.phone_jid
    item.primary_lid_jid = primary_lid or item.primary_lid_jid
    item.display_name = str(source.get("displayName") or source.get("name") or item.display_name)
    item.source = str(source.get("source") or item.source or "gateway")
    item.confidence = float(source.get("confidence") or item.confidence or 0)
    item.active = bool(source.get("active", True))
    item.last_seen_at = gateway_datetime(source.get("lastSeenAt"), synced_at)
    item.last_synced_at = synced_at
    if token:
        item.token_status = str(token.get("status") or "unknown")
        item.token_checked_at = gateway_datetime(token.get("checkedAt"), synced_at)
        item.token_issued_at = (
            gateway_datetime(token.get("issuedAt")) if token.get("issuedAt") else None
        )
    session.add(item)
    session.flush()
    aliases = source.get("aliases") or []
    if primary_lid and not any(
        (alias.get("lidJid") or alias.get("lid_jid")) == primary_lid
        for alias in aliases
        if isinstance(alias, dict)
    ):
        aliases = [*aliases, {"lidJid": primary_lid, "source": item.source}]
    for alias_source in aliases:
        if not isinstance(alias_source, dict):
            continue
        lid_jid = str(alias_source.get("lidJid") or alias_source.get("lid_jid") or "").strip()
        if not lid_jid:
            continue
        alias = session.scalar(
            select(WhatsAppIdentityAlias).where(
                WhatsAppIdentityAlias.account_id == account.id,
                WhatsAppIdentityAlias.lid_jid == lid_jid,
            )
        )
        if alias is None:
            alias = WhatsAppIdentityAlias(
                account_id=account.id,
                contact_id=item.id,
                lid_jid=lid_jid,
                first_seen_at=gateway_datetime(alias_source.get("firstSeenAt"), synced_at),
            )
        alias.contact_id = item.id
        alias.source = str(alias_source.get("source") or item.source)
        alias.confidence = float(alias_source.get("confidence") or item.confidence)
        alias.last_seen_at = gateway_datetime(alias_source.get("lastSeenAt"), synced_at)
        session.add(alias)
    return item


@router.post("/directory/sync")
async def sync_directory(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    config = get_settings()
    groups_result = await gateway_request(
        config.whatsapp_group_directory_subject,
        {"refresh": True},
        timeout=30,
    )
    identities_result = await gateway_request(
        config.whatsapp_identity_directory_subject,
        {"action": "list"},
        timeout=30,
    )
    synced_at = gateway_datetime(
        identities_result.get("checkedAt") or groups_result.get("checkedAt")
    )
    for item in session.scalars(
        select(WhatsAppDirectoryGroup).where(
            WhatsAppDirectoryGroup.account_id == account.id
        )
    ):
        item.available = False
        session.add(item)
    for source in groups_result.get("groups") or []:
        jid = str(source.get("jid") or "").strip()
        if not jid.endswith("@g.us"):
            continue
        item = session.scalar(
            select(WhatsAppDirectoryGroup).where(
                WhatsAppDirectoryGroup.account_id == account.id,
                WhatsAppDirectoryGroup.jid == jid,
            )
        )
        if item is None:
            item = WhatsAppDirectoryGroup(account_id=account.id, jid=jid)
        item.name = str(source.get("name") or item.name or jid)
        item.description = str(source.get("description") or "")
        item.owner_jid = str(source.get("ownerJid") or "") or None
        item.participant_count = int(source.get("participantCount") or 0)
        item.available = True
        item.last_seen_at = synced_at
        item.last_synced_at = synced_at
        session.add(item)
    for item in session.scalars(
        select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account.id
        )
    ):
        item.active = False
        session.add(item)
    contact_count = 0
    for source in identities_result.get("contacts") or []:
        if upsert_directory_contact(session, account, source, synced_at):
            contact_count += 1
    activity(
        session,
        account,
        "directory_synchronized",
        "Synchronized WhatsApp groups and identities",
        details={
            "groups": len(groups_result.get("groups") or []),
            "contacts": contact_count,
        },
    )
    session.commit()
    return {
        "groups": len(groups_result.get("groups") or []),
        "contacts": contact_count,
        "synced_at": synced_at,
    }


@router.get("/directory/summary")
def directory_summary(session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    count = lambda model, *filters: session.scalar(
        select(func.count()).select_from(model).where(*filters)
    ) or 0
    last_group_sync = session.scalar(
        select(func.max(WhatsAppDirectoryGroup.last_synced_at)).where(
            WhatsAppDirectoryGroup.account_id == account.id
        )
    )
    last_contact_sync = session.scalar(
        select(func.max(WhatsAppDirectoryContact.last_synced_at)).where(
            WhatsAppDirectoryContact.account_id == account.id
        )
    )
    return {
        "groups": count(
            WhatsAppDirectoryGroup,
            WhatsAppDirectoryGroup.account_id == account.id,
            WhatsAppDirectoryGroup.available.is_(True),
        ),
        "configured_groups": count(
            WhatsAppGroup,
            WhatsAppGroup.account_id == account.id,
            WhatsAppGroup.enabled.is_(True),
        ),
        "contacts": count(
            WhatsAppDirectoryContact,
            WhatsAppDirectoryContact.account_id == account.id,
            WhatsAppDirectoryContact.active.is_(True),
        ),
        "lid_mappings": count(
            WhatsAppIdentityAlias,
            WhatsAppIdentityAlias.account_id == account.id,
        ),
        "last_synced_at": max(
            (value for value in [last_group_sync, last_contact_sync] if value),
            default=None,
        ),
    }


@router.get("/directory/groups")
def directory_groups(
    search: str = "",
    availability: Literal["all", "available", "unavailable"] = "all",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    filters: list[Any] = [WhatsAppDirectoryGroup.account_id == account.id]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppDirectoryGroup.name.ilike(pattern),
                WhatsAppDirectoryGroup.jid.ilike(pattern),
            )
        )
    if availability != "all":
        filters.append(
            WhatsAppDirectoryGroup.available.is_(availability == "available")
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDirectoryGroup).where(*filters)
    ) or 0
    records = session.execute(
        select(WhatsAppDirectoryGroup, WhatsAppGroup, Wing)
        .outerjoin(
            WhatsAppGroup,
            WhatsAppGroup.directory_group_id == WhatsAppDirectoryGroup.id,
        )
        .outerjoin(Wing, Wing.id == WhatsAppGroup.wing_id)
        .where(*filters)
        .order_by(WhatsAppDirectoryGroup.name, WhatsAppDirectoryGroup.jid)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            directory_group_dict(item, configured, wing)
            for item, configured, wing in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/directory/groups/{group_id}")
def directory_group(group_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppDirectoryGroup, group_id)
    if item is None or item.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    configured = session.scalar(
        select(WhatsAppGroup).where(WhatsAppGroup.directory_group_id == item.id)
    )
    wing = session.get(Wing, configured.wing_id) if configured else None
    return directory_group_dict(item, configured, wing)


@router.post("/directory/groups/{group_id}/members/sync")
async def sync_group_members(
    group_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    group = session.get(WhatsAppDirectoryGroup, group_id)
    if group is None or group.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    result = await gateway_request(
        get_settings().whatsapp_group_members_subject,
        {"groupJid": group.jid},
        timeout=30,
    )
    synced_at = gateway_datetime(result.get("checkedAt"))
    for item in session.scalars(
        select(WhatsAppGroupMember).where(
            WhatsAppGroupMember.directory_group_id == group.id
        )
    ):
        item.active = False
        session.add(item)
    for source in result.get("members") or []:
        member_jid = str(source.get("id") or "").strip() or None
        phone_jid = str(source.get("phoneNumber") or "").strip() or None
        lid_jid = str(source.get("lid") or "").strip() or None
        if member_jid and member_jid.endswith("@s.whatsapp.net"):
            phone_jid = phone_jid or member_jid
        if member_jid and member_jid.endswith("@lid"):
            lid_jid = lid_jid or member_jid
        member_key = phone_jid or lid_jid or member_jid
        if not member_key:
            continue
        contact = upsert_directory_contact(
            session,
            account,
            {
                "phoneJid": phone_jid,
                "primaryLidJid": lid_jid,
                "displayName": source.get("name") or "",
                "source": "group_metadata",
                "confidence": 0.9 if phone_jid and lid_jid else 0.6,
                "lastSeenAt": synced_at.isoformat(),
                "aliases": [{"lidJid": lid_jid, "source": "group_metadata"}]
                if lid_jid
                else [],
            },
            synced_at,
        )
        member = session.scalar(
            select(WhatsAppGroupMember).where(
                WhatsAppGroupMember.directory_group_id == group.id,
                WhatsAppGroupMember.member_key == member_key,
            )
        )
        if member is None:
            member = WhatsAppGroupMember(
                directory_group_id=group.id,
                member_key=member_key,
            )
        member.contact_id = contact.id if contact else None
        member.member_jid = member_jid
        member.phone_jid = phone_jid
        member.lid_jid = lid_jid
        member.display_name = str(source.get("name") or member.display_name)
        member.admin_role = str(source.get("admin") or "") or None
        member.active = True
        member.last_seen_at = synced_at
        session.add(member)
    group.name = str(result.get("groupName") or group.name)
    group.participant_count = int(result.get("size") or 0)
    group.last_synced_at = synced_at
    group.last_seen_at = synced_at
    session.add(group)
    activity(
        session,
        account,
        "group_members_synchronized",
        f"Synchronized members for {group.name}",
        details={"group_id": str(group.id), "members": len(result.get("members") or [])},
    )
    session.commit()
    return {"members": len(result.get("members") or []), "synced_at": synced_at}


@router.get("/directory/groups/{group_id}/members")
def directory_group_members(
    group_id: uuid.UUID,
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    group = session.get(WhatsAppDirectoryGroup, group_id)
    if group is None or group.account_id != account.id:
        raise HTTPException(status_code=404, detail="Detected WhatsApp group not found")
    filters: list[Any] = [
        WhatsAppGroupMember.directory_group_id == group.id,
        WhatsAppGroupMember.active.is_(True),
    ]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppGroupMember.display_name.ilike(pattern),
                WhatsAppGroupMember.phone_jid.ilike(pattern),
                WhatsAppGroupMember.lid_jid.ilike(pattern),
                WhatsAppGroupMember.member_jid.ilike(pattern),
            )
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppGroupMember).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppGroupMember)
        .where(*filters)
        .order_by(WhatsAppGroupMember.display_name, WhatsAppGroupMember.member_key)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            {
                "id": str(item.id),
                "contact_id": str(item.contact_id) if item.contact_id else None,
                "display_name": item.display_name,
                "member_jid": item.member_jid,
                "phone_jid": item.phone_jid,
                "lid_jid": item.lid_jid,
                "admin_role": item.admin_role,
                "last_seen_at": item.last_seen_at,
            }
            for item in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/directory/contacts")
def directory_contacts(
    search: str = "",
    token_status: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    filters: list[Any] = [
        WhatsAppDirectoryContact.account_id == account.id,
        WhatsAppDirectoryContact.active.is_(True),
    ]
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(
                WhatsAppDirectoryContact.display_name.ilike(pattern),
                WhatsAppDirectoryContact.phone_jid.ilike(pattern),
                WhatsAppDirectoryContact.primary_lid_jid.ilike(pattern),
            )
        )
    if token_status:
        filters.append(WhatsAppDirectoryContact.token_status == token_status)
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDirectoryContact).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppDirectoryContact)
        .where(*filters)
        .order_by(
            WhatsAppDirectoryContact.display_name,
            WhatsAppDirectoryContact.phone_jid,
        )
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    items = []
    for item in records:
        aliases = session.scalars(
            select(WhatsAppIdentityAlias)
            .where(WhatsAppIdentityAlias.contact_id == item.id)
            .order_by(WhatsAppIdentityAlias.confidence.desc())
        ).all()
        group_count = session.scalar(
            select(func.count())
            .select_from(WhatsAppGroupMember)
            .where(
                WhatsAppGroupMember.contact_id == item.id,
                WhatsAppGroupMember.active.is_(True),
            )
        ) or 0
        link_count = session.scalar(
            select(func.count())
            .select_from(WhatsAppContactLink)
            .where(
                WhatsAppContactLink.directory_contact_id == item.id,
                WhatsAppContactLink.active.is_(True),
                WhatsAppContactLink.status == "verified",
            )
        ) or 0
        items.append(
            {
                "id": str(item.id),
                "display_name": item.display_name,
                "phone_jid": item.phone_jid,
                "primary_lid_jid": item.primary_lid_jid,
                "aliases": [alias.lid_jid for alias in aliases],
                "alias_count": len(aliases),
                "group_count": group_count,
                "link_count": link_count,
                "source": item.source,
                "confidence": item.confidence,
                "token_status": item.token_status,
                "token_checked_at": item.token_checked_at,
                "token_issued_at": item.token_issued_at,
                "last_seen_at": item.last_seen_at,
            }
        )
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("/directory/contacts/{contact_id}/token/refresh")
async def refresh_contact_token(
    contact_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or contact.account_id != account.id:
        raise HTTPException(status_code=404, detail="WhatsApp contact not found")
    if not contact.primary_lid_jid:
        raise HTTPException(status_code=422, detail="This contact has no mapped LID")
    result = await gateway_request(
        get_settings().whatsapp_identity_directory_subject,
        {"action": "refresh_token", "lidJid": contact.primary_lid_jid},
        timeout=30,
    )
    token = result.get("token") or {}
    contact.token_status = str(token.get("status") or "unknown")
    contact.token_checked_at = gateway_datetime(token.get("checkedAt"))
    contact.token_issued_at = (
        gateway_datetime(token.get("issuedAt")) if token.get("issuedAt") else None
    )
    contact.last_synced_at = utcnow()
    session.add(contact)
    activity(
        session,
        account,
        "identity_token_refreshed",
        f"Refreshed privacy token status for {contact.display_name or contact.phone_jid}",
        details={"contact_id": str(contact.id), "status": contact.token_status},
    )
    session.commit()
    return {
        "id": str(contact.id),
        "token_status": contact.token_status,
        "token_checked_at": contact.token_checked_at,
        "token_issued_at": contact.token_issued_at,
    }


def report_type_dict(
    item: WhatsAppReportType, application: WhatsAppApplication
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "description": item.description,
        "artifact_kind": item.artifact_kind,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }


def recipient_scope_dict(
    item: WhatsAppRecipientScope,
    application: WhatsAppApplication,
    parent: WhatsAppRecipientScope | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "parent_id": str(item.parent_id) if item.parent_id else None,
        "parent_name": parent.name if parent else None,
        "channel": item.channel,
        "key": item.key,
        "name": item.name,
        "hierarchy_level": item.hierarchy_level,
        "description": item.description,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }


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


def audience_dict(
    session: Session,
    item: WhatsAppAudience,
    application: WhatsAppApplication,
) -> dict[str, Any]:
    group_count = session.scalar(
        select(func.count()).select_from(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == item.id,
            WhatsAppAudienceMember.target_type == "group",
            WhatsAppAudienceMember.enabled.is_(True),
        )
    ) or 0
    contact_count = session.scalar(
        select(func.count()).select_from(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == item.id,
            WhatsAppAudienceMember.target_type == "contact",
            WhatsAppAudienceMember.enabled.is_(True),
        )
    ) or 0
    profile_count = session.scalar(
        select(func.count()).select_from(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.audience_id == item.id,
            WhatsAppDispatchProfile.enabled.is_(True),
        )
    ) or 0
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "description": item.description,
        "enabled": item.enabled,
        "group_count": group_count,
        "contact_count": contact_count,
        "member_count": group_count + contact_count,
        "profile_count": profile_count,
        "updated_at": item.updated_at,
    }


@router.get("/audiences")
def audiences(
    application_id: uuid.UUID | None = None,
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters: list[Any] = []
    if application_id:
        filters.append(WhatsAppAudience.application_id == application_id)
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(
            or_(WhatsAppAudience.name.ilike(pattern), WhatsAppAudience.key.ilike(pattern))
        )
    total = session.scalar(
        select(func.count()).select_from(WhatsAppAudience).where(*filters)
    ) or 0
    records = session.execute(
        select(WhatsAppAudience, WhatsAppApplication)
        .join(WhatsAppApplication, WhatsAppApplication.id == WhatsAppAudience.application_id)
        .where(*filters)
        .order_by(WhatsAppApplication.name, WhatsAppAudience.name)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [
            audience_dict(session, item, application)
            for item, application in records
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/audiences/{audience_id}")
def audience(audience_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    ensure_defaults(session)
    item = session.get(WhatsAppAudience, audience_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    application = session.get(WhatsAppApplication, item.application_id)
    return audience_dict(session, item, application)


@router.post("/audiences", status_code=status.HTTP_201_CREATED)
def save_audience(
    data: AudienceInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    application = session.get(WhatsAppApplication, data.application_id)
    if application is None or not application.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    item = session.get(WhatsAppAudience, data.id) if data.id else None
    creating = item is None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    generated_key = data.key
    if creating:
        base_key = generated_key or re.sub(r"[^a-z0-9]+", "_", data.name.lower()).strip("_")
        base_key = (base_key or "audience")[:100]
        generated_key = base_key
        suffix = 2
        while session.scalar(
            select(WhatsAppAudience.id).where(
                WhatsAppAudience.application_id == data.application_id,
                WhatsAppAudience.key == generated_key,
            )
        ):
            ending = f"_{suffix}"
            generated_key = f"{base_key[:100 - len(ending)]}{ending}"
            suffix += 1
    if item is None:
        item = WhatsAppAudience(
            application_id=data.application_id,
            key=generated_key,
            name=data.name,
        )
    elif item.application_id != data.application_id:
        raise HTTPException(status_code=409, detail="An audience cannot move between modules")
    # Automation code may refer to this identifier, so it is immutable after create.
    if creating:
        item.key = generated_key
    item.name = data.name
    item.description = data.description
    item.enabled = data.enabled
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "audience_saved", f"Saved audience {application.name} / {item.name}")
    session.commit()
    return audience_dict(session, item, application)


@router.delete("/audiences/{audience_id}/hard")
def hard_delete_audience(
    audience_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppAudience, audience_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    name = item.name
    paths = _delete_audience_records(session, item)
    activity(session, account, "audience_deleted", f"Permanently deleted audience {name}")
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {"id": str(audience_id), "deleted": True}


def _validate_group_route(
    session: Session,
    data: AudienceMemberInput,
) -> Wing:
    if data.wing_id is None:
        raise HTTPException(status_code=422, detail="Select the wing authorized to use this group")
    wing = session.get(Wing, data.wing_id)
    if wing is None or not wing.active:
        raise HTTPException(status_code=422, detail="Select an active wing")
    try:
        area_id = uuid.UUID(data.route_scope_value)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="Select a valid hierarchy area") from exc
    area_valid = False
    if data.route_scope_key == "district":
        area = session.get(District, area_id)
        area_valid = area is not None and area.active and area.id == wing.district_id
    elif data.route_scope_key == "wing":
        area_valid = area_id == wing.id
    elif data.route_scope_key == "tehsil":
        area = session.get(Tehsil, area_id)
        area_valid = area is not None and area.active and area.district_id == wing.district_id
    elif data.route_scope_key == "markaz":
        area = session.get(Markaz, area_id)
        area_valid = area is not None and area.active and area.wing_id == wing.id
    if not area_valid:
        raise HTTPException(
            status_code=422,
            detail="The selected hierarchy area does not belong to the authorized wing",
        )
    return wing


def _authorize_directory_group(
    session: Session,
    *,
    account: WhatsAppAccount,
    group: WhatsAppDirectoryGroup,
    wing: Wing,
) -> WhatsAppGroup:
    configured = session.scalar(
        select(WhatsAppGroup).where(
            WhatsAppGroup.account_id == account.id,
            WhatsAppGroup.jid == group.jid,
        )
    )
    if configured is None:
        configured = WhatsAppGroup(
            account_id=account.id,
            directory_group_id=group.id,
            wing_id=wing.id,
            name=group.name,
            jid=group.jid,
        )
    configured.directory_group_id = group.id
    configured.wing_id = wing.id
    configured.name = group.name
    configured.jid = group.jid
    configured.enabled = True
    configured.verified_at = utcnow()
    configured.updated_at = utcnow()
    session.add(configured)
    return configured


@router.get("/audiences/{audience_id}/members")
def audience_members(
    audience_id: uuid.UUID,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    audience_item = session.get(WhatsAppAudience, audience_id)
    if audience_item is None:
        raise HTTPException(status_code=404, detail="Audience not found")
    filters = [
        WhatsAppAudienceMember.audience_id == audience_id,
        WhatsAppAudienceMember.enabled.is_(True),
    ]
    total = session.scalar(
        select(func.count()).select_from(WhatsAppAudienceMember).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppAudienceMember)
        .where(*filters)
        .order_by(WhatsAppAudienceMember.target_type, WhatsAppAudienceMember.created_at)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    items: list[dict[str, Any]] = []
    for member in records:
        if member.target_type == "group":
            target = session.get(WhatsAppDirectoryGroup, member.directory_group_id)
            configured = (
                session.scalar(
                    select(WhatsAppGroup).where(
                        WhatsAppGroup.account_id == target.account_id,
                        WhatsAppGroup.jid == target.jid,
                    )
                )
                if target
                else None
            )
            target_wing = session.get(Wing, configured.wing_id) if configured else None
            items.append({
                "id": str(member.id),
                "target_type": "group",
                "target_id": str(member.directory_group_id),
                "name": target.name if target else "Deleted group",
                "identifier": target.jid if target else member.target_key,
                "available": target.available if target else False,
                "detail": f"{target.participant_count} participants" if target else "Unavailable",
                "route_scope_key": member.route_scope_key,
                "route_scope_value": member.route_scope_value,
                "route_scope_label": member.route_scope_label,
                "wing_id": str(configured.wing_id) if configured else None,
                "wing_name": target_wing.name if target_wing else "Not authorized",
            })
        else:
            target = session.get(WhatsAppDirectoryContact, member.directory_contact_id)
            items.append({
                "id": str(member.id),
                "target_type": "contact",
                "target_id": str(member.directory_contact_id),
                "name": target.display_name or "Unnamed contact" if target else "Deleted contact",
                "identifier": (target.phone_jid or target.primary_lid_jid) if target else member.target_key,
                "available": target.active if target else False,
                "detail": target.primary_lid_jid or "No LID mapping" if target else "Unavailable",
                "route_scope_key": member.route_scope_key,
                "route_scope_value": member.route_scope_value,
                "route_scope_label": member.route_scope_label,
            })
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("/audiences/{audience_id}/members", status_code=status.HTTP_201_CREATED)
def add_audience_member(
    audience_id: uuid.UUID,
    data: AudienceMemberInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    audience_item = session.get(WhatsAppAudience, audience_id)
    if audience_item is None or not audience_item.enabled:
        raise HTTPException(status_code=404, detail="Enabled audience not found")
    group = session.get(WhatsAppDirectoryGroup, data.target_id) if data.target_type == "group" else None
    contact = session.get(WhatsAppDirectoryContact, data.target_id) if data.target_type == "contact" else None
    target = group or contact
    if target is None or target.account_id != account.id:
        raise HTTPException(status_code=422, detail="Select a target from the synchronized directory")
    if data.target_type == "group" and not group.available:
        raise HTTPException(status_code=422, detail="The detected group is not currently available")
    if data.target_type == "contact" and not contact.active:
        raise HTTPException(status_code=422, detail="The directory contact is inactive")
    if data.target_type == "group" and (
        data.route_scope_key not in {"district", "wing", "tehsil", "markaz", "other"}
        or not data.route_scope_value.strip()
        or not data.route_scope_label.strip()
    ):
        raise HTTPException(
            status_code=422,
            detail="Bind every group target to a district, wing, tehsil, markaz or custom route.",
        )
    wing = _validate_group_route(session, data) if group else None
    if group and wing:
        _authorize_directory_group(session, account=account, group=group, wing=wing)
    target_key = f"{data.target_type}:{data.target_id}"
    existing = session.scalar(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience_id,
            WhatsAppAudienceMember.target_key == target_key,
        )
    )
    if existing and existing.enabled:
        raise HTTPException(status_code=409, detail="This target is already in the audience")
    item = existing or WhatsAppAudienceMember(
        audience_id=audience_id,
        target_type=data.target_type,
        target_key=target_key,
    )
    item.target_type = data.target_type
    item.directory_group_id = group.id if group else None
    item.directory_contact_id = contact.id if contact else None
    item.route_scope_key = data.route_scope_key if group else ""
    item.route_scope_value = data.route_scope_value.strip() if group else ""
    item.route_scope_label = data.route_scope_label.strip() if group else ""
    item.enabled = True
    session.add(item)
    activity(session, account, "audience_member_added", f"Added {data.target_type} to {audience_item.name}")
    session.commit()
    return {
        "id": str(item.id),
        "target_type": item.target_type,
        "target_id": str(data.target_id),
        "route_scope_key": item.route_scope_key,
        "route_scope_value": item.route_scope_value,
        "route_scope_label": item.route_scope_label,
        "wing_id": str(wing.id) if wing else None,
    }


@router.put("/audiences/{audience_id}/members/{member_id}")
def update_audience_member(
    audience_id: uuid.UUID,
    member_id: uuid.UUID,
    data: AudienceMemberInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppAudienceMember, member_id)
    if item is None or item.audience_id != audience_id or not item.enabled:
        raise HTTPException(status_code=404, detail="Audience target not found")
    if data.target_id != (item.directory_group_id or item.directory_contact_id):
        raise HTTPException(status_code=409, detail="Remove and re-add a target to change its identity")
    if item.target_type == "group" and (
        data.route_scope_key not in {"district", "wing", "tehsil", "markaz", "other"}
        or not data.route_scope_value.strip()
        or not data.route_scope_label.strip()
    ):
        raise HTTPException(status_code=422, detail="Select the hierarchy route represented by this group")
    group = session.get(WhatsAppDirectoryGroup, item.directory_group_id) if item.target_type == "group" else None
    wing = _validate_group_route(session, data) if group else None
    if group and wing:
        _authorize_directory_group(session, account=account, group=group, wing=wing)
    item.route_scope_key = data.route_scope_key if item.target_type == "group" else ""
    item.route_scope_value = data.route_scope_value.strip() if item.target_type == "group" else ""
    item.route_scope_label = data.route_scope_label.strip() if item.target_type == "group" else ""
    session.add(item)
    activity(
        session,
        account,
        "audience_member_route_updated",
        f"Updated audience route binding to {item.route_scope_key} / {item.route_scope_label}",
    )
    session.commit()
    return {
        "id": str(item.id),
        "route_scope_key": item.route_scope_key,
        "route_scope_value": item.route_scope_value,
        "route_scope_label": item.route_scope_label,
        "wing_id": str(wing.id) if wing else None,
    }


@router.delete("/audiences/{audience_id}/members/{member_id}")
def remove_audience_member(
    audience_id: uuid.UUID,
    member_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppAudienceMember, member_id)
    if item is None or item.audience_id != audience_id:
        raise HTTPException(status_code=404, detail="Audience member not found")
    session.delete(item)
    activity(session, account, "audience_member_deleted", "Permanently deleted a target from an audience")
    session.commit()
    return {"id": str(member_id), "deleted": True}


def dispatch_profile_dict(
    item: WhatsAppDispatchProfile,
    application: WhatsAppApplication,
    report_type: WhatsAppReportType,
    audience_item: WhatsAppAudience,
    account: WhatsAppAccount,
    wing: Wing | None,
    recipient_scope: WhatsAppRecipientScope | None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id),
        "application_key": application.key,
        "application_name": application.name,
        "key": item.key,
        "name": item.name,
        "report_type_id": str(item.report_type_id),
        "report_type_name": report_type.name,
        "audience_id": str(item.audience_id),
        "audience_name": audience_item.name,
        "account_id": str(item.account_id),
        "account_name": account.name,
        "template_id": str(item.template_id) if item.template_id else None,
        "recipient_channel": item.recipient_channel,
        "recipient_scope_id": str(item.recipient_scope_id) if item.recipient_scope_id else None,
        "recipient_scope_name": recipient_scope.name if recipient_scope else "Legacy / unscoped",
        "wing_id": str(item.wing_id) if item.wing_id else None,
        "wing_name": wing.name if wing else None,
        "delivery_mode": item.delivery_mode,
        "require_approval": item.require_approval,
        "fallback_policy": item.fallback_policy,
        "max_retries": item.max_retries,
        "messages_per_minute": item.messages_per_minute,
        "presentation_policy": item.presentation_policy or {},
        "guided_setup": item.guided_setup,
        "enabled": item.enabled,
        "version": item.version,
        "notes": item.notes,
        "updated_at": item.updated_at,
    }


@router.get("/dispatch-profiles")
def dispatch_profiles(
    application_id: uuid.UUID | None = None,
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters: list[Any] = []
    if application_id:
        filters.append(WhatsAppDispatchProfile.application_id == application_id)
    if search:
        pattern = f"%{search.strip()}%"
        filters.append(or_(WhatsAppDispatchProfile.name.ilike(pattern), WhatsAppDispatchProfile.key.ilike(pattern)))
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDispatchProfile).where(*filters)
    ) or 0
    profiles = session.scalars(
        select(WhatsAppDispatchProfile)
        .where(*filters)
        .order_by(WhatsAppDispatchProfile.name)
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    items = []
    for item in profiles:
        application = session.get(WhatsAppApplication, item.application_id)
        report_type = session.get(WhatsAppReportType, item.report_type_id)
        audience_item = session.get(WhatsAppAudience, item.audience_id)
        account = session.get(WhatsAppAccount, item.account_id)
        wing = session.get(Wing, item.wing_id) if item.wing_id else None
        recipient_scope = (
            session.get(WhatsAppRecipientScope, item.recipient_scope_id)
            if item.recipient_scope_id
            else None
        )
        items.append(dispatch_profile_dict(item, application, report_type, audience_item, account, wing, recipient_scope))
    return {"items": items, "total": total, "page": page, "page_size": page_size}


def validate_dispatch_profile(
    session: Session,
    data: DispatchProfileInput,
) -> tuple[WhatsAppApplication, WhatsAppReportType, WhatsAppAudience, WhatsAppAccount, Wing | None, WhatsAppRecipientScope | None]:
    application = session.get(WhatsAppApplication, data.application_id)
    report_type = session.get(WhatsAppReportType, data.report_type_id)
    audience_item = session.get(WhatsAppAudience, data.audience_id)
    account = session.get(WhatsAppAccount, data.account_id)
    wing = session.get(Wing, data.wing_id) if data.wing_id else None
    recipient_scope = (
        session.get(WhatsAppRecipientScope, data.recipient_scope_id)
        if data.recipient_scope_id
        else None
    )
    if application is None or not application.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled platform module")
    if report_type is None or report_type.application_id != application.id or not report_type.enabled:
        raise HTTPException(status_code=422, detail="Report type does not belong to the selected module")
    if audience_item is None or audience_item.application_id != application.id or not audience_item.enabled:
        raise HTTPException(status_code=422, detail="Audience does not belong to the selected module")
    if account is None or not account.enabled:
        raise HTTPException(status_code=422, detail="Select an enabled WhatsApp connection")
    if data.enabled and recipient_scope is None:
        raise HTTPException(status_code=422, detail="Select an individual role or group hierarchy scope")
    if recipient_scope and (
        recipient_scope.application_id != application.id
        or recipient_scope.channel != data.recipient_channel
        or not recipient_scope.enabled
    ):
        raise HTTPException(status_code=422, detail="Recipient scope does not match the selected module and channel")
    if data.enabled and not data.template_id:
        raise HTTPException(status_code=422, detail="Enabled dispatch profiles require a report template")
    if data.template_id:
        template = session.get(WhatsAppTemplate, data.template_id)
        if template is None or not template.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled message template")
        if template.application_id != application.id:
            raise HTTPException(status_code=422, detail="Template belongs to another module")
        if template.report_type_id != report_type.id:
            raise HTTPException(status_code=422, detail="Template belongs to another report type")
        if template.category != "report":
            raise HTTPException(status_code=422, detail="Select a report template, not a system template")
        if template.recipient_channel not in {"any", data.recipient_channel}:
            raise HTTPException(status_code=422, detail="Template belongs to another recipient channel")
        if template.recipient_scope_id and template.recipient_scope_id != data.recipient_scope_id:
            raise HTTPException(status_code=422, detail="Template belongs to another recipient role or scope")
    members = session.scalars(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience_item.id,
            WhatsAppAudienceMember.enabled.is_(True),
        )
    ).all()
    group_members = [member for member in members if member.target_type == "group"]
    contact_members = [member for member in members if member.target_type == "contact"]
    if data.enabled and not members:
        raise HTTPException(status_code=422, detail="Add at least one target before enabling this profile")
    if data.recipient_channel == "group" and (not group_members or contact_members):
        raise HTTPException(status_code=422, detail="Group profiles require a group-only audience")
    if data.recipient_channel == "individual" and (not contact_members or group_members):
        raise HTTPException(status_code=422, detail="Individual profiles require a contact-only audience")
    if data.recipient_channel == "group" and recipient_scope:
        unbound = [
            member
            for member in group_members
            if member.route_scope_key != recipient_scope.key
            or not member.route_scope_value
            or not member.route_scope_label
        ]
        if unbound:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Every audience group must be bound to a {recipient_scope.name} route "
                    "before this profile can be enabled."
                ),
            )
    for member in group_members:
        target = session.get(WhatsAppDirectoryGroup, member.directory_group_id)
        if target is None or target.account_id != account.id:
            raise HTTPException(status_code=422, detail="Audience contains a group from another WhatsApp connection")
    for member in contact_members:
        target = session.get(WhatsAppDirectoryContact, member.directory_contact_id)
        if target is None or target.account_id != account.id:
            raise HTTPException(status_code=422, detail="Audience contains a contact from another WhatsApp connection")
    if application.key == "antidengue" and (wing is None or not wing.active):
        raise HTTPException(status_code=422, detail="AntiDengue profiles require an active wing scope")
    if data.fallback_policy == "same_scope" and application.key == "antidengue" and not wing:
        raise HTTPException(status_code=422, detail="Same-scope fallback requires a wing scope")
    if application.key == "antidengue" and wing and group_members:
        group_ids = [member.directory_group_id for member in group_members]
        conflicts = session.execute(
            select(WhatsAppDispatchProfile, WhatsAppAudienceMember)
            .join(
                WhatsAppAudienceMember,
                WhatsAppAudienceMember.audience_id == WhatsAppDispatchProfile.audience_id,
            )
            .where(
                WhatsAppDispatchProfile.application_id == application.id,
                WhatsAppDispatchProfile.enabled.is_(True),
                WhatsAppDispatchProfile.wing_id.is_not(None),
                WhatsAppDispatchProfile.wing_id != wing.id,
                WhatsAppDispatchProfile.id != data.id if data.id else True,
                WhatsAppAudienceMember.directory_group_id.in_(group_ids),
                WhatsAppAudienceMember.enabled.is_(True),
            )
        ).first()
        if conflicts:
            raise HTTPException(status_code=409, detail="A group in this audience is already authorized for another AntiDengue wing")
    return application, report_type, audience_item, account, wing, recipient_scope


@router.post("/dispatch-profiles", status_code=status.HTTP_201_CREATED)
def save_dispatch_profile(
    data: DispatchProfileInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    activity_account, _ = ensure_defaults(session)
    data.delivery_mode = "groups" if data.recipient_channel == "group" else "individuals"
    application, report_type, audience_item, account, wing, recipient_scope = validate_dispatch_profile(session, data)
    item = session.get(WhatsAppDispatchProfile, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="Dispatch profile not found")
    duplicate = session.scalar(
        select(WhatsAppDispatchProfile).where(
            WhatsAppDispatchProfile.application_id == data.application_id,
            WhatsAppDispatchProfile.key == data.key,
            WhatsAppDispatchProfile.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This profile key already exists in the module")
    if item is None:
        item = WhatsAppDispatchProfile(
            application_id=data.application_id,
            key=data.key,
            name=data.name,
            report_type_id=data.report_type_id,
            audience_id=data.audience_id,
            account_id=data.account_id,
        )
    elif item.application_id != data.application_id:
        raise HTTPException(status_code=409, detail="A profile cannot move between modules")
    else:
        item.version += 1
    values = data.model_dump(exclude={"id", "message_style", "attachment_mode", "image_content"})
    values["presentation_policy"] = {
        "message_style": data.message_style,
        "attachment_mode": data.attachment_mode,
        "image_content": data.image_content,
    }
    for key, value in values.items():
        setattr(item, key, value)
    item.updated_at = utcnow()
    session.add(item)
    activity(
        session,
        activity_account,
        "dispatch_profile_saved",
        f"Saved dispatch profile {application.name} / {item.name} version {item.version}",
        details={"profile_id": str(item.id), "version": item.version},
    )
    session.commit()
    return dispatch_profile_dict(item, application, report_type, audience_item, account, wing, recipient_scope)


@router.post("/reporting-routes/setup", status_code=status.HTTP_201_CREATED)
def setup_reporting_route(
    data: ReportingRouteSetupInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    """Create a complete route, or add a group to one, without partial state."""
    activity_account, _ = ensure_defaults(session)
    existing_profile = session.get(WhatsAppDispatchProfile, data.profile_id) if data.profile_id else None
    if data.profile_id and (existing_profile is None or not existing_profile.enabled):
        raise HTTPException(status_code=404, detail="Select an enabled reporting setup")

    if existing_profile:
        application = session.get(WhatsAppApplication, existing_profile.application_id)
        report_type = session.get(WhatsAppReportType, existing_profile.report_type_id)
        audience_item = session.get(WhatsAppAudience, existing_profile.audience_id)
        account = session.get(WhatsAppAccount, existing_profile.account_id)
        wing = session.get(Wing, existing_profile.wing_id) if existing_profile.wing_id else None
        recipient_scope = session.get(WhatsAppRecipientScope, existing_profile.recipient_scope_id) if existing_profile.recipient_scope_id else None
        template = session.get(WhatsAppTemplate, existing_profile.template_id) if existing_profile.template_id else None
        if not all((application, report_type, audience_item, account, wing, recipient_scope, template)):
            raise HTTPException(status_code=409, detail="The selected reporting setup has incomplete dependencies")
        profile = existing_profile
        created = {"audience": False, "template": False, "profile": False}
    else:
        application = session.get(WhatsAppApplication, data.application_id) if data.application_id else None
        report_type = session.get(WhatsAppReportType, data.report_type_id) if data.report_type_id else None
        account = session.get(WhatsAppAccount, data.account_id) if data.account_id else None
        wing = session.get(Wing, data.wing_id) if data.wing_id else None
        recipient_scope = session.get(WhatsAppRecipientScope, data.recipient_scope_id) if data.recipient_scope_id else None
        if application is None or not application.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled platform module")
        if report_type is None or report_type.application_id != application.id or not report_type.enabled:
            raise HTTPException(status_code=422, detail="Select a report type from this module")
        if account is None or not account.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled WhatsApp connection")
        if wing is None or not wing.active:
            raise HTTPException(status_code=422, detail="Select an active AntiDengue wing")
        if recipient_scope is None or recipient_scope.application_id != application.id or recipient_scope.channel != "group" or not recipient_scope.enabled:
            raise HTTPException(status_code=422, detail="Select an enabled group hierarchy scope")

        def available_key(model: Any, application_id: uuid.UUID | None, name: str) -> str:
            base = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")[:90] or "reporting_route"
            candidate = base
            suffix = 2
            filters = [model.key == candidate]
            if hasattr(model, "application_id") and application_id:
                filters.append(model.application_id == application_id)
            while session.scalar(select(model.id).where(*filters)):
                candidate = f"{base[:90]}_{suffix}"
                filters = [model.key == candidate]
                if hasattr(model, "application_id") and application_id:
                    filters.append(model.application_id == application_id)
                suffix += 1
            return candidate

        wing_label = re.sub(r"^DEO[\s_-]+", "", wing.code or wing.name, flags=re.IGNORECASE)
        audience_name = data.audience_name.strip() or f"{application.name} – {wing_label} – {recipient_scope.name}s"
        audience_item = WhatsAppAudience(
            application_id=application.id,
            key=available_key(WhatsAppAudience, application.id, audience_name),
            name=audience_name,
            description="Created by the guided reporting-route setup.",
        )
        session.add(audience_item)
        session.flush()

        template = session.get(WhatsAppTemplate, data.template_id) if data.template_id else None
        if template and (
            not template.enabled
            or template.application_id != application.id
            or template.report_type_id != report_type.id
            or template.recipient_channel not in {"any", "group"}
            or (template.recipient_scope_id and template.recipient_scope_id != recipient_scope.id)
        ):
            raise HTTPException(status_code=422, detail="The selected template is incompatible with this reporting route")
        template_created = False
        if template is None:
            if not data.create_recommended_template:
                raise HTTPException(status_code=422, detail="Select a compatible template or create the recommended template")
            template_name = data.template_name.strip() or f"{report_type.name} – {recipient_scope.name}"
            template_body = data.template_body
            if report_type.key != "tehsil_dormant_summary" and template_body.strip() == "{{report_body}}":
                template_body = "{{message}}"
            template = WhatsAppTemplate(
                application_id=application.id,
                report_type_id=report_type.id,
                recipient_scope_id=recipient_scope.id,
                recipient_channel="group",
                key=available_key(WhatsAppTemplate, None, template_name),
                name=template_name,
                category="report",
                body=template_body,
            )
            session.add(template)
            session.flush()
            template_created = True

        profile_name = data.profile_name.strip() or f"{report_type.name} – {wing_label} – {recipient_scope.name}"
        profile = WhatsAppDispatchProfile(
            application_id=application.id,
            key=available_key(WhatsAppDispatchProfile, application.id, profile_name),
            name=profile_name,
            report_type_id=report_type.id,
            audience_id=audience_item.id,
            account_id=account.id,
            template_id=template.id,
            recipient_scope_id=recipient_scope.id,
            recipient_channel="group",
            wing_id=wing.id,
            delivery_mode="groups",
            require_approval=data.require_approval,
            max_retries=data.max_retries,
            messages_per_minute=data.messages_per_minute,
            presentation_policy={
                "message_style": data.message_style,
                "attachment_mode": data.attachment_mode,
                "image_content": data.image_content,
            },
            guided_setup=True,
            owns_audience=True,
            owns_template=template_created,
            notes="Created by the guided reporting-route setup.",
        )
        session.add(profile)
        session.flush()
        created = {"audience": True, "template": template_created, "profile": True}

    if application.key != "antidengue":
        raise HTTPException(status_code=422, detail="The first guided workflow currently supports AntiDengue group reporting")
    if recipient_scope.channel != "group":
        raise HTTPException(status_code=422, detail="This guided workflow requires a group recipient scope")

    group = session.get(WhatsAppDirectoryGroup, data.directory_group_id)
    if group is None or group.account_id != account.id or not group.available:
        raise HTTPException(status_code=422, detail="Select an available group from this WhatsApp connection")
    route_data = AudienceMemberInput(
        target_type="group",
        target_id=group.id,
        wing_id=wing.id,
        route_scope_key=recipient_scope.key,
        route_scope_value=data.route_scope_value,
        route_scope_label=data.route_scope_label,
    )
    _validate_group_route(session, route_data)
    configured = session.scalar(
        select(WhatsAppGroup).where(
            WhatsAppGroup.account_id == account.id,
            or_(WhatsAppGroup.directory_group_id == group.id, WhatsAppGroup.jid == group.jid),
        )
    )
    if configured and configured.wing_id != wing.id:
        configured_wing = session.get(Wing, configured.wing_id)
        raise HTTPException(
            status_code=409,
            detail=f"{group.name} is already authorized for {configured_wing.name if configured_wing else 'another wing'}",
        )
    _authorize_directory_group(session, account=account, group=group, wing=wing)
    target_key = f"group:{group.id}"
    member = session.scalar(
        select(WhatsAppAudienceMember).where(
            WhatsAppAudienceMember.audience_id == audience_item.id,
            WhatsAppAudienceMember.target_key == target_key,
        )
    )
    if member and member.enabled:
        raise HTTPException(status_code=409, detail=f"{group.name} is already part of this reporting setup")
    member = member or WhatsAppAudienceMember(
        audience_id=audience_item.id,
        target_type="group",
        target_key=target_key,
        directory_group_id=group.id,
    )
    member.enabled = True
    member.route_scope_key = recipient_scope.key
    member.route_scope_value = data.route_scope_value
    member.route_scope_label = data.route_scope_label
    session.add(member)
    session.flush()

    validate_dispatch_profile(
        session,
        DispatchProfileInput(
            id=profile.id,
            application_id=application.id,
            key=profile.key,
            name=profile.name,
            report_type_id=report_type.id,
            audience_id=audience_item.id,
            account_id=account.id,
            template_id=template.id,
            recipient_scope_id=recipient_scope.id,
            recipient_channel="group",
            wing_id=wing.id,
            delivery_mode="groups",
            require_approval=profile.require_approval,
            fallback_policy=profile.fallback_policy,
            max_retries=profile.max_retries,
            messages_per_minute=profile.messages_per_minute,
            message_style=(profile.presentation_policy or {}).get("message_style", "summary"),
            attachment_mode=(profile.presentation_policy or {}).get("attachment_mode", "none"),
            image_content=(profile.presentation_policy or {}).get("image_content", "details"),
            enabled=True,
            notes=profile.notes,
        ),
    )
    activity(
        session,
        activity_account,
        "reporting_route_setup",
        f"Configured {report_type.name} for {group.name} / {data.route_scope_label}",
        details={"profile_id": str(profile.id), "audience_id": str(audience_item.id), "group_id": str(group.id)},
    )
    session.commit()
    return {
        "profile": dispatch_profile_dict(profile, application, report_type, audience_item, account, wing, recipient_scope),
        "audience_id": str(audience_item.id),
        "member_id": str(member.id),
        "group_name": group.name,
        "route": {"scope": recipient_scope.name, "area": data.route_scope_label, "wing": wing.name},
        "created": created,
    }


@router.delete("/dispatch-profiles/{profile_id}")
def archive_dispatch_profile(
    profile_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppDispatchProfile, profile_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Dispatch profile not found")
    item.enabled = False
    item.version += 1
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "dispatch_profile_archived", f"Archived dispatch profile {item.name}")
    session.commit()
    return {"id": str(item.id), "enabled": False, "version": item.version}


@router.delete("/dispatch-profiles/{profile_id}/hard")
def hard_delete_dispatch_profile(
    profile_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppDispatchProfile, profile_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Dispatch profile not found")
    name = item.name
    guided_setup = item.guided_setup
    if guided_setup:
        paths, audience_deleted, template_deleted = _delete_guided_setup_records(session, item)
        activity(session, account, "reporting_setup_deleted", f"Permanently deleted reporting setup {name}")
    else:
        paths = _delete_profile_records(session, item)
        audience_deleted = False
        template_deleted = False
        activity(session, account, "dispatch_profile_deleted", f"Permanently deleted dispatch profile {name}")
    session.commit()
    cleanup_unreferenced_preview_files(session, paths)
    return {
        "id": str(profile_id),
        "deleted": True,
        "guided_setup": guided_setup,
        "audience_deleted": audience_deleted,
        "template_deleted": template_deleted,
    }


def normalize_target(value: str) -> str:
    target = value.strip()
    if target.endswith("@g.us") or target.endswith("@s.whatsapp.net"):
        return target
    digits = re.sub(r"\D", "", target)
    if digits.startswith("03"):
        digits = "92" + digits[1:]
    elif digits.startswith("3") and len(digits) == 10:
        digits = "92" + digits
    if not re.fullmatch(r"923\d{9}", digits):
        raise HTTPException(status_code=422, detail="Enter a valid Pakistani WhatsApp number")
    return f"{digits}@s.whatsapp.net"


@router.post("/test-message", status_code=status.HTTP_202_ACCEPTED)
async def send_test_message(
    data: TestMessageInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account, gateway_settings = ensure_defaults(session)
    if not gateway_settings.live_delivery_enabled:
        raise HTTPException(
            status_code=409,
            detail="Live delivery is disabled in WhatsApp Settings",
        )
    health = await refresh_health(session, account)
    if not health.get("ready"):
        raise HTTPException(status_code=503, detail="WhatsApp gateway is not ready")

    target = normalize_target(data.target)
    delivery = WhatsAppDelivery(
        account_id=account.id,
        recipient_type="test",
        recipient_name=data.recipient_name,
        target=target,
        message=data.message,
        status_subject=f"whatsapp.status.platform.{uuid.uuid4().hex}",
    )
    session.add(delivery)
    session.flush()
    activity(
        session,
        account,
        "test_message_queued",
        f"Queued a test message for {data.recipient_name}",
        details={"delivery_id": str(delivery.id), "target": target},
    )
    session.commit()

    client = None
    try:
        config = get_settings()
        client = await nats.connect(config.whatsapp_nats_url, connect_timeout=2)
        subscription = await client.subscribe(delivery.status_subject)
        await client.flush()
        payload = {
            "job_id": str(delivery.id),
            "target": target,
            "type": "group" if target.endswith("@g.us") else "contact",
            "recipient_name": data.recipient_name,
            "text": data.message,
            "delay_ms": gateway_settings.send_delay_ms,
            "status_subject": delivery.status_subject,
        }
        acknowledgement = await client.jetstream().publish(
            account.command_subject,
            json.dumps(payload).encode("utf-8"),
            headers={"Nats-Msg-Id": str(delivery.id)},
        )
        delivery.queue_stream = acknowledgement.stream
        delivery.queue_sequence = acknowledgement.seq
        session.add(delivery)
        session.commit()

        try:
            message = await subscription.next_msg(
                timeout=gateway_settings.acknowledgement_timeout_seconds
            )
            result = json.loads(message.data.decode("utf-8"))
            delivery.status = str(result.get("status") or "failed")
            delivery.error = result.get("error")
            delivery.provider_result = result
            delivery.completed_at = utcnow()
            activity(
                session,
                account,
                "test_message_completed",
                f"Test message {delivery.status} for {data.recipient_name}",
                level="error" if delivery.status == "failed" else "info",
                details={"delivery_id": str(delivery.id), "status": delivery.status},
            )
        except NatsTimeoutError:
            delivery.status = "timed_out"
            delivery.error = "Gateway acknowledgement timed out"
            delivery.completed_at = utcnow()
            activity(
                session,
                account,
                "test_message_timeout",
                f"Test message acknowledgement timed out for {data.recipient_name}",
                level="warning",
                details={"delivery_id": str(delivery.id)},
            )
        session.add(delivery)
        session.commit()
    except Exception as exc:
        delivery.status = "failed"
        delivery.error = str(exc)
        delivery.completed_at = utcnow()
        activity(
            session,
            account,
            "test_message_failed",
            f"Could not queue test message: {exc}",
            level="error",
            details={"delivery_id": str(delivery.id)},
        )
        session.add(delivery)
        session.commit()
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    finally:
        if client is not None:
            await client.close()
    return delivery_dict(delivery)


def delivery_dict(item: WhatsAppDelivery) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "recipient_type": item.recipient_type,
        "recipient_name": item.recipient_name,
        "target": item.target,
        "message": item.message,
        "status": item.status,
        "error": item.error,
        "queue_stream": item.queue_stream,
        "queue_sequence": item.queue_sequence,
        "queued_at": item.queued_at,
        "completed_at": item.completed_at,
    }


@router.get("/deliveries")
def deliveries(
    status_filter: str = Query(default="", alias="status"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    ensure_defaults(session)
    filters = [WhatsAppDelivery.status == status_filter] if status_filter else []
    total = session.scalar(
        select(func.count()).select_from(WhatsAppDelivery).where(*filters)
    ) or 0
    records = session.scalars(
        select(WhatsAppDelivery)
        .where(*filters)
        .order_by(WhatsAppDelivery.queued_at.desc())
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()
    return {
        "items": [delivery_dict(item) for item in records],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/recipients")
def recipients(
    search: str = "",
    recipient_type: Literal["all", "officer", "school_head"] = "all",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    if recipient_type in {"all", "officer"}:
        for officer in session.scalars(
            select(Officer).where(Officer.active.is_(True), Officer.mobile != "")
        ):
            wing = session.get(Wing, officer.wing_id)
            records.append(
                {
                    "id": str(officer.id),
                    "type": "officer",
                    "role": officer.role.upper(),
                    "name": officer.name,
                    "mobile": officer.mobile,
                    "wing": wing.name if wing else "",
                }
            )
    if recipient_type in {"all", "school_head"}:
        head_rows = session.execute(
            select(SchoolHead, School, Wing)
            .join(School, School.id == SchoolHead.school_id)
            .join(Wing, Wing.id == School.wing_id)
            .where(
                SchoolHead.active.is_(True),
                School.active.is_(True),
                SchoolHead.mobile != "",
            )
        ).all()
        for head, school, wing in head_rows:
            records.append(
                {
                    "id": str(head.id),
                    "type": "school_head",
                    "role": "School head",
                    "name": head.name,
                    "mobile": head.mobile,
                    "wing": wing.name,
                    "school": school.name,
                }
            )
    if search:
        needle = search.casefold()
        records = [
            item
            for item in records
            if needle in " ".join(str(value) for value in item.values()).casefold()
        ]
    records.sort(key=lambda item: (item["name"].casefold(), item["mobile"]))
    total = len(records)
    start = (page - 1) * page_size
    return {
        "items": records[start : start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


def group_dict(
    item: WhatsAppGroup,
    wing_name: str,
    directory_group: WhatsAppDirectoryGroup | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "directory_group_id": (
            str(item.directory_group_id) if item.directory_group_id else None
        ),
        "wing_id": str(item.wing_id),
        "wing_name": wing_name,
        "name": item.name,
        "jid": item.jid,
        "purpose": item.purpose,
        "enabled": item.enabled,
        "verified_at": item.verified_at,
        "notes": item.notes,
        "participant_count": directory_group.participant_count if directory_group else None,
        "last_detected_at": directory_group.last_seen_at if directory_group else None,
    }


@router.get("/groups")
def groups(session: Session = Depends(get_session)) -> list[dict[str, Any]]:
    ensure_defaults(session)
    records = session.execute(
        select(WhatsAppGroup, Wing)
        .join(Wing, Wing.id == WhatsAppGroup.wing_id)
        .order_by(Wing.name, WhatsAppGroup.name)
    ).all()
    return [
        group_dict(item, wing.name, session.get(WhatsAppDirectoryGroup, item.directory_group_id))
        for item, wing in records
    ]


@router.post("/groups", status_code=status.HTTP_201_CREATED)
def save_group(data: GroupInput, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    wing = session.get(Wing, data.wing_id)
    if wing is None or not wing.active:
        raise HTTPException(status_code=422, detail="Select a valid wing")
    directory_group = (
        session.get(WhatsAppDirectoryGroup, data.directory_group_id)
        if data.directory_group_id
        else None
    )
    if data.directory_group_id and (
        directory_group is None
        or directory_group.account_id != account.id
        or not directory_group.available
    ):
        raise HTTPException(status_code=422, detail="Select an available detected group")
    resolved_name = directory_group.name if directory_group else data.name.strip()
    resolved_jid = directory_group.jid if directory_group else data.jid.strip()
    if not resolved_jid.endswith("@g.us"):
        raise HTTPException(status_code=422, detail="Group JID must end with @g.us")
    duplicate = session.scalar(
        select(WhatsAppGroup).where(
            WhatsAppGroup.account_id == account.id,
            WhatsAppGroup.jid == resolved_jid,
            WhatsAppGroup.id != data.id if data.id else True,
        )
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="This WhatsApp group already exists")
    item = session.get(WhatsAppGroup, data.id) if data.id else None
    if data.id and item is None:
        raise HTTPException(status_code=404, detail="WhatsApp group not found")
    if item is None:
        item = WhatsAppGroup(
            account_id=account.id,
            wing_id=data.wing_id,
            name=resolved_name,
            jid=resolved_jid,
        )
    item.directory_group_id = directory_group.id if directory_group else None
    item.wing_id = data.wing_id
    item.name = resolved_name
    item.jid = resolved_jid
    item.purpose = data.purpose
    item.notes = data.notes
    item.enabled = data.enabled
    item.verified_at = utcnow() if directory_group else item.verified_at
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "group_saved", f"Saved WhatsApp group {item.name}")
    session.commit()
    return group_dict(item, wing.name, directory_group)


@router.delete("/groups/{group_id}")
def archive_group(group_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppGroup, group_id)
    if item is None:
        raise HTTPException(status_code=404, detail="WhatsApp group not found")
    item.enabled = False
    item.updated_at = utcnow()
    session.add(item)
    activity(session, account, "group_archived", f"Archived WhatsApp group {item.name}")
    session.commit()
    wing = session.get(Wing, item.wing_id)
    directory_group = (
        session.get(WhatsAppDirectoryGroup, item.directory_group_id)
        if item.directory_group_id
        else None
    )
    return group_dict(item, wing.name if wing else "", directory_group)


@router.delete("/groups/{group_id}/hard")
def hard_delete_group(group_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    account, _ = ensure_defaults(session)
    item = session.get(WhatsAppGroup, group_id)
    if item is None:
        raise HTTPException(status_code=404, detail="WhatsApp group not found")
    name = item.name
    session.delete(item)
    activity(session, account, "group_deleted", f"Permanently deleted WhatsApp group {name}")
    session.commit()
    return {"id": str(group_id), "deleted": True}


def template_dict(
    item: WhatsAppTemplate,
    application_name: str | None = None,
    report_type_name: str | None = None,
    recipient_scope_name: str | None = None,
) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "application_id": str(item.application_id) if item.application_id else None,
        "report_type_id": str(item.report_type_id) if item.report_type_id else None,
        "application_name": application_name or "Shared",
        "report_type_name": report_type_name or "All report types",
        "recipient_channel": item.recipient_channel,
        "recipient_scope_id": str(item.recipient_scope_id) if item.recipient_scope_id else None,
        "recipient_scope_name": recipient_scope_name or "All roles / scopes",
        "key": item.key,
        "name": item.name,
        "category": item.category,
        "body": item.body,
        "enabled": item.enabled,
        "updated_at": item.updated_at,
    }


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
