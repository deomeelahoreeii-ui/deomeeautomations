from __future__ import annotations

from whatsapp_gateway.api_imports import *

from whatsapp_gateway.api_schemas import *

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
