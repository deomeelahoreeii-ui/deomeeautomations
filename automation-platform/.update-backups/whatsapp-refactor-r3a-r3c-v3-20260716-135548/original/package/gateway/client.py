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
