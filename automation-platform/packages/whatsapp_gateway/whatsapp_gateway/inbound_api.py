from __future__ import annotations

import hashlib
import json
import hmac
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import nats
from nats.errors import NoRespondersError, TimeoutError as NatsTimeoutError
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.job_service import create_job, set_task_id
from automation_core.models import Job, JobType
from automation_core.time import utcnow
from whatsapp_gateway.inbound_media import detect_file_type
from whatsapp_gateway.inbound_service import (
    build_preview,
    contact_message_filter,
    create_export_run,
    serialize_run,
)
from whatsapp_gateway.inbound_tasks import build_inbound_export_job
from whatsapp_gateway.models import (
    WhatsAppAccount,
    WhatsAppDirectoryContact,
    WhatsAppIdentityAlias,
    WhatsAppInboundAttachment,
    WhatsAppInboundExportRun,
    WhatsAppInboundHistoryRequest,
    WhatsAppInboundMessage,
)

router = APIRouter(prefix="/api/v1/whatsapp/inbound", tags=["whatsapp-inbound"])


class AttachmentEvent(BaseModel):
    mediaKind: str
    messageKey: str
    originalFilename: str | None = None
    mimeType: str | None = None
    declaredSize: int | None = None
    mediaSha256: str | None = None
    caption: str | None = None


class InboundMessageEvent(BaseModel):
    workerId: str
    messageId: str
    remoteJid: str
    participantJid: str | None = None
    senderJid: str
    fromMe: bool = False
    chatScope: str
    messageTimestamp: datetime
    pushName: str | None = None
    text: str | None = None
    messageType: str
    ingestionSource: str
    payloadSha256: str
    rawPayload: dict[str, Any] = Field(default_factory=dict)
    attachment: AttachmentEvent | None = None


class InboundFileFilter(BaseModel):
    contact_id: uuid.UUID
    date_from: datetime | None = None
    date_to: datetime | None = None
    chat_scope: str = Field(default="direct", pattern="^(direct|direct_and_groups)$")
    media_types: list[str] = Field(default_factory=lambda: ["image", "pdf", "spreadsheet"])


class CreateInboundExportRequest(InboundFileFilter):
    requested_by: str = Field(default="web-operator", max_length=100)


class RequestInboundHistory(BaseModel):
    contact_id: uuid.UUID
    count: int = Field(default=50, ge=1, le=200)


def _verify_worker_token(
    x_whatsapp_worker_token: str | None = Header(default=None),
    settings: Settings = Depends(get_settings),
) -> None:
    expected = settings.whatsapp_inbound_ingest_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="WHATSAPP_INBOUND_INGEST_TOKEN is not configured",
        )
    if not x_whatsapp_worker_token or not hmac.compare_digest(
        x_whatsapp_worker_token, expected
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid worker token",
        )


def _resolve_account(session: Session, worker_key: str) -> WhatsAppAccount:
    account = session.exec(
        select(WhatsAppAccount).where(WhatsAppAccount.worker_key == worker_key)
    ).first()
    if account is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Unknown WhatsApp worker account: {worker_key}",
        )
    return account


def _resolve_contact_id(session: Session, account_id, sender_jid: str):
    contact = session.exec(
        select(WhatsAppDirectoryContact).where(
            WhatsAppDirectoryContact.account_id == account_id,
            or_(
                WhatsAppDirectoryContact.phone_jid == sender_jid,
                WhatsAppDirectoryContact.primary_lid_jid == sender_jid,
            ),
        )
    ).first()
    if contact:
        return contact.id
    alias = session.exec(
        select(WhatsAppIdentityAlias).where(
            WhatsAppIdentityAlias.account_id == account_id,
            WhatsAppIdentityAlias.lid_jid == sender_jid,
        )
    ).first()
    return alias.contact_id if alias else None


def _insert_idempotently(
    session: Session,
    *,
    table: Any,
    values: dict[str, Any],
    constraint_name: str,
    conflict_columns: list[str],
) -> uuid.UUID | None:
    """Insert once and return the new id, or ``None`` after a conflict.

    History sync and the durable worker outbox can legitimately deliver the
    same WhatsApp event at the same time.  A SELECT followed by INSERT is not
    atomic, so concurrent requests could both observe no row and one would
    fail with a unique-constraint error.  Use the database's native
    ``ON CONFLICT DO NOTHING`` support for PostgreSQL and SQLite instead.
    """

    dialect_name = session.get_bind().dialect.name
    if dialect_name == "postgresql":
        statement = (
            postgresql_insert(table)
            .values(**values)
            .on_conflict_do_nothing(constraint=constraint_name)
            .returning(table.c.id)
        )
        return session.execute(statement).scalar_one_or_none()

    if dialect_name == "sqlite":
        statement = (
            sqlite_insert(table)
            .values(**values)
            .on_conflict_do_nothing(index_elements=conflict_columns)
            .returning(table.c.id)
        )
        return session.execute(statement).scalar_one_or_none()

    # Portable fallback for an unexpected SQLAlchemy dialect.  The nested
    # transaction keeps the outer request Session usable after a conflict.
    try:
        with session.begin_nested():
            session.execute(table.insert().values(**values))
        return values["id"]
    except IntegrityError:
        return None


def _upsert_inbound_message(
    session: Session,
    *,
    account_id: uuid.UUID,
    event: InboundMessageEvent,
    values: dict[str, Any],
    now: datetime,
) -> tuple[uuid.UUID, bool]:
    table = WhatsAppInboundMessage.__table__
    candidate_id = uuid.uuid4()
    inserted_id = _insert_idempotently(
        session,
        table=table,
        values={
            "id": candidate_id,
            "account_id": account_id,
            "message_id": event.messageId,
            "remote_jid": event.remoteJid,
            "first_ingested_at": now,
            **values,
        },
        constraint_name="uq_whatsapp_inbound_message_identity",
        conflict_columns=["account_id", "remote_jid", "message_id"],
    )
    if inserted_id is not None:
        return inserted_id, True

    existing_id = session.execute(
        update(table)
        .where(
            table.c.account_id == account_id,
            table.c.remote_jid == event.remoteJid,
            table.c.message_id == event.messageId,
        )
        .values(**values)
        .returning(table.c.id)
    ).scalar_one_or_none()
    if existing_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The inbound message conflicted but could not be reloaded",
        )
    return existing_id, False


HISTORY_ACTIVE_STATUSES = {"requested", "accepted", "syncing"}
HISTORY_QUIET_SECONDS = 8
HISTORY_NO_RESULT_SECONDS = 45
HISTORY_HARD_TIMEOUT_SECONDS = 180


def _history_contact_counts(
    session: Session, account_id: uuid.UUID, contact_id: uuid.UUID
) -> tuple[int, int]:
    contact = session.get(WhatsAppDirectoryContact, contact_id)
    if contact is None or contact.account_id != account_id:
        return 0, 0
    identity_filter = contact_message_filter(session, contact)
    message_count = session.exec(
        select(func.count(WhatsAppInboundMessage.id)).where(
            identity_filter,
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
    attachment_count = session.exec(
        select(func.count(WhatsAppInboundAttachment.id))
        .join(
            WhatsAppInboundMessage,
            WhatsAppInboundMessage.id == WhatsAppInboundAttachment.message_id,
        )
        .where(
            identity_filter,
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
    return int(message_count or 0), int(attachment_count or 0)


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _serialize_history_request(item: WhatsAppInboundHistoryRequest) -> dict[str, Any]:
    return {
        "accepted": item.status not in {"failed", "timed_out"},
        "id": str(item.id),
        "request_id": item.request_id,
        "account_id": str(item.account_id),
        "contact_id": str(item.contact_id),
        "worker_key": item.worker_key,
        "requested_count": item.requested_count,
        "remote_jid": item.remote_jid,
        "anchor_message_id": item.anchor_message_id,
        "anchor_timestamp": item.anchor_timestamp,
        "operation_id": item.operation_id,
        "status": item.status,
        "baseline_messages": item.baseline_messages,
        "baseline_attachments": item.baseline_attachments,
        "messages_received": item.messages_received,
        "attachments_discovered": item.attachments_discovered,
        "error": item.error,
        "requested_at": item.requested_at,
        "accepted_at": item.accepted_at,
        "last_activity_at": item.last_activity_at,
        "finished_at": item.finished_at,
        "updated_at": item.updated_at,
        "active": item.status in HISTORY_ACTIVE_STATUSES,
    }


def _reconcile_history_requests(
    session: Session, *, contact_id: uuid.UUID | None = None
) -> bool:
    now = _utc_naive(utcnow())
    query = select(WhatsAppInboundHistoryRequest).where(
        WhatsAppInboundHistoryRequest.status.in_(HISTORY_ACTIVE_STATUSES)
    )
    if contact_id is not None:
        query = query.where(WhatsAppInboundHistoryRequest.contact_id == contact_id)
    changed = False
    for item in session.exec(query).all():
        requested_at = _utc_naive(item.requested_at)
        age = (now - requested_at).total_seconds()
        quiet_since = _utc_naive(
            item.last_activity_at or item.accepted_at or item.requested_at
        )
        quiet = (now - quiet_since).total_seconds()
        if item.messages_received > 0 and quiet >= HISTORY_QUIET_SECONDS:
            item.status = "succeeded"
            item.finished_at = now
        elif item.messages_received == 0 and age >= HISTORY_NO_RESULT_SECONDS:
            item.status = "no_results"
            item.finished_at = now
        elif age >= HISTORY_HARD_TIMEOUT_SECONDS:
            item.status = "timed_out"
            item.finished_at = now
            item.error = item.error or "WhatsApp did not finish the history request in time"
        else:
            continue
        item.updated_at = now
        session.add(item)
        changed = True
    if changed:
        session.commit()
    return changed


def _record_history_progress(
    session: Session,
    *,
    account_id: uuid.UUID,
    contact_id: uuid.UUID | None,
    created_message: bool,
    has_attachment: bool,
    ingestion_source: str,
) -> None:
    if not created_message or contact_id is None:
        return
    if ingestion_source not in {"history_sync", "offline_sync"}:
        return
    cutoff = _utc_naive(utcnow()) - timedelta(minutes=10)
    request = session.exec(
        select(WhatsAppInboundHistoryRequest)
        .where(
            WhatsAppInboundHistoryRequest.account_id == account_id,
            WhatsAppInboundHistoryRequest.contact_id == contact_id,
            WhatsAppInboundHistoryRequest.requested_at >= cutoff,
            WhatsAppInboundHistoryRequest.status.in_(
                ["requested", "accepted", "syncing", "no_results", "timed_out"]
            ),
        )
        .order_by(WhatsAppInboundHistoryRequest.requested_at.desc())
    ).first()
    if request is None:
        return
    now = _utc_naive(utcnow())
    request.status = "syncing"
    request.messages_received += 1
    if has_attachment:
        request.attachments_discovered += 1
    request.last_activity_at = now
    request.finished_at = None
    request.error = None
    request.updated_at = now
    session.add(request)


def _upsert_inbound_attachment(
    session: Session,
    *,
    message_id: uuid.UUID,
    mapped: dict[str, Any],
    now: datetime,
) -> uuid.UUID:
    table = WhatsAppInboundAttachment.__table__
    candidate_id = uuid.uuid4()
    inserted_id = _insert_idempotently(
        session,
        table=table,
        values={
            "id": candidate_id,
            "message_id": message_id,
            "created_at": now,
            **mapped,
        },
        constraint_name="uq_whatsapp_inbound_attachment_message",
        conflict_columns=["message_id"],
    )
    if inserted_id is not None:
        return inserted_id

    existing_id = session.execute(
        update(table)
        .where(table.c.message_id == message_id)
        .values(**mapped)
        .returning(table.c.id)
    ).scalar_one_or_none()
    if existing_id is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The inbound attachment conflicted but could not be reloaded",
        )
    return existing_id


@router.post("/events", dependencies=[Depends(_verify_worker_token)])
def ingest_event(
    event: InboundMessageEvent,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    account = _resolve_account(session, event.workerId)
    now = utcnow()
    values = {
        "worker_key": event.workerId,
        "participant_jid": event.participantJid,
        "sender_jid": event.senderJid,
        "directory_contact_id": _resolve_contact_id(
            session, account.id, event.senderJid
        ),
        "from_me": event.fromMe,
        "chat_scope": event.chatScope,
        "message_timestamp": event.messageTimestamp.astimezone(timezone.utc).replace(
            tzinfo=None
        ),
        "push_name": event.pushName,
        "text_content": event.text,
        "message_type": event.messageType,
        "ingestion_source": event.ingestionSource,
        "payload_sha256": event.payloadSha256,
        "raw_payload": event.rawPayload,
        "last_ingested_at": now,
    }
    message_id, created = _upsert_inbound_message(
        session,
        account_id=account.id,
        event=event,
        values=values,
        now=now,
    )

    if event.attachment:
        data = event.attachment.model_dump()
        _upsert_inbound_attachment(
            session,
            message_id=message_id,
            now=now,
            mapped={
                "media_kind": data["mediaKind"],
                "message_key": data["messageKey"],
                "original_filename": data.get("originalFilename"),
                "mime_type": data.get("mimeType"),
                "declared_size": data.get("declaredSize"),
                "media_sha256": data.get("mediaSha256"),
                "caption": data.get("caption"),
                "updated_at": now,
            },
        )

    _record_history_progress(
        session,
        account_id=account.id,
        contact_id=values["directory_contact_id"],
        created_message=created,
        has_attachment=event.attachment is not None,
        ingestion_source=event.ingestionSource,
    )
    session.commit()
    return {"accepted": True, "created": created, "message_id": str(message_id)}


@router.post(
    "/attachments/{attachment_id}/content",
    dependencies=[Depends(_verify_worker_token)],
)
async def upload_attachment_content(
    attachment_id: uuid.UUID,
    request: Request,
    x_content_sha256: str | None = Header(default=None),
    x_declared_mime_type: str | None = Header(default=None),
    x_whatsapp_worker_id: str | None = Header(default=None),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    attachment = session.get(WhatsAppInboundAttachment, attachment_id)
    if attachment is None:
        raise HTTPException(status_code=404, detail="Inbound attachment not found")
    message = session.get(WhatsAppInboundMessage, attachment.message_id)
    if message is None:
        raise HTTPException(status_code=409, detail="Inbound message is missing")
    if not x_whatsapp_worker_id:
        raise HTTPException(status_code=400, detail="X-WhatsApp-Worker-ID is required")
    if not hmac.compare_digest(x_whatsapp_worker_id, message.worker_key):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="The worker is not authorized for this inbound message",
        )
    if not x_content_sha256 or len(x_content_sha256) != 64:
        raise HTTPException(status_code=400, detail="A valid X-Content-SHA256 header is required")

    root = settings.whatsapp_inbound_media_root / str(attachment.id)
    root.mkdir(parents=True, exist_ok=True)
    temporary = root / f"upload-{uuid.uuid4().hex}.part"
    digest = hashlib.sha256()
    size = 0
    try:
        with temporary.open("wb") as handle:
            async for chunk in request.stream():
                if not chunk:
                    continue
                size += len(chunk)
                if size > settings.whatsapp_inbound_media_max_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=(
                            "Inbound attachment exceeds the configured maximum of "
                            f"{settings.whatsapp_inbound_media_max_bytes} bytes"
                        ),
                    )
                digest.update(chunk)
                handle.write(chunk)
        if size == 0:
            raise HTTPException(status_code=400, detail="Worker uploaded an empty file")
        actual_sha256 = digest.hexdigest()
        if x_content_sha256 and not hmac.compare_digest(
            x_content_sha256.lower(), actual_sha256
        ):
            raise HTTPException(status_code=409, detail="Inbound media checksum mismatch")
        try:
            detected_mime, category, extension = detect_file_type(
                temporary,
                declared_mime=x_declared_mime_type or attachment.mime_type,
                original_filename=attachment.original_filename,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
                detail=str(exc),
            ) from exc

        final_path = root / f"attachment-{attachment.id}{extension}"
        os.replace(temporary, final_path)
        previous_path = Path(attachment.stored_path) if attachment.stored_path else None
        if previous_path and previous_path != final_path and previous_path.is_file():
            previous_path.unlink(missing_ok=True)
        attachment.detected_mime_type = detected_mime
        attachment.media_category = category
        attachment.safe_extension = extension
        attachment.actual_size = size
        attachment.actual_sha256 = actual_sha256
        attachment.stored_path = str(final_path)
        attachment.download_status = "archived"
        attachment.last_error = None
        attachment.archived_at = utcnow()
        attachment.updated_at = utcnow()
        session.add(attachment)
        session.commit()
        return {
            "uploaded": True,
            "attachment_id": str(attachment.id),
            "size_bytes": size,
            "sha256": actual_sha256,
            "detected_mime_type": detected_mime,
            "media_category": category,
        }
    finally:
        temporary.unlink(missing_ok=True)


@router.post("/history/request", status_code=status.HTTP_202_ACCEPTED)
async def request_inbound_history(
    data: RequestInboundHistory,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    _reconcile_history_requests(session, contact_id=data.contact_id)
    contact = session.get(WhatsAppDirectoryContact, data.contact_id)
    if contact is None or not contact.active:
        raise HTTPException(status_code=404, detail="WhatsApp contact was not found")
    account = session.get(WhatsAppAccount, contact.account_id)
    if account is None or not account.enabled:
        raise HTTPException(status_code=409, detail="WhatsApp account is unavailable")
    active = session.exec(
        select(WhatsAppInboundHistoryRequest)
        .where(
            WhatsAppInboundHistoryRequest.contact_id == contact.id,
            WhatsAppInboundHistoryRequest.status.in_(HISTORY_ACTIVE_STATUSES),
        )
        .order_by(WhatsAppInboundHistoryRequest.requested_at.desc())
    ).first()
    if active is not None:
        raise HTTPException(
            status_code=409,
            detail=f"A history request is already active ({active.request_id})",
        )
    aliases = session.exec(
        select(WhatsAppIdentityAlias.lid_jid).where(
            WhatsAppIdentityAlias.account_id == account.id,
            WhatsAppIdentityAlias.contact_id == contact.id,
        )
    ).all()
    remote_jids = list(dict.fromkeys(
        value for value in [contact.phone_jid, contact.primary_lid_jid, *aliases] if value
    ))
    if not remote_jids:
        raise HTTPException(status_code=409, detail="This contact has no WhatsApp JID available for history lookup")

    request_id = str(uuid.uuid4())
    baseline_messages, baseline_attachments = _history_contact_counts(
        session, account.id, contact.id
    )
    audit = WhatsAppInboundHistoryRequest(
        request_id=request_id,
        account_id=account.id,
        contact_id=contact.id,
        worker_key=account.worker_key,
        requested_count=data.count,
        baseline_messages=baseline_messages,
        baseline_attachments=baseline_attachments,
    )
    session.add(audit)
    session.commit()
    session.refresh(audit)

    payload = {
        "action": "request_history",
        "requestId": request_id,
        "workerId": account.worker_key,
        "remoteJids": remote_jids,
        "count": data.count,
    }
    subject = f"{settings.whatsapp_inbound_history_subject}.{account.worker_key}"
    client = None
    try:
        client = await nats.connect(settings.whatsapp_nats_url, connect_timeout=2)
        message = await client.request(
            subject,
            json.dumps(payload).encode("utf-8"),
            timeout=settings.whatsapp_inbound_history_timeout_seconds,
        )
        result = json.loads(message.data.decode("utf-8"))
        if not result.get("accepted"):
            raise HTTPException(status_code=409, detail=result.get("error") or "WhatsApp history request was rejected")
        now = utcnow()
        audit.status = "accepted"
        audit.remote_jid = result.get("remoteJid")
        audit.anchor_message_id = result.get("anchorMessageId")
        anchor = result.get("anchorTimestamp")
        audit.anchor_timestamp = datetime.fromisoformat(anchor.replace("Z", "+00:00")).replace(tzinfo=None) if anchor else None
        audit.operation_id = result.get("operationId")
        audit.accepted_at = now
        audit.updated_at = now
        session.add(audit)
        session.commit()
        session.refresh(audit)
        return _serialize_history_request(audit)
    except HTTPException as exc:
        audit.status = "failed"
        audit.error = str(exc.detail)
        audit.finished_at = utcnow()
        audit.updated_at = utcnow()
        session.add(audit)
        session.commit()
        raise
    except NoRespondersError as exc:
        detail = "WhatsApp worker is not listening for history requests"
        audit.status = "failed"; audit.error = detail; audit.finished_at = utcnow(); audit.updated_at = utcnow(); session.add(audit); session.commit()
        raise HTTPException(status_code=503, detail=detail) from exc
    except NatsTimeoutError as exc:
        detail = "WhatsApp history request timed out"
        audit.status = "failed"; audit.error = detail; audit.finished_at = utcnow(); audit.updated_at = utcnow(); session.add(audit); session.commit()
        raise HTTPException(status_code=504, detail=detail) from exc
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        detail = f"WhatsApp history request failed: {exc}"
        audit.status = "failed"; audit.error = detail; audit.finished_at = utcnow(); audit.updated_at = utcnow(); session.add(audit); session.commit()
        raise HTTPException(status_code=502, detail=detail) from exc
    finally:
        if client is not None:
            await client.close()


@router.get("/history/requests")
def list_inbound_history_requests(
    contact_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    _reconcile_history_requests(session, contact_id=contact_id)
    query = select(WhatsAppInboundHistoryRequest)
    if contact_id is not None:
        query = query.where(WhatsAppInboundHistoryRequest.contact_id == contact_id)
    items = session.exec(
        query.order_by(WhatsAppInboundHistoryRequest.requested_at.desc()).limit(limit)
    ).all()
    return {"items": [_serialize_history_request(item) for item in items]}


@router.get("/history/requests/{request_id}")
def get_inbound_history_request(
    request_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    item = session.get(WhatsAppInboundHistoryRequest, request_id)
    if item is None:
        raise HTTPException(status_code=404, detail="History request not found")
    _reconcile_history_requests(session, contact_id=item.contact_id)
    session.refresh(item)
    return _serialize_history_request(item)


@router.post("/exports/preview")
def preview_inbound_export(
    filters: InboundFileFilter,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return build_preview(
            session,
            contact_id=filters.contact_id,
            date_from=filters.date_from,
            date_to=filters.date_to,
            chat_scope=filters.chat_scope,
            media_types=filters.media_types,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/exports", status_code=status.HTTP_202_ACCEPTED)
def create_inbound_export(
    data: CreateInboundExportRequest,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        preview = build_preview(
            session,
            contact_id=data.contact_id,
            date_from=data.date_from,
            date_to=data.date_to,
            chat_scope=data.chat_scope,
            media_types=data.media_types,
            item_limit=1,
        )
        if not preview["files_found"]:
            raise ValueError("No matching inbound files were found")
        contact_name = preview["contact"]["display_name"] or preview["contact"][
            "phone_jid"
        ]
        job = create_job(
            session,
            job_type=JobType.whatsapp_inbound_export.value,
            title=f"WhatsApp inbound files: {contact_name}",
            parameters={
                "contact_id": str(data.contact_id),
                "date_from": data.date_from.isoformat() if data.date_from else None,
                "date_to": data.date_to.isoformat() if data.date_to else None,
                "chat_scope": data.chat_scope,
                "media_types": data.media_types,
                "requested_by": data.requested_by,
            },
        )
        run = create_export_run(
            session,
            job_id=job.id,
            contact_id=data.contact_id,
            date_from=data.date_from,
            date_to=data.date_to,
            chat_scope=data.chat_scope,
            media_types=data.media_types,
        )
        job.parameters = {**job.parameters, "export_run_id": str(run.id)}
        session.add(job)
        session.commit()
        task = build_inbound_export_job.delay(str(job.id))
        set_task_id(session, job.id, task.id)
        return {"job_id": str(job.id), "export": serialize_run(session, run)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/exports")
def list_inbound_exports(
    contact_id: uuid.UUID | None = None,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    filters: list[Any] = []
    if contact_id:
        filters.append(WhatsAppInboundExportRun.contact_id == contact_id)
    total = session.exec(
        select(func.count()).select_from(WhatsAppInboundExportRun).where(*filters)
    ).one()
    runs = session.exec(
        select(WhatsAppInboundExportRun)
        .where(*filters)
        .order_by(col(WhatsAppInboundExportRun.created_at).desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    return {
        "items": [serialize_run(session, run) for run in runs],
        "total": int(total),
        "page": page,
        "page_size": page_size,
    }


@router.get("/exports/{export_id}")
def read_inbound_export(
    export_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    run = session.get(WhatsAppInboundExportRun, export_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Inbound export was not found")
    result = serialize_run(session, run)
    job = session.get(Job, run.job_id)
    result["job"] = {
        "id": str(job.id),
        "status": job.status,
        "error": job.error,
        "result": job.result,
    } if job else None
    return result


@router.get("/status")
def inbound_status(session: Session = Depends(get_session)) -> dict[str, Any]:
    message_count = session.exec(
        select(func.count()).select_from(WhatsAppInboundMessage)
    ).one()
    attachment_count = session.exec(
        select(func.count()).select_from(WhatsAppInboundAttachment)
    ).one()
    archived_count = session.exec(
        select(func.count())
        .select_from(WhatsAppInboundAttachment)
        .where(WhatsAppInboundAttachment.download_status == "archived")
    ).one()
    bounds = session.exec(
        select(
            func.min(WhatsAppInboundMessage.message_timestamp),
            func.max(WhatsAppInboundMessage.message_timestamp),
        )
    ).one()
    unresolved = session.exec(
        select(func.count())
        .select_from(WhatsAppInboundMessage)
        .where(
            WhatsAppInboundMessage.directory_contact_id.is_(None),
            WhatsAppInboundMessage.from_me.is_(False),
        )
    ).one()
    return {
        "messages": int(message_count),
        "attachments": int(attachment_count),
        "archived_attachments": int(archived_count),
        "unresolved_messages": int(unresolved),
        "earliest_message_at": bounds[0],
        "latest_message_at": bounds[1],
    }
