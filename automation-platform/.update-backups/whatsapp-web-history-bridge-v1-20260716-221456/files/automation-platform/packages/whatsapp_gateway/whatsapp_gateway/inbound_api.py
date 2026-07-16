"""Compatibility facade and canonical router for WhatsApp inbound APIs."""

import nats
from fastapi import APIRouter, Depends, status

from whatsapp_gateway.inbound.accounts import (
    resolve_account as _resolve_account,
    resolve_contact_id as _resolve_contact_id,
)
from whatsapp_gateway.inbound.authentication import verify_worker_token as _verify_worker_token
from whatsapp_gateway.inbound.export_api import (
    create_inbound_export,
    list_inbound_exports,
    preview_inbound_export,
    read_inbound_export,
)
from whatsapp_gateway.inbound.history import (
    get_inbound_history_request,
    list_inbound_history_requests,
    request_inbound_history,
)
from whatsapp_gateway.inbound.history_tracking import (
    HISTORY_ACTIVE_STATUSES,
    HISTORY_HARD_TIMEOUT_SECONDS,
    HISTORY_NO_RESULT_SECONDS,
    HISTORY_QUIET_SECONDS,
    history_contact_counts as _history_contact_counts,
    reconcile_history_requests as _reconcile_history_requests,
    record_history_progress as _record_history_progress,
    serialize_history_request as _serialize_history_request,
    utc_naive as _utc_naive,
)
from whatsapp_gateway.inbound.ingestion import ingest_event
from whatsapp_gateway.inbound.media_upload import upload_attachment_content
from whatsapp_gateway.inbound.schemas import (
    AttachmentEvent,
    CreateInboundExportRequest,
    InboundFileFilter,
    InboundMessageEvent,
    RequestInboundHistory,
)
from whatsapp_gateway.inbound.status import inbound_status
from whatsapp_gateway.inbound.upserts import (
    insert_idempotently as _insert_idempotently,
    upsert_inbound_attachment as _upsert_inbound_attachment,
    upsert_inbound_message as _upsert_inbound_message,
)

router = APIRouter(prefix="/api/v1/whatsapp/inbound", tags=["whatsapp-inbound"])
router.post("/events", dependencies=[Depends(_verify_worker_token)])(ingest_event)
router.post(
    "/attachments/{attachment_id}/content",
    dependencies=[Depends(_verify_worker_token)],
)(upload_attachment_content)
router.post("/history/request", status_code=status.HTTP_202_ACCEPTED)(request_inbound_history)
router.get("/history/requests")(list_inbound_history_requests)
router.get("/history/requests/{request_id}")(get_inbound_history_request)
router.post("/exports/preview")(preview_inbound_export)
router.post("/exports", status_code=status.HTTP_202_ACCEPTED)(create_inbound_export)
router.get("/exports")(list_inbound_exports)
router.get("/exports/{export_id}")(read_inbound_export)
router.get("/status")(inbound_status)

__all__ = [
    "AttachmentEvent", "CreateInboundExportRequest", "HISTORY_ACTIVE_STATUSES",
    "HISTORY_HARD_TIMEOUT_SECONDS", "HISTORY_NO_RESULT_SECONDS", "HISTORY_QUIET_SECONDS",
    "InboundFileFilter", "InboundMessageEvent", "RequestInboundHistory",
    "_history_contact_counts", "_insert_idempotently", "_reconcile_history_requests",
    "_record_history_progress", "_resolve_account", "_resolve_contact_id",
    "_serialize_history_request", "_upsert_inbound_attachment", "_upsert_inbound_message",
    "_utc_naive", "_verify_worker_token", "create_inbound_export",
    "get_inbound_history_request", "inbound_status", "ingest_event",
    "list_inbound_exports", "list_inbound_history_requests", "nats",
    "preview_inbound_export", "read_inbound_export", "request_inbound_history",
    "router", "upload_attachment_content",
]
