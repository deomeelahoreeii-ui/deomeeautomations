"""Compatibility facade and canonical router for WhatsApp inbound APIs."""

import nats
from fastapi import APIRouter, Depends, status

from whatsapp_gateway.inbound.accounts import (
    resolve_account as _resolve_account,
    resolve_contact_id as _resolve_contact_id,
)
from whatsapp_gateway.inbound.authentication import verify_worker_token as _verify_worker_token
from whatsapp_gateway.inbound.batch_api import (
    download_inbound_batch_item,
    list_inbound_batch_events,
    list_inbound_batches,
    object_storage_status,
    read_inbound_batch,
    retry_inbound_batch_storage,
)
from whatsapp_gateway.inbound.export_api import (
    create_inbound_export,
    list_inbound_exports,
    preview_inbound_export,
    read_inbound_export,
)
from whatsapp_gateway.inbound.history import (
    get_inbound_history_request,
    history_bridge_status,
    list_inbound_history_requests,
    request_inbound_history,
)
from whatsapp_gateway.inbound.contact_workspace import (
    contact_intake_workspace,
    download_contact_evidence,
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
from whatsapp_gateway.inbound.processing_api import (
    batch_approve_inbound_complaint_groups,
    create_inbound_processing_run,
    preview_inbound_processing_item,
    review_inbound_complaint_group,
    review_inbound_processing_item,
)
from whatsapp_gateway.inbound.processing_query_api import (
    list_inbound_complaint_groups,
    list_inbound_processing_events,
    list_inbound_processing_runs,
    read_inbound_processing_item,
    read_inbound_processing_run,
)
from whatsapp_gateway.inbound.schemas import (
    AttachmentEvent,
    CreateInboundExportRequest,
    InboundFileFilter,
    InboundMessageEvent,
    RequestInboundHistory,
)
from whatsapp_gateway.inbound.status import inbound_status
from whatsapp_gateway.inbound.spreadsheet_intake_api import (
    list_spreadsheet_intake_batches,
    read_spreadsheet_intake_batch,
    review_spreadsheet_intake_row,
)
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
router.get("/history/bridge/status")(history_bridge_status)
router.get("/storage/status")(object_storage_status)
router.get("/contacts/{contact_id}/workspace")(contact_intake_workspace)
router.get("/contacts/{contact_id}/evidence/{attachment_id}/download")(download_contact_evidence)
router.get("/batches")(list_inbound_batches)
router.get("/batches/{batch_id}")(read_inbound_batch)
router.get("/batches/{batch_id}/events")(list_inbound_batch_events)
router.get("/batches/{batch_id}/items/{item_id}/download")(download_inbound_batch_item)
router.post("/batches/{batch_id}/retry-storage")(retry_inbound_batch_storage)
router.post("/history/request", status_code=status.HTTP_202_ACCEPTED)(request_inbound_history)
router.get("/history/requests")(list_inbound_history_requests)
router.get("/history/requests/{request_id}")(get_inbound_history_request)
router.post("/exports/preview")(preview_inbound_export)
router.post("/exports", status_code=status.HTTP_202_ACCEPTED)(create_inbound_export)
router.get("/exports")(list_inbound_exports)
router.get("/exports/{export_id}")(read_inbound_export)
router.post("/processing-runs", status_code=status.HTTP_202_ACCEPTED)(create_inbound_processing_run)
router.get("/processing-runs")(list_inbound_processing_runs)
router.get("/processing-runs/{run_id}")(read_inbound_processing_run)
router.get("/processing-runs/{run_id}/events")(list_inbound_processing_events)
router.get("/processing-runs/{run_id}/complaint-groups")(list_inbound_complaint_groups)
router.post("/processing-runs/{run_id}/complaint-group-approvals")(
    batch_approve_inbound_complaint_groups
)
router.post("/processing-runs/{run_id}/complaint-groups/{complaint_number}/review")(
    review_inbound_complaint_group
)
router.post("/processing-items/{item_id}/review")(review_inbound_processing_item)
router.get("/processing-items/{item_id}")(read_inbound_processing_item)
router.get("/processing-items/{item_id}/content")(preview_inbound_processing_item)
router.get("/spreadsheet-batches")(list_spreadsheet_intake_batches)
router.get("/spreadsheet-batches/{batch_id}")(read_spreadsheet_intake_batch)
router.post("/spreadsheet-rows/{row_id}/review")(review_spreadsheet_intake_row)
router.get("/status")(inbound_status)
