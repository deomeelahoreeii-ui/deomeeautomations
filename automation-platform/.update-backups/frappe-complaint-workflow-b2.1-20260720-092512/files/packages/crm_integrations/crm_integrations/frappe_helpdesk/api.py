from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_integrations.frappe_helpdesk.client import FrappeHelpdeskError
from crm_integrations.frappe_helpdesk.service import (
    ComplaintHelpdeskSyncService,
    PreviewBatchChangedError,
    PreviewBatchError,
)
from crm_integrations.frappe_helpdesk.batch_tokens import SignedBatchError
from crm_integrations.frappe_helpdesk.routing import (
    ComplaintHelpdeskRoutingService,
    RoutingBatchChangedError,
)
from crm_integrations.frappe_helpdesk.workflow import ComplaintHelpdeskWorkflowService


router = APIRouter(prefix="/api/v1/crm/helpdesk", tags=["crm-helpdesk"])


class SyncBatchRequest(BaseModel):
    case_ids: list[uuid.UUID] = Field(default_factory=list, max_length=200)
    limit: int = Field(default=100, ge=1, le=200)
    force: bool = False
    preview_token: str | None = Field(default=None, min_length=20)


class RoutingBatchRequest(BaseModel):
    preview_token: str = Field(min_length=20)
    force: bool = False


class WorkflowPullRequest(BaseModel):
    case_ids: list[uuid.UUID] = Field(default_factory=list, max_length=500)
    limit: int = Field(default=200, ge=1, le=500)


def _service(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> ComplaintHelpdeskSyncService:
    return ComplaintHelpdeskSyncService(session, settings)


def _routing_service(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> ComplaintHelpdeskRoutingService:
    return ComplaintHelpdeskRoutingService(session, settings)


def _workflow_service(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> ComplaintHelpdeskWorkflowService:
    return ComplaintHelpdeskWorkflowService(session, settings)


@router.get("/health")
def helpdesk_health(
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    try:
        return service.health()
    except FrappeHelpdeskError as exc:
        raise HTTPException(
            status_code=503,
            detail={"message": str(exc), "http_status": exc.status_code},
        ) from exc


@router.get("/statistics")
def helpdesk_statistics(
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    return service.statistics()


@router.get("/preview")
def helpdesk_preview(
    limit: int = Query(default=200, ge=1, le=200),
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    return service.preview(limit=limit)


@router.post("/bootstrap")
def bootstrap_helpdesk(
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    try:
        return service.bootstrap()
    except (FrappeHelpdeskError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/sync")
def sync_helpdesk_batch(
    payload: SyncBatchRequest,
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    try:
        return service.sync_many(
            case_ids=payload.case_ids or None,
            preview_token=payload.preview_token,
            limit=payload.limit,
            force=payload.force,
        )
    except PreviewBatchChangedError as exc:
        raise HTTPException(
            status_code=409,
            detail={"message": str(exc), "changes": exc.changes},
        ) from exc
    except (PreviewBatchError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/cases/{case_id}/sync")
def sync_helpdesk_case(
    case_id: uuid.UUID,
    force: bool = Query(default=False),
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    try:
        return service.sync_case(case_id, force=force)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/links")
def helpdesk_links(
    limit: int = Query(default=200, ge=1, le=1000),
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    return service.audit_links(limit=limit)


@router.get("/events")
def helpdesk_events(
    limit: int = Query(default=100, ge=1, le=500),
    service: ComplaintHelpdeskSyncService = Depends(_service),
) -> dict[str, Any]:
    return {"items": service.recent_events(limit=limit)}


@router.post("/teams/bootstrap")
def bootstrap_helpdesk_teams(
    service: ComplaintHelpdeskRoutingService = Depends(_routing_service),
) -> dict[str, Any]:
    try:
        return service.bootstrap_teams()
    except (FrappeHelpdeskError, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/routing/statistics")
def helpdesk_routing_statistics(
    service: ComplaintHelpdeskRoutingService = Depends(_routing_service),
) -> dict[str, Any]:
    return service.statistics()


@router.get("/routing/preview")
def helpdesk_routing_preview(
    limit: int = Query(default=200, ge=1, le=500),
    service: ComplaintHelpdeskRoutingService = Depends(_routing_service),
) -> dict[str, Any]:
    try:
        return service.preview(limit=limit)
    except FrappeHelpdeskError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/routing/apply")
def apply_helpdesk_routing(
    payload: RoutingBatchRequest,
    service: ComplaintHelpdeskRoutingService = Depends(_routing_service),
) -> dict[str, Any]:
    try:
        return service.apply(payload.preview_token, force=payload.force)
    except RoutingBatchChangedError as exc:
        raise HTTPException(status_code=409, detail={"message": str(exc), "changes": exc.changes}) from exc
    except SignedBatchError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/workflow/statistics")
def helpdesk_workflow_statistics(
    service: ComplaintHelpdeskWorkflowService = Depends(_workflow_service),
) -> dict[str, Any]:
    return service.statistics()


@router.get("/workflow/preview")
def helpdesk_workflow_preview(
    limit: int = Query(default=200, ge=1, le=500),
    service: ComplaintHelpdeskWorkflowService = Depends(_workflow_service),
) -> dict[str, Any]:
    return service.preview(limit=limit)


@router.post("/workflow/pull")
def pull_helpdesk_workflow(
    payload: WorkflowPullRequest,
    service: ComplaintHelpdeskWorkflowService = Depends(_workflow_service),
) -> dict[str, Any]:
    return service.pull_many(case_ids=payload.case_ids or None, limit=payload.limit)


@router.post("/cases/{case_id}/workflow/pull")
def pull_helpdesk_case_workflow(
    case_id: uuid.UUID,
    service: ComplaintHelpdeskWorkflowService = Depends(_workflow_service),
) -> dict[str, Any]:
    try:
        return service.pull_case(case_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/workflow/audit")
def helpdesk_workflow_audit(
    limit: int = Query(default=200, ge=1, le=500),
    service: ComplaintHelpdeskWorkflowService = Depends(_workflow_service),
) -> dict[str, Any]:
    return service.audit(limit=limit)
