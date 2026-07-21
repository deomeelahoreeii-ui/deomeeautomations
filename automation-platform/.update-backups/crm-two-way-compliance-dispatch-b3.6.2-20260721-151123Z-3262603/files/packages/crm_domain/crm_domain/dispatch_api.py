from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlmodel import Session

from automation_core.database import get_session
from crm_domain.dispatch import (
    CrmDispatchError,
    CrmDispatchNotFound,
    CrmDispatchService,
    CrmDispatchValidationError,
)

router = APIRouter(prefix="/api/v1/crm/dispatch", tags=["crm-dispatch"])


class DestinationProfileInput(BaseModel):
    id: uuid.UUID | None = None
    name: str = Field(min_length=2, max_length=200)
    target_type: str
    target_ids: list[uuid.UUID]
    template_body: str = Field(default="", max_length=10_000)
    packet_policy: str = "complete_pdf"
    privacy_policy: str = "full"
    max_packet_bytes: int = 15 * 1024 * 1024
    require_approval: bool = True
    messages_per_minute: int = 20
    max_retries: int = 5
    enabled: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class EnabledInput(BaseModel):
    enabled: bool


class RoutingRuleInput(BaseModel):
    id: uuid.UUID | None = None
    name: str = Field(min_length=2, max_length=200)
    description: str | None = Field(default=None, max_length=5_000)
    priority: int = Field(default=100, ge=-10_000, le=10_000)
    selection_mode: str = "suggested"
    conditions: dict[str, Any] = Field(default_factory=dict)
    profile_ids: list[uuid.UUID]
    stop_after_match: bool = True
    enabled: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class BatchInput(BaseModel):
    official_letter_ids: list[uuid.UUID]
    purpose: str = Field(default="complaint_forwarding", max_length=80)
    response_due_at: datetime | None = None
    actor: str = Field(default="web-operator", max_length=120)


class ActorInput(BaseModel):
    actor: str = Field(default="web-operator", max_length=120)


class ManualRouteInput(BaseModel):
    profile_ids: list[uuid.UUID]
    reason: str = Field(default="", max_length=2_000)
    actor: str = Field(default="web-operator", max_length=120)


class ExclusionInput(BaseModel):
    excluded: bool
    reason: str = Field(default="", max_length=2_000)


def service(session: Session) -> CrmDispatchService:
    return CrmDispatchService(session)


def fail(exc: CrmDispatchError) -> None:
    if isinstance(exc, CrmDispatchNotFound):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, CrmDispatchValidationError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/bootstrap")
def bootstrap(session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        result = service(session).ensure_defaults()
        return {
            "application_id": str(result["application"].id),
            "application_name": result["application"].name,
            "report_type_id": str(result["report_type"].id),
            "report_type_name": result["report_type"].name,
            "account_id": str(result["account"].id),
            "account_name": result["account"].name,
            "template_id": str(result["template"].id),
        }
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/statistics")
def statistics(session: Session = Depends(get_session)) -> dict[str, int]:
    return service(session).statistics()


@router.get("/profiles")
def profiles(
    search: str = Query(default="", max_length=300),
    include_inactive: bool = False,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        items = service(session).profiles(search=search, include_inactive=include_inactive)
        return {"items": items, "total": len(items)}
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/profiles", status_code=status.HTTP_201_CREATED)
def save_profile(payload: DestinationProfileInput, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).save_profile(
            profile_id=payload.id,
            name=payload.name,
            target_type=payload.target_type,
            target_ids=payload.target_ids,
            template_body=payload.template_body,
            packet_policy=payload.packet_policy,
            privacy_policy=payload.privacy_policy,
            max_packet_bytes=payload.max_packet_bytes,
            require_approval=payload.require_approval,
            messages_per_minute=payload.messages_per_minute,
            max_retries=payload.max_retries,
            enabled=payload.enabled,
            actor=payload.actor,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.patch("/profiles/{profile_id}/enabled")
def set_profile_enabled(
    profile_id: uuid.UUID,
    payload: EnabledInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).set_profile_enabled(profile_id, payload.enabled)
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/rules")
def rules(include_inactive: bool = True, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        items = service(session).rules(include_inactive=include_inactive)
        return {"items": items, "total": len(items)}
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/rules", status_code=status.HTTP_201_CREATED)
def save_rule(payload: RoutingRuleInput, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).save_rule(
            rule_id=payload.id,
            name=payload.name,
            description=payload.description,
            priority=payload.priority,
            selection_mode=payload.selection_mode,
            conditions=payload.conditions,
            profile_ids=payload.profile_ids,
            stop_after_match=payload.stop_after_match,
            enabled=payload.enabled,
            actor=payload.actor,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        service(session).delete_rule(rule_id)
        return {"id": str(rule_id), "enabled": False}
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/routing-test/{case_id}")
def routing_test(case_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).test_routing(case_id)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches", status_code=status.HTTP_201_CREATED)
def create_batch(payload: BatchInput, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).create_batch(
            official_letter_ids=payload.official_letter_ids,
            actor=payload.actor,
            purpose=payload.purpose,
            response_due_at=payload.response_due_at,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.get("/batches")
def list_batches(
    search: str = Query(default="", max_length=300),
    status_filter: str = Query(default="", alias="status", max_length=40),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    return service(session).list_batches(search=search, status=status_filter, page=page, page_size=page_size)


@router.get("/batches/{batch_id}")
def detail(batch_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).detail(batch_id)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/resolve")
def resolve(batch_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).resolve_batch(batch_id)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/compile")
def compile_batch(
    batch_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).compile_previews(batch_id, actor=payload.actor)
    except CrmDispatchError as exc:
        fail(exc)


@router.post("/batches/{batch_id}/refresh")
def refresh(batch_id: uuid.UUID, session: Session = Depends(get_session)) -> dict[str, Any]:
    try:
        return service(session).refresh(batch_id)
    except CrmDispatchError as exc:
        fail(exc)


@router.put("/items/{item_id}/route")
def manual_route(
    item_id: uuid.UUID,
    payload: ManualRouteInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).set_manual_route(
            item_id=item_id,
            profile_ids=payload.profile_ids,
            reason=payload.reason,
            actor=payload.actor,
        )
    except CrmDispatchError as exc:
        fail(exc)


@router.put("/items/{item_id}/excluded")
def exclude_item(
    item_id: uuid.UUID,
    payload: ExclusionInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    try:
        return service(session).set_item_excluded(item_id, excluded=payload.excluded, reason=payload.reason)
    except CrmDispatchError as exc:
        fail(exc)
