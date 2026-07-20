from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.reply_workspace import ComplaintReplyWorkspaceService
from crm_domain.taxonomy import ComplaintTaxonomyService, TaxonomyError, TaxonomyMutation


router = APIRouter(prefix="/api/v1/crm/taxonomy", tags=["crm-taxonomy"])


class CategoryInput(BaseModel):
    name: str = Field(min_length=1, max_length=140)
    description: str = Field(default="", max_length=10_000)
    display_order: int = Field(default=100, ge=0, le=100_000)
    active: bool = True
    default_ai_eligible: bool = False
    reply_guidance: str = Field(default="", max_length=50_000)
    policy_notes: str = Field(default="", max_length=50_000)
    actor: str = Field(default="web-operator", max_length=120)
    synchronize_cases: bool = True


class SubcategoryInput(BaseModel):
    category_id: uuid.UUID
    name: str = Field(min_length=1, max_length=140)
    description: str = Field(default="", max_length=10_000)
    display_order: int = Field(default=100, ge=0, le=100_000)
    active: bool = True
    reply_guidance: str = Field(default="", max_length=50_000)
    policy_notes: str = Field(default="", max_length=50_000)
    actor: str = Field(default="web-operator", max_length=120)
    synchronize_cases: bool = True


class ActiveInput(BaseModel):
    active: bool
    actor: str = Field(default="web-operator", max_length=120)
    synchronize_cases: bool = True


class MergeInput(BaseModel):
    target_id: uuid.UUID
    actor: str = Field(default="web-operator", max_length=120)
    synchronize_cases: bool = True


def _services(
    session: Session,
    settings: Settings,
) -> tuple[ComplaintTaxonomyService, ComplaintReplyWorkspaceService]:
    return (
        ComplaintTaxonomyService(session, settings),
        ComplaintReplyWorkspaceService(session, settings),
    )


def _sync_affected(
    workspace: ComplaintReplyWorkspaceService,
    mutation: TaxonomyMutation,
    *,
    actor: str,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled or not mutation.affected_case_ids:
        return {"requested": len(mutation.affected_case_ids), "results": [], "counts": {}}
    results = [
        workspace.sync_classification(case_id, actor=actor)
        for case_id in mutation.affected_case_ids
    ]
    counts: dict[str, int] = {}
    for result in results:
        status = str(result.get("status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return {"requested": len(results), "results": results, "counts": counts}


def _mutation_response(
    mutation: TaxonomyMutation,
    workspace: ComplaintReplyWorkspaceService,
    *,
    actor: str,
    synchronize_cases: bool,
) -> dict[str, Any]:
    return {
        "entity": mutation.entity,
        "frappe_category_sync": mutation.remote_sync,
        "case_sync": _sync_affected(
            workspace,
            mutation,
            actor=actor,
            enabled=synchronize_cases,
        ),
    }


@router.get("")
def taxonomy_tree(
    include_inactive: bool = Query(default=True),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return ComplaintTaxonomyService(session, settings).tree(
        include_inactive=include_inactive
    )


@router.get("/statistics")
def taxonomy_statistics(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return ComplaintTaxonomyService(session, settings).statistics()


@router.get("/audit")
def taxonomy_audit(
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return ComplaintTaxonomyService(session, settings).audit(limit=limit)


@router.post("/categories")
def create_category(
    payload: CategoryInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.create_category(
            name=payload.name,
            description=payload.description,
            display_order=payload.display_order,
            active=payload.active,
            default_ai_eligible=payload.default_ai_eligible,
            reply_guidance=payload.reply_guidance,
            policy_notes=payload.policy_notes,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.put("/categories/{category_id}")
def update_category(
    category_id: uuid.UUID,
    payload: CategoryInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.update_category(
            category_id,
            name=payload.name,
            description=payload.description,
            display_order=payload.display_order,
            active=payload.active,
            default_ai_eligible=payload.default_ai_eligible,
            reply_guidance=payload.reply_guidance,
            policy_notes=payload.policy_notes,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.patch("/categories/{category_id}/active")
def set_category_active(
    category_id: uuid.UUID,
    payload: ActiveInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.set_category_active(
            category_id,
            active=payload.active,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/categories/{category_id}/sync")
def sync_category(
    category_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return ComplaintTaxonomyService(session, settings).sync_category(category_id)
    except TaxonomyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/categories/{category_id}/merge")
def merge_category(
    category_id: uuid.UUID,
    payload: MergeInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.merge_category(
            category_id,
            payload.target_id,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/subcategories")
def create_subcategory(
    payload: SubcategoryInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.create_subcategory(
            category_id=payload.category_id,
            name=payload.name,
            description=payload.description,
            display_order=payload.display_order,
            active=payload.active,
            reply_guidance=payload.reply_guidance,
            policy_notes=payload.policy_notes,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.put("/subcategories/{subcategory_id}")
def update_subcategory(
    subcategory_id: uuid.UUID,
    payload: SubcategoryInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.update_subcategory(
            subcategory_id,
            category_id=payload.category_id,
            name=payload.name,
            description=payload.description,
            display_order=payload.display_order,
            active=payload.active,
            reply_guidance=payload.reply_guidance,
            policy_notes=payload.policy_notes,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.patch("/subcategories/{subcategory_id}/active")
def set_subcategory_active(
    subcategory_id: uuid.UUID,
    payload: ActiveInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.set_subcategory_active(
            subcategory_id,
            active=payload.active,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/subcategories/{subcategory_id}/merge")
def merge_subcategory(
    subcategory_id: uuid.UUID,
    payload: MergeInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    taxonomy, workspace = _services(session, settings)
    try:
        mutation = taxonomy.merge_subcategory(
            subcategory_id,
            payload.target_id,
            actor=payload.actor,
        )
        return _mutation_response(
            mutation,
            workspace,
            actor=payload.actor,
            synchronize_cases=payload.synchronize_cases,
        )
    except TaxonomyError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
