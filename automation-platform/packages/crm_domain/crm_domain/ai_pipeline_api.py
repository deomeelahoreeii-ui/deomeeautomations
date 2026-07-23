from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel, Field
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.ai_pipeline import AiPipelineError, CrmAiPipelineService
from crm_domain.bulk_operations import BulkOperationError, BulkOperationNotFound, BulkOperationValidationError


router = APIRouter(prefix="/api/v1/crm/ai-pipeline", tags=["crm-ai-pipeline"])
MAX_AI_CSV_BYTES = 5 * 1024 * 1024


class ClassificationExportInput(BaseModel):
    scope: str = Field(
        default="unclassified",
        pattern="^(unclassified|awaiting_reply|all|selected)$",
    )
    case_ids: list[uuid.UUID] = Field(default_factory=list, max_length=1000)
    actor: str = Field(default="web-operator", max_length=120)


class ClassificationCommitInput(BaseModel):
    allow_partial: bool = False
    actor: str = Field(default="web-operator", max_length=120)


class ResolveClassificationInput(BaseModel):
    category_id: uuid.UUID | None = None
    subcategory_id: uuid.UUID | None = None
    tag_ids: list[uuid.UUID] = Field(default_factory=list, max_length=100)
    decision: str = Field(default="approve", pattern="^(approve|reject)$")
    actor: str = Field(default="web-operator", max_length=120)




class ResolveTaxonomySuggestionInput(BaseModel):
    status: str = Field(pattern="^(merged|rejected|deferred)$")
    resolved_category_id: uuid.UUID | None = None
    resolved_subcategory_id: uuid.UUID | None = None
    resolved_tag_id: uuid.UUID | None = None
    actor: str = Field(default="web-operator", max_length=120)


class TagGroupInput(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    slug: str | None = Field(default=None, max_length=80)
    description: str = Field(default="", max_length=2000)
    display_order: int = Field(default=100, ge=0, le=9999)
    active: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class TagInput(BaseModel):
    display_name: str = Field(min_length=1, max_length=140)
    slug: str | None = Field(default=None, max_length=120)
    group_name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=4000)
    aliases: list[str] = Field(default_factory=list, max_length=50)
    active: bool = True
    ai_available: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class TagMergeInput(BaseModel):
    target_tag_id: uuid.UUID
    actor: str = Field(default="web-operator", max_length=120)


class TagSuggestionInput(BaseModel):
    display_name: str = Field(min_length=1, max_length=140)
    group_name: str = Field(min_length=1, max_length=80)
    reason: str = Field(default="", max_length=4000)
    actor: str = Field(default="web-operator", max_length=120)


class CreateTagFromSuggestionInput(BaseModel):
    display_name: str | None = Field(default=None, max_length=140)
    slug: str | None = Field(default=None, max_length=120)
    group_name: str | None = Field(default=None, max_length=80)
    description: str = Field(default="", max_length=4000)
    aliases: list[str] = Field(default_factory=list, max_length=50)
    active: bool = True
    ai_available: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class ReplyContextExportInput(BaseModel):
    scope: str = Field(default="awaiting", pattern="^(awaiting|selected|classification_batch)$")
    parent_batch_id: uuid.UUID | None = None
    case_ids: list[uuid.UUID] = Field(default_factory=list, max_length=1000)
    examples_limit: int = Field(default=4, ge=0, le=10)
    redact_personal_data: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class PromptVersionInput(BaseModel):
    version_label: str = Field(min_length=1, max_length=80)
    content: str = Field(min_length=1, max_length=200000)
    activate: bool = True
    actor: str = Field(default="web-operator", max_length=120)


class ActorInput(BaseModel):
    actor: str = Field(default="web-operator", max_length=120)


def _service(session: Session, settings: Settings) -> CrmAiPipelineService:
    return CrmAiPipelineService(session, settings)


def _raise(exc: BulkOperationError) -> None:
    if isinstance(exc, BulkOperationNotFound):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, (BulkOperationValidationError, AiPipelineError)):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/statistics")
def statistics(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).statistics()


@router.get("/taxonomy-options")
def taxonomy_options(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).taxonomy_options()


@router.post("/classification-exports")
def classification_export(
    payload: ClassificationExportInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_classification_export_batch(
            scope=payload.scope,
            case_ids=payload.case_ids,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/classification-imports/validate")
async def classification_import_validate(
    file: UploadFile = File(...),
    parent_batch_id: uuid.UUID = Form(...),
    auto_accept_threshold: float = Form(default=0.85, ge=0, le=1),
    actor: str = Form(default="web-operator", max_length=120),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    filename = file.filename or "classification.csv"
    if not filename.casefold().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Upload a UTF-8 classification CSV file.")
    content = await file.read(MAX_AI_CSV_BYTES + 1)
    await file.close()
    if len(content) > MAX_AI_CSV_BYTES:
        raise HTTPException(status_code=413, detail="The classification CSV exceeds 5 MB.")
    try:
        return _service(session, settings).validate_classification_import(
            content=content,
            filename=filename,
            parent_batch_id=parent_batch_id,
            actor=actor,
            auto_accept_threshold=auto_accept_threshold,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/classification-imports/{batch_id}/commit")
def classification_import_commit(
    batch_id: uuid.UUID,
    payload: ClassificationCommitInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).commit_classification_import(
            batch_id,
            allow_partial=payload.allow_partial,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/classification-review")
def classification_review(
    search: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).review_queue(search=search, page=page, page_size=page_size)


@router.post("/classification-review/{classification_id}/resolve")
def classification_resolve(
    classification_id: uuid.UUID,
    payload: ResolveClassificationInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).resolve_classification(
            classification_id,
            category_id=payload.category_id,
            subcategory_id=payload.subcategory_id,
            tag_ids=payload.tag_ids,
            decision=payload.decision,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/taxonomy-suggestions")
def taxonomy_suggestions(
    status: str = Query(default="pending", pattern="^(pending|approved|merged|rejected|deferred|all)$"),
    search: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).taxonomy_suggestions(
            status=status, search=search, page=page, page_size=page_size
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/taxonomy-suggestions/{suggestion_id}/resolve")
def taxonomy_suggestion_resolve(
    suggestion_id: uuid.UUID,
    payload: ResolveTaxonomySuggestionInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).resolve_taxonomy_suggestion(
            suggestion_id,
            status=payload.status,
            actor=payload.actor,
            resolved_category_id=payload.resolved_category_id,
            resolved_subcategory_id=payload.resolved_subcategory_id,
            resolved_tag_id=payload.resolved_tag_id,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/tag-statistics")
def tag_statistics(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).tag_statistics()


@router.get("/tag-groups")
def tag_groups(
    include_inactive: bool = Query(default=False),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).tag_groups(include_inactive=include_inactive)


@router.post("/tag-groups")
def create_tag_group(
    payload: TagGroupInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_tag_group(
            name=payload.name,
            slug=payload.slug,
            description=payload.description,
            display_order=payload.display_order,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.put("/tag-groups/{group_id}")
def update_tag_group(
    group_id: uuid.UUID,
    payload: TagGroupInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).update_tag_group(
            group_id,
            name=payload.name,
            description=payload.description,
            display_order=payload.display_order,
            active=payload.active,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/tags")
def tags(
    search: str = Query(default="", max_length=200),
    group: str = Query(default="", max_length=80),
    status: str = Query(default="active", pattern="^(active|inactive|all)$"),
    ai_available: bool | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).tags(
            search=search,
            group=group,
            status=status,
            ai_available=ai_available,
            page=page,
            page_size=page_size,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/tags")
def create_tag(
    payload: TagInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_tag(
            display_name=payload.display_name,
            slug=payload.slug,
            group_name=payload.group_name,
            description=payload.description,
            aliases=payload.aliases,
            active=payload.active,
            ai_available=payload.ai_available,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.put("/tags/{tag_id}")
def update_tag(
    tag_id: uuid.UUID,
    payload: TagInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).update_tag(
            tag_id,
            display_name=payload.display_name,
            slug=payload.slug,
            group_name=payload.group_name,
            description=payload.description,
            aliases=payload.aliases,
            active=payload.active,
            ai_available=payload.ai_available,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/tags/{tag_id}/deactivate")
def deactivate_tag(
    tag_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).deactivate_tag(tag_id, actor=payload.actor)
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/tags/{tag_id}/merge")
def merge_tag(
    tag_id: uuid.UUID,
    payload: TagMergeInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).merge_tag(
            tag_id,
            target_tag_id=payload.target_tag_id,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/classification-review/{classification_id}/tag-suggestions")
def suggest_tag(
    classification_id: uuid.UUID,
    payload: TagSuggestionInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).suggest_tag(
            classification_id,
            display_name=payload.display_name,
            group_name=payload.group_name,
            reason=payload.reason,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/taxonomy-suggestions/{suggestion_id}/create-tag")
def create_tag_from_suggestion(
    suggestion_id: uuid.UUID,
    payload: CreateTagFromSuggestionInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_tag_from_suggestion(
            suggestion_id,
            display_name=payload.display_name,
            slug=payload.slug,
            group_name=payload.group_name,
            description=payload.description,
            aliases=payload.aliases,
            active=payload.active,
            ai_available=payload.ai_available,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/reply-context-exports")
def reply_context_export(
    payload: ReplyContextExportInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_reply_context_export(
            scope=payload.scope,
            parent_batch_id=payload.parent_batch_id,
            case_ids=payload.case_ids,
            examples_limit=payload.examples_limit,
            redact_personal_data=payload.redact_personal_data,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/prompt-profiles")
def prompt_profiles(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).prompt_profiles()


@router.get("/prompt-versions/{version_id}")
def prompt_version(
    version_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).prompt_version(version_id)
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/prompt-profiles/{profile_id}/versions")
def create_prompt_version(
    profile_id: uuid.UUID,
    payload: PromptVersionInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_prompt_version(
            profile_id,
            version_label=payload.version_label,
            content=payload.content,
            activate=payload.activate,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/prompt-versions/{version_id}/activate")
def activate_prompt_version(
    version_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).activate_prompt_version(version_id, actor=payload.actor)
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/reference/{kind}")
def reference_file(
    kind: str,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> Response:
    try:
        name, content_type, content = _service(session, settings).reference_file(kind)
        return Response(
            content=content,
            media_type=content_type,
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )
    except BulkOperationError as exc:
        _raise(exc)
