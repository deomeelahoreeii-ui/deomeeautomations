from __future__ import annotations

import uuid
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.reply_workspace import (
    ComplaintReplyWorkspaceService,
    ReplyNotFoundError,
    ReplyRemoteError,
    ReplyValidationError,
    ReplyWorkspaceError,
)


router = APIRouter(
    prefix="/api/v1/crm/reply-workspace",
    tags=["crm-reply-workspace"],
)


class ClassificationInput(BaseModel):
    category_id: uuid.UUID
    subcategory_id: uuid.UUID | None = None
    actor: str = Field(default="web-operator", max_length=120)
    synchronize: bool = True


class BulkClassificationInput(ClassificationInput):
    case_ids: list[uuid.UUID] = Field(min_length=1, max_length=500)


class ReplyInput(BaseModel):
    inquiry_findings: str = Field(default="", max_length=200_000)
    school_version: str = Field(default="", max_length=200_000)
    applicable_policy: str = Field(default="", max_length=200_000)
    final_reply: str = Field(default="", max_length=500_000)
    reply_status: str = Field(default="Draft", max_length=40)
    disposal_outcome: str = Field(default="", max_length=10_000)
    ai_eligible: bool = False
    actor: str = Field(default="web-operator", max_length=120)


def _service(
    session: Session,
    settings: Settings,
) -> ComplaintReplyWorkspaceService:
    return ComplaintReplyWorkspaceService(session, settings)


@router.get("/statistics")
def reply_workspace_statistics(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).statistics()


@router.get("/cases")
def list_reply_cases(
    category_id: uuid.UUID | None = Query(default=None),
    subcategory_id: uuid.UUID | None = Query(default=None),
    reply_status: str = Query(default="", max_length=40),
    source_system: str = Query(default="", max_length=80),
    search: str = Query(default="", max_length=500),
    ai_eligible: bool | None = Query(default=None),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).list_cases(
        category_id=category_id,
        subcategory_id=subcategory_id,
        reply_status=reply_status,
        source_system=source_system,
        search=search,
        ai_eligible=ai_eligible,
        date_from=date_from,
        date_to=date_to,
        page=page,
        page_size=page_size,
    )


@router.get("/cases/{case_id}")
def reply_editor(
    case_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).editor(case_id)
    except ReplyNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ReplyWorkspaceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.put("/cases/{case_id}/classification")
def save_case_classification(
    case_id: uuid.UUID,
    payload: ClassificationInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).save_classification(
            case_id,
            category_id=payload.category_id,
            subcategory_id=payload.subcategory_id,
            actor=payload.actor,
            synchronize=payload.synchronize,
        )
    except ReplyWorkspaceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/cases/{case_id}/classification/sync")
def sync_case_classification(
    case_id: uuid.UUID,
    actor: str = Query(default="web-operator", max_length=120),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).sync_classification(case_id, actor=actor)
    except ReplyWorkspaceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/classifications/bulk")
def bulk_classify(
    payload: BulkClassificationInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).bulk_classify(
            payload.case_ids,
            category_id=payload.category_id,
            subcategory_id=payload.subcategory_id,
            actor=payload.actor,
            synchronize=payload.synchronize,
        )
    except ReplyWorkspaceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.put("/cases/{case_id}/reply")
def save_case_reply(
    case_id: uuid.UUID,
    payload: ReplyInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).save_reply(
            case_id,
            inquiry_findings=payload.inquiry_findings,
            school_version=payload.school_version,
            applicable_policy=payload.applicable_policy,
            final_reply=payload.final_reply,
            reply_status=payload.reply_status,
            disposal_outcome=payload.disposal_outcome,
            ai_eligible=payload.ai_eligible,
            actor=payload.actor,
        )
    except ReplyNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ReplyValidationError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ReplyRemoteError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except ReplyWorkspaceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/cases/{case_id}/documents", status_code=201)
async def upload_case_document(
    case_id: uuid.UUID,
    file: UploadFile = File(...),
    role: str = Form(default="report"),
    actor: str = Form(default="web-operator"),
    dispatch_batch_id: uuid.UUID | None = Form(default=None),
    dispatch_item_id: uuid.UUID | None = Form(default=None),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        content = await file.read()
        return _service(session, settings).upload_case_document(
            case_id, filename=file.filename or "case-file", content=content,
            content_type=file.content_type, role=role, actor=actor,
            dispatch_batch_id=dispatch_batch_id, dispatch_item_id=dispatch_item_id,
        )
    except ReplyNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ReplyWorkspaceError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/documents/{document_id}/download")
def download_case_document(
    document_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    try:
        document, path = _service(session, settings).case_document_path(document_id)
        return FileResponse(
            path, media_type=document.mime_type or "application/octet-stream",
            filename=document.original_filename or path.name,
        )
    except ReplyNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/audit")
def reply_workspace_audit(
    case_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).audit(case_id=case_id, limit=limit)
