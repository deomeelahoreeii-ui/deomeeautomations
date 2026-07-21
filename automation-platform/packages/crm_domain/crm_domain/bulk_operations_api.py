from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel, Field
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from crm_domain.bulk_operations import (
    BulkOperationError,
    BulkOperationNotFound,
    BulkOperationValidationError,
    CrmBulkOperationService,
    reference_file,
)


router = APIRouter(
    prefix="/api/v1/crm/bulk-operations",
    tags=["crm-bulk-operations"],
)
MAX_REPLY_FILE_BYTES = 5 * 1024 * 1024


class ExportBatchInput(BaseModel):
    scope: str = Field(default="awaiting", pattern="^(awaiting|all|selected)$")
    case_ids: list[uuid.UUID] = Field(default_factory=list, max_length=1000)
    actor: str = Field(default="web-operator", max_length=120)


class CommitImportInput(BaseModel):
    allow_partial: bool = False
    actor: str = Field(default="web-operator", max_length=120)


class LetterBatchInput(BaseModel):
    parent_batch_id: uuid.UUID | None = None
    scope: str = Field(default="ready", pattern="^(ready|all_imported|selected|import_batch)$")
    case_ids: list[uuid.UUID] = Field(default_factory=list, max_length=1000)
    actor: str = Field(default="web-operator", max_length=120)


class ActorInput(BaseModel):
    actor: str = Field(default="web-operator", max_length=120)


def _service(session: Session, settings: Settings) -> CrmBulkOperationService:
    return CrmBulkOperationService(session, settings)


def _raise(exc: BulkOperationError) -> None:
    if isinstance(exc, BulkOperationNotFound):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, BulkOperationValidationError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/statistics")
def statistics(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).statistics()


@router.get("/batches")
def list_batches(
    operation_type: str = Query(default="", max_length=32),
    status: str = Query(default="", max_length=32),
    search: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    return _service(session, settings).list_batches(
        operation_type=operation_type,
        status=status,
        search=search,
        page=page,
        page_size=page_size,
    )


@router.post("/export-batches")
def create_export_batch(
    payload: ExportBatchInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_export_batch(
            scope=payload.scope,
            case_ids=payload.case_ids,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/import-batches/validate")
async def validate_import_batch(
    file: UploadFile = File(...),
    parent_batch_id: uuid.UUID | None = Form(default=None),
    actor: str = Form(default="web-operator", max_length=120),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    filename = file.filename or "replies.csv"
    if not filename.casefold().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Upload a UTF-8 CSV reply file.")
    content = await file.read(MAX_REPLY_FILE_BYTES + 1)
    await file.close()
    if len(content) > MAX_REPLY_FILE_BYTES:
        raise HTTPException(status_code=413, detail="The reply CSV exceeds 5 MB.")
    try:
        return _service(session, settings).validate_import_batch(
            content=content,
            filename=filename,
            parent_batch_id=parent_batch_id,
            actor=actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/import-batches/{batch_id}/commit")
def commit_import_batch(
    batch_id: uuid.UUID,
    payload: CommitImportInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).commit_import_batch(
            batch_id,
            allow_partial=payload.allow_partial,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/letter-batches")
def create_letter_batch(
    payload: LetterBatchInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).create_letter_batch(
            parent_batch_id=payload.parent_batch_id,
            scope=payload.scope,
            case_ids=payload.case_ids,
            actor=payload.actor,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/batches/{batch_id}")
def batch_detail(
    batch_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).batch_detail(batch_id)
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/batches/{batch_id}/items")
def batch_items(
    batch_id: uuid.UUID,
    status: str = Query(default="", max_length=32),
    search: str = Query(default="", max_length=200),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).list_items(
            batch_id,
            status=status,
            search=search,
            page=page,
            page_size=page_size,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/batches/{batch_id}/retry")
def retry_batch(
    batch_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).retry_batch(batch_id, actor=payload.actor)
    except BulkOperationError as exc:
        _raise(exc)


@router.post("/batches/{batch_id}/cancel")
def cancel_batch(
    batch_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).cancel_batch(batch_id, actor=payload.actor)
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(
    artifact_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    try:
        artifact, path = _service(session, settings).artifact_path(artifact_id)
        return FileResponse(
            path,
            media_type=artifact.content_type,
            filename=artifact.name,
        )
    except BulkOperationError as exc:
        _raise(exc)


@router.get("/reference/{kind}")
def download_reference(kind: str) -> Response:
    try:
        name, content_type, content = reference_file(kind)
        return Response(
            content=content,
            media_type=content_type,
            headers={"Content-Disposition": f'attachment; filename="{name}"'},
        )
    except BulkOperationError as exc:
        _raise(exc)
