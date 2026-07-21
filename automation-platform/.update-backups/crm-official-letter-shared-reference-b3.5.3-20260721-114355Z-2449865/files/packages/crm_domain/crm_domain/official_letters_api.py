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
from crm_domain.official_letters import (
    OfficialLetterError,
    OfficialLetterNotFound,
    OfficialLetterService,
    OfficialLetterValidationError,
)

router = APIRouter(prefix="/api/v1/crm/official-letters", tags=["crm-official-letters"])


class ConfigurationInput(BaseModel):
    office_title: str = Field(max_length=2_000)
    default_recipient_name: str = Field(max_length=240)
    default_recipient_location: str = Field(max_length=160)
    default_subject_prefix: str = Field(max_length=160)
    default_cc_entries: str = Field(max_length=5_000)
    date_format: str = Field(default="DD/MM/YYYY", max_length=40)
    numbering_prefix: str = Field(default="PMDU/CRM", max_length=120)
    last_numeric_number: int = Field(default=1510, ge=0)
    allow_manual_override: bool = True
    require_unique_number: bool = True
    default_template_id: uuid.UUID | None = None
    default_signature_id: uuid.UUID | None = None
    actor: str = Field(default="web-operator", max_length=120)


class ActiveInput(BaseModel):
    active: bool
    actor: str = Field(default="web-operator", max_length=120)


class GenerateInput(BaseModel):
    letter_number: str = Field(max_length=180)
    letter_date: date
    recipient_name: str = Field(max_length=240)
    recipient_location: str = Field(max_length=160)
    subject_prefix: str = Field(default="COMPLAINT NO.", max_length=160)
    cc_entries: str = Field(default="Office Record.", max_length=5_000)
    template_id: uuid.UUID | None = None
    signature_profile_id: uuid.UUID | None = None
    reply_revision_id: uuid.UUID | None = None
    letter_id: uuid.UUID | None = None
    supersedes_letter_id: uuid.UUID | None = None
    finalize: bool = False
    actor: str = Field(default="web-operator", max_length=120)


class ActorInput(BaseModel):
    actor: str = Field(default="web-operator", max_length=120)


def _service(session: Session, settings: Settings) -> OfficialLetterService:
    return OfficialLetterService(session, settings)


def _raise(exc: OfficialLetterError) -> None:
    if isinstance(exc, OfficialLetterNotFound):
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if isinstance(exc, OfficialLetterValidationError):
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/configuration")
def configuration(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).configuration()
    except OfficialLetterError as exc:
        _raise(exc)


@router.put("/configuration")
def update_configuration(
    payload: ConfigurationInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        values = payload.model_dump(exclude={"actor"})
        return _service(session, settings).update_configuration(values, actor=payload.actor)
    except OfficialLetterError as exc:
        _raise(exc)


@router.post("/templates")
async def upload_template(
    file: UploadFile = File(...),
    name: str = Form(default="DEO CRM Official Letter"),
    description: str = Form(default=""),
    make_default: bool = Form(default=True),
    actor: str = Form(default="web-operator"),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    content = await file.read(20 * 1024 * 1024 + 1)
    await file.close()
    try:
        return _service(session, settings).upload_template(
            content,
            filename=file.filename or "official-letter.odt",
            name=name,
            description=description or None,
            actor=actor,
            make_default=make_default,
        )
    except OfficialLetterError as exc:
        _raise(exc)


@router.post("/signatures")
async def upload_signature(
    file: UploadFile = File(...),
    name: str = Form(default="DEO M-EE Lahore"),
    officer_name: str = Form(default=""),
    designation: str = Form(default="DISTRICT EDUCATION OFFICER (M-EE)"),
    office_location: str = Form(default="LAHORE"),
    effective_from: date | None = Form(default=None),
    effective_to: date | None = Form(default=None),
    make_default: bool = Form(default=True),
    actor: str = Form(default="web-operator"),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    content = await file.read(5 * 1024 * 1024 + 1)
    await file.close()
    try:
        return _service(session, settings).upload_signature(
            content,
            filename=file.filename or "signature.png",
            name=name,
            officer_name=officer_name or None,
            designation=designation,
            office_location=office_location,
            effective_from=effective_from,
            effective_to=effective_to,
            actor=actor,
            make_default=make_default,
        )
    except OfficialLetterError as exc:
        _raise(exc)


@router.put("/templates/{template_id}/active")
def set_template_active(
    template_id: uuid.UUID,
    payload: ActiveInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).set_template_active(
            template_id, active=payload.active, actor=payload.actor
        )
    except OfficialLetterError as exc:
        _raise(exc)


@router.put("/signatures/{signature_id}/active")
def set_signature_active(
    signature_id: uuid.UUID,
    payload: ActiveInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).set_signature_active(
            signature_id, active=payload.active, actor=payload.actor
        )
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/templates/{template_id}/download")
def download_template(
    template_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    try:
        record, path = _service(session, settings).template_path(template_id)
        return FileResponse(path, media_type="application/vnd.oasis.opendocument.text", filename=f"{record.name}-v{record.version}.odt")
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/signatures/{signature_id}/image")
def signature_image(
    signature_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    try:
        _record, path = _service(session, settings).signature_path(signature_id)
        media = "image/png" if path.suffix.casefold() == ".png" else "image/jpeg"
        return FileResponse(path, media_type=media)
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/cases/{case_id}/prepare")
def prepare_case(
    case_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).prepare_case(case_id)
    except OfficialLetterError as exc:
        _raise(exc)


@router.post("/cases/{case_id}/generate")
def generate_letter(
    case_id: uuid.UUID,
    payload: GenerateInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).generate(
            case_id,
            letter_number=payload.letter_number,
            letter_date=payload.letter_date,
            recipient_name=payload.recipient_name,
            recipient_location=payload.recipient_location,
            subject_prefix=payload.subject_prefix,
            cc_entries=payload.cc_entries,
            template_id=payload.template_id,
            signature_profile_id=payload.signature_profile_id,
            reply_revision_id=payload.reply_revision_id,
            actor=payload.actor,
            finalize=payload.finalize,
            letter_id=payload.letter_id,
            supersedes_letter_id=payload.supersedes_letter_id,
        )
    except OfficialLetterError as exc:
        _raise(exc)


@router.post("/letters/{letter_id}/complete-pdf")
def create_complete_pdf(
    letter_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).compose_complete_pdf(
            letter_id, actor=payload.actor
        )
    except OfficialLetterError as exc:
        _raise(exc)


@router.post("/letters/{letter_id}/finalize")
def finalize_letter(
    letter_id: uuid.UUID,
    payload: ActorInput,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).finalize(letter_id, actor=payload.actor)
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/statistics")
def statistics(
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, int]:
    try:
        return _service(session, settings).statistics()
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/letters")
def list_letters(
    status: str = Query(default="", max_length=24),
    search: str = Query(default="", max_length=300),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=200),
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).list_letters(
            status=status, search=search, page=page, page_size=page_size
        )
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/letters/{letter_id}")
def letter_detail(
    letter_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    try:
        return _service(session, settings).detail(letter_id)
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/artifacts/{artifact_id}/download")
def download_artifact(
    artifact_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    try:
        record, path = _service(session, settings).artifact_path(artifact_id)
        return FileResponse(path, media_type=record.content_type, filename=record.name)
    except OfficialLetterError as exc:
        _raise(exc)


@router.get("/artifacts/{artifact_id}/preview")
def preview_artifact(
    artifact_id: uuid.UUID,
    session: Session = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> FileResponse:
    try:
        record, path = _service(session, settings).artifact_path(artifact_id)
        if record.kind not in {"pdf", "complete_pdf"}:
            raise OfficialLetterValidationError("Only PDF artifacts can be previewed")
        return FileResponse(path, media_type="application/pdf", headers={"Content-Disposition": "inline"})
    except OfficialLetterError as exc:
        _raise(exc)
