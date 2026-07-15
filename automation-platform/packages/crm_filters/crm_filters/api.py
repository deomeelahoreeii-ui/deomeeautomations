from __future__ import annotations

import hashlib
import shutil
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlmodel import Session, col, func, or_, select

from automation_core.config import get_settings
from automation_core.database import get_session
from automation_core.job_service import (
    append_log,
    create_job,
    get_job,
    mark_job_failed,
    set_task_id,
)
from automation_core.models import (
    Artifact,
    Job,
    JobLog,
    JobPublic,
    JobStatus,
    JobType,
    SourceFile,
    SourceFileRun,
)
from automation_core.time import utcnow
from crm_filters.intake import ALLOWED_EXTENSIONS, safe_filename, validate_crm_sheet
from crm_filters.schemas import PdfFilterJobRequest, SheetFilterJobRequest
from crm_filters.tasks import run_pdf_filter_job, run_sheet_filter_job

router = APIRouter(prefix="/api/v1/crm", tags=["crm"])


def _latest_source_run(session: Session, source_file_id: uuid.UUID) -> tuple[SourceFileRun, Job] | None:
    return session.execute(
        select(SourceFileRun, Job)
        .join(Job, Job.id == SourceFileRun.job_id)
        .where(SourceFileRun.source_file_id == source_file_id)
        .order_by(col(SourceFileRun.created_at).desc())
        .limit(1)
    ).first()


def _source_file_dict(session: Session, item: SourceFile, *, include_runs: bool = False) -> dict:
    latest = _latest_source_run(session, item.id)
    latest_job = latest[1] if latest else None
    data = {
        "id": str(item.id),
        "module_key": item.module_key,
        "source_kind": item.source_kind,
        "original_name": item.original_name,
        "content_type": item.content_type,
        "extension": item.extension,
        "size_bytes": item.size_bytes,
        "sha256": item.sha256,
        "validation_status": item.validation_status,
        "schema_version": item.schema_version,
        "detected_metadata": item.detected_metadata,
        "validation_errors": item.validation_errors,
        "validation_warnings": item.validation_warnings,
        "duplicate_of_id": str(item.duplicate_of_id) if item.duplicate_of_id else None,
        "created_at": item.created_at,
        "latest_job": (
            {
                "id": str(latest_job.id),
                "status": latest_job.status,
                "error": latest_job.error,
                "result": latest_job.result,
                "created_at": latest_job.created_at,
                "finished_at": latest_job.finished_at,
            }
            if latest_job
            else None
        ),
    }
    if include_runs:
        attempts = session.execute(
            select(SourceFileRun, Job)
            .join(Job, Job.id == SourceFileRun.job_id)
            .where(SourceFileRun.source_file_id == item.id)
            .order_by(col(SourceFileRun.created_at).desc())
        ).all()
        data["processing_runs"] = [
            {
                "id": str(link.id),
                "job_id": str(job.id),
                "status": job.status,
                "error": job.error,
                "result": job.result,
                "created_at": link.created_at,
                "finished_at": job.finished_at,
            }
            for link, job in attempts
        ]
    return data


def _paperless_configured() -> bool:
    settings = get_settings()
    return bool(
        settings.paperless_url
        and (
            settings.paperless_token
            or (settings.paperless_username and settings.paperless_password)
        )
    )


def _remove_file_and_empty_parents(path: Path, root: Path) -> None:
    resolved = path.expanduser().resolve(strict=False)
    normalized_root = root.expanduser().resolve()
    if not resolved.is_relative_to(normalized_root):
        return
    resolved.unlink(missing_ok=True)
    parent = resolved.parent
    while parent != normalized_root and parent.is_relative_to(normalized_root):
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def _queue_sheet_job(session: Session, item: SourceFile) -> Job:
    if item.validation_status != "valid":
        raise HTTPException(status_code=422, detail="Only a valid CRM sheet can be processed.")
    source_path = Path(item.stored_path).resolve()
    if not source_path.is_relative_to(get_settings().source_file_root) or not source_path.is_file():
        raise HTTPException(status_code=422, detail="The immutable source file is unavailable.")
    if not _paperless_configured():
        raise HTTPException(
            status_code=422,
            detail="Paperless credentials are not configured. Set PAPERLESS_TOKEN or username/password.",
        )

    active = session.execute(
        select(SourceFileRun, Job)
        .join(Job, Job.id == SourceFileRun.job_id)
        .where(SourceFileRun.source_file_id == item.id)
        .where(Job.status.in_([JobStatus.queued.value, JobStatus.running.value]))
        .limit(1)
    ).first()
    if active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"This CRM sheet already has a {active[1].status} job: {active[1].id}",
        )

    job = create_job(
        session,
        job_type=JobType.crm_sheet_filter.value,
        title=f"CRM sheet filter: {item.original_name}",
        parameters={
            "source_file_id": str(item.id),
            "source_filename": item.original_name,
            "input_source": "manual_upload",
        },
    )
    session.add(SourceFileRun(source_file_id=item.id, job_id=job.id))
    session.commit()
    try:
        task = run_sheet_filter_job.apply_async(args=[str(job.id)], queue="crm")
    except Exception as exc:
        append_log(session, job.id, f"Queue unavailable: {exc}", level="error")
        mark_job_failed(session, job.id, f"Queue unavailable: {exc}")
        raise HTTPException(status_code=503, detail="CRM worker queue is unavailable.") from exc
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, "Queued native CRM sheet filter job on the crm worker queue.")
    queued = get_job(session, job.id)
    assert queued is not None
    return queued


@router.get("/overview")
def read_crm_overview(session: Session = Depends(get_session)) -> dict:
    settings = get_settings()
    total_uploads = session.scalar(
        select(func.count()).select_from(SourceFile).where(SourceFile.module_key == "crm")
    ) or 0
    active_jobs = session.scalar(
        select(func.count())
        .select_from(Job)
        .where(Job.type == JobType.crm_sheet_filter.value)
        .where(Job.status.in_([JobStatus.queued.value, JobStatus.running.value]))
    ) or 0
    return {
        "paperless": {
            "url": settings.paperless_url,
            "configured": _paperless_configured(),
            "verify_ssl": settings.paperless_verify_ssl,
            "authentication": "token" if settings.paperless_token else "username_password",
        },
        "counts": {"uploads": int(total_uploads), "active_jobs": int(active_jobs)},
    }


@router.get("/sheets")
def list_crm_sheets(
    search: str = "",
    validation_status: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict:
    filters = [SourceFile.module_key == "crm", SourceFile.source_kind == "crm_sheet_upload"]
    if search.strip():
        term = f"%{search.strip()}%"
        filters.append(or_(SourceFile.original_name.ilike(term), SourceFile.sha256.ilike(term)))
    if validation_status:
        filters.append(SourceFile.validation_status == validation_status)
    total = session.scalar(select(func.count()).select_from(SourceFile).where(*filters)) or 0
    items = session.scalars(
        select(SourceFile)
        .where(*filters)
        .order_by(col(SourceFile.created_at).desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    return {
        "items": [_source_file_dict(session, item) for item in items],
        "total": int(total),
        "page": page,
        "page_size": page_size,
    }


@router.post("/sheets/uploads", status_code=status.HTTP_201_CREATED)
async def upload_crm_sheet(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
) -> dict:
    settings = get_settings()
    original_name = Path(file.filename or "crm-sheet").name
    extension = Path(original_name).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=422,
            detail="Upload an .xlsx, .xls, .xlsm, or .csv CRM sheet.",
        )

    source_id = uuid.uuid4()
    created_at = utcnow()
    destination_dir = (
        settings.source_file_root
        / "crm"
        / created_at.strftime("%Y")
        / created_at.strftime("%m")
        / str(source_id)
    )
    destination_dir.mkdir(parents=True, exist_ok=False)
    destination = destination_dir / safe_filename(original_name)
    digest = hashlib.sha256()
    size_bytes = 0
    try:
        with destination.open("wb") as target:
            while chunk := await file.read(1024 * 1024):
                size_bytes += len(chunk)
                if size_bytes > settings.source_file_max_bytes:
                    raise HTTPException(status_code=413, detail="The upload exceeds the configured size limit.")
                digest.update(chunk)
                target.write(chunk)
        if size_bytes == 0:
            raise HTTPException(status_code=422, detail="The uploaded CRM sheet is empty.")
    except Exception:
        shutil.rmtree(destination_dir, ignore_errors=True)
        raise
    finally:
        await file.close()

    sha256 = digest.hexdigest()
    duplicate = session.scalar(
        select(SourceFile)
        .where(SourceFile.module_key == "crm", SourceFile.sha256 == sha256)
        .order_by(SourceFile.created_at)
        .limit(1)
    )
    if duplicate:
        shutil.rmtree(destination_dir, ignore_errors=True)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "This exact CRM sheet was already uploaded. Use Reprocess on the existing record.",
                "source_file_id": str(duplicate.id),
            },
        )

    validation_status, schema_version, metadata, errors, warnings = validate_crm_sheet(destination)
    item = SourceFile(
        id=source_id,
        module_key="crm",
        source_kind="crm_sheet_upload",
        original_name=original_name,
        stored_path=str(destination.resolve()),
        content_type=file.content_type,
        extension=extension,
        size_bytes=size_bytes,
        sha256=sha256,
        validation_status=validation_status,
        schema_version=schema_version,
        detected_metadata=metadata,
        validation_errors=errors,
        validation_warnings=warnings,
        created_at=created_at,
    )
    session.add(item)
    session.commit()
    session.refresh(item)
    return _source_file_dict(session, item, include_runs=True)


@router.get("/sheets/{source_file_id}")
def read_crm_sheet(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "crm":
        raise HTTPException(status_code=404, detail="CRM sheet not found.")
    return _source_file_dict(session, item, include_runs=True)


@router.get("/sheets/{source_file_id}/download")
def download_crm_sheet(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> FileResponse:
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "crm":
        raise HTTPException(status_code=404, detail="CRM sheet not found.")
    path = Path(item.stored_path).resolve()
    if not path.is_relative_to(get_settings().source_file_root) or not path.is_file():
        raise HTTPException(status_code=404, detail="Stored CRM sheet is unavailable.")
    return FileResponse(path, filename=item.original_name, media_type=item.content_type)


@router.post(
    "/sheets/{source_file_id}/process",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def process_crm_sheet(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> JobPublic:
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "crm":
        raise HTTPException(status_code=404, detail="CRM sheet not found.")
    return _queue_sheet_job(session, item)


@router.delete("/sheets/{source_file_id}/hard")
def hard_delete_crm_sheet(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "crm":
        raise HTTPException(status_code=404, detail="CRM sheet not found.")

    links = session.scalars(
        select(SourceFileRun).where(SourceFileRun.source_file_id == source_file_id)
    ).all()
    jobs = [job for link in links if (job := session.get(Job, link.job_id)) is not None]
    active = [job for job in jobs if job.status in {JobStatus.queued.value, JobStatus.running.value}]
    if active:
        raise HTTPException(
            status_code=409,
            detail=f"Cannot delete while job {active[0].id} is {active[0].status}.",
        )

    settings = get_settings()
    generated_files: list[Path] = []
    job_output_dirs = [settings.crm_filter_artifact_root / str(job.id) for job in jobs]
    for job in jobs:
        for artifact in session.scalars(select(Artifact).where(Artifact.job_id == job.id)).all():
            generated_files.append(Path(artifact.path))
            session.delete(artifact)
        for log in session.scalars(select(JobLog).where(JobLog.job_id == job.id)).all():
            session.delete(log)
    session.flush()
    for link in links:
        session.delete(link)
    session.flush()
    for job in jobs:
        session.delete(job)
    for duplicate in session.scalars(
        select(SourceFile).where(SourceFile.duplicate_of_id == source_file_id)
    ).all():
        duplicate.duplicate_of_id = None
        session.add(duplicate)
    source_path = Path(item.stored_path)
    session.delete(item)
    session.commit()

    for path in generated_files:
        _remove_file_and_empty_parents(path, settings.crm_filter_artifact_root)
    for directory in job_output_dirs:
        resolved = directory.resolve(strict=False)
        if resolved.is_relative_to(settings.crm_filter_artifact_root.resolve()):
            shutil.rmtree(resolved, ignore_errors=True)
    _remove_file_and_empty_parents(source_path, settings.source_file_root)
    return {
        "id": str(source_file_id),
        "deleted": True,
        "jobs_deleted": len(jobs),
        "generated_files_deleted": len(generated_files),
    }


# Compatibility endpoint retained for API clients while switching from paths to source IDs.
@router.post(
    "/filters/sheets/jobs",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_sheet_filter_job(
    request: SheetFilterJobRequest,
    session: Session = Depends(get_session),
) -> JobPublic:
    item = session.get(SourceFile, request.source_file_id)
    if item is None or item.module_key != "crm":
        raise HTTPException(status_code=404, detail="CRM sheet not found.")
    return _queue_sheet_job(session, item)


@router.post(
    "/filters/pdfs/jobs",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_pdf_filter_job(
    request: PdfFilterJobRequest,
    session: Session = Depends(get_session),
) -> JobPublic:
    parameters = request.model_dump(exclude_none=True)
    job = create_job(
        session,
        job_type=JobType.crm_pdf_filter.value,
        title="CRM PDF duplicate filter",
        parameters=parameters,
    )
    try:
        task = run_pdf_filter_job.apply_async(args=[str(job.id)], queue="crm")
    except Exception as exc:
        append_log(session, job.id, f"Queue unavailable: {exc}", level="error")
        mark_job_failed(session, job.id, f"Queue unavailable: {exc}")
        raise HTTPException(status_code=503, detail="CRM worker queue is unavailable.") from exc
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, "Queued legacy CRM PDF filter job.")
    queued = get_job(session, job.id)
    assert queued is not None
    return queued
