from __future__ import annotations

import hashlib
import shutil
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import FileResponse
from sqlmodel import Session, col, func, or_, select

from antidengue_automation.intake import ALLOWED_EXTENSIONS, safe_filename, validate_antidengue_report
from antidengue_automation.schemas import AntiDengueRunRequest
from antidengue_automation.tasks import run_antidengue_job
from automation_core.database import get_session
from automation_core.job_service import (
    append_log,
    create_job,
    get_active_job,
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
from automation_core.config import get_settings
from automation_core.time import utcnow

router = APIRouter(prefix="/api/v1/antidengue", tags=["antidengue"])


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
                "id": str(latest[1].id),
                "status": latest[1].status,
                "error": latest[1].error,
                "created_at": latest[1].created_at,
                "finished_at": latest[1].finished_at,
            }
            if latest
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
                "created_at": link.created_at,
                "finished_at": job.finished_at,
            }
            for link, job in attempts
        ]
    return data


def _count_files(folder: Path) -> int:
    if not folder.exists():
        return 0
    return sum(1 for path in folder.rglob("*") if path.is_file())


def _managed_job_file(path: Path) -> bool:
    settings = get_settings()
    roots = (
        settings.artifact_root.expanduser().resolve(),
        (settings.antidengue_root / "output-files").resolve(),
        (settings.antidengue_root / "unmapped-officer-reports").resolve(),
    )
    resolved = path.expanduser().resolve(strict=False)
    return any(resolved.is_relative_to(root) for root in roots)


def _remove_file_and_empty_parents(path: Path, stop_roots: tuple[Path, ...]) -> None:
    resolved = path.expanduser().resolve(strict=False)
    resolved.unlink(missing_ok=True)
    parent = resolved.parent
    normalized_roots = tuple(root.expanduser().resolve() for root in stop_roots)
    while parent not in normalized_roots and any(parent.is_relative_to(root) for root in normalized_roots):
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


@router.get("/overview")
def read_antidengue_overview(session: Session = Depends(get_session)) -> dict:
    root = get_settings().antidengue_root
    active_job = get_active_job(session, JobType.antidengue_report.value)
    return {
        "project_available": root.is_dir(),
        "active_job_id": str(active_job.id) if active_job else None,
        "counts": {
            "raw_files": _count_files(root / "drop-raw-files"),
            "output_files": _count_files(root / "output-files"),
            "archived_files": _count_files(root / "archived-files"),
            "unmapped_reports": _count_files(root / "unmapped-officer-reports"),
        },
    }


@router.get("/manual-reports")
def list_manual_reports(
    search: str = "",
    validation_status: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=10, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict:
    filters = [SourceFile.module_key == "antidengue", SourceFile.source_kind == "manual_upload"]
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
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/manual-reports/uploads", status_code=status.HTTP_201_CREATED)
async def upload_manual_report(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
) -> dict:
    settings = get_settings()
    original_name = Path(file.filename or "report").name
    extension = Path(original_name).suffix.lower()
    if extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=422, detail="Upload an .xls, .xlsx, or .csv portal report")

    source_id = uuid.uuid4()
    created_at = utcnow()
    destination_dir = (
        settings.source_file_root
        / "antidengue"
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
                    raise HTTPException(status_code=413, detail="The upload exceeds the 50 MB limit")
                digest.update(chunk)
                target.write(chunk)
        if size_bytes == 0:
            raise HTTPException(status_code=422, detail="The uploaded report is empty")
    except Exception:
        shutil.rmtree(destination_dir, ignore_errors=True)
        raise
    finally:
        await file.close()

    sha256 = digest.hexdigest()
    duplicate = session.scalar(
        select(SourceFile)
        .where(SourceFile.module_key == "antidengue", SourceFile.sha256 == sha256)
        .order_by(SourceFile.created_at)
        .limit(1)
    )
    if duplicate:
        shutil.rmtree(destination_dir, ignore_errors=True)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "This exact report was already uploaded. Use Reprocess on the existing record.",
                "source_file_id": str(duplicate.id),
            },
        )

    validation_status, schema_version, metadata, errors, warnings = validate_antidengue_report(destination)
    item = SourceFile(
        id=source_id,
        module_key="antidengue",
        source_kind="manual_upload",
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


@router.get("/manual-reports/{source_file_id}")
def manual_report(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "antidengue":
        raise HTTPException(status_code=404, detail="Manual report not found")
    return _source_file_dict(session, item, include_runs=True)


@router.get("/manual-reports/{source_file_id}/download")
def download_manual_report(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> FileResponse:
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "antidengue":
        raise HTTPException(status_code=404, detail="Manual report not found")
    path = Path(item.stored_path).resolve()
    if not path.is_relative_to(get_settings().source_file_root) or not path.is_file():
        raise HTTPException(status_code=404, detail="Stored report file is unavailable")
    return FileResponse(path, filename=item.original_name, media_type=item.content_type)


@router.delete("/manual-reports/{source_file_id}/hard")
def hard_delete_manual_report(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    """Permanently purge a manual source, its attempts, previews, logs and files."""
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "antidengue" or item.source_kind != "manual_upload":
        raise HTTPException(status_code=404, detail="Manual report not found")
    source_path = Path(item.stored_path).resolve(strict=False)

    links = session.scalars(
        select(SourceFileRun).where(SourceFileRun.source_file_id == source_file_id)
    ).all()
    jobs = [job for link in links if (job := session.get(Job, link.job_id)) is not None]
    active = [job for job in jobs if job.status in {JobStatus.queued.value, JobStatus.running.value}]
    if active:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot permanently delete while job {active[0].id} is {active[0].status}",
        )

    # Import locally so the AntiDengue package stays usable without the gateway runtime.
    from whatsapp_gateway.models import WhatsAppDispatchPreview
    from whatsapp_gateway.preview_service import (
        cleanup_unreferenced_preview_files,
        delete_preview_records,
    )

    preview_files: set[Path] = set()
    generated_files: set[Path] = set()
    for job in jobs:
        previews = session.scalars(
            select(WhatsAppDispatchPreview).where(WhatsAppDispatchPreview.source_job_id == job.id)
        ).all()
        for preview in previews:
            preview_files.update(delete_preview_records(session, preview))
        for artifact in session.scalars(select(Artifact).where(Artifact.job_id == job.id)).all():
            path = Path(artifact.path)
            if _managed_job_file(path):
                generated_files.add(path)
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
    session.flush()
    session.delete(item)
    session.commit()

    cleanup_unreferenced_preview_files(session, preview_files)
    settings = get_settings()
    job_roots = (
        settings.artifact_root.resolve(),
        (settings.antidengue_root / "output-files").resolve(),
        (settings.antidengue_root / "unmapped-officer-reports").resolve(),
    )
    for path in generated_files:
        _remove_file_and_empty_parents(path, job_roots)
    if source_path.is_relative_to(settings.source_file_root):
        _remove_file_and_empty_parents(source_path, (settings.source_file_root,))
    return {
        "id": str(source_file_id),
        "deleted": True,
        "jobs_deleted": len(jobs),
        "generated_files_deleted": len(generated_files),
    }


@router.post(
    "/manual-reports/{source_file_id}/process",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def process_manual_report(
    source_file_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> JobPublic:
    item = session.get(SourceFile, source_file_id)
    if item is None or item.module_key != "antidengue":
        raise HTTPException(status_code=404, detail="Manual report not found")
    if item.validation_status != "valid":
        raise HTTPException(status_code=422, detail="Only a valid manual report can be processed")
    source_path = Path(item.stored_path).resolve()
    if not source_path.is_relative_to(get_settings().source_file_root) or not source_path.is_file():
        raise HTTPException(status_code=422, detail="The immutable source file is unavailable")
    active_job = get_active_job(session, JobType.antidengue_report.value)
    if active_job is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"AntiDengue job {active_job.id} is already {active_job.status}",
        )

    job = create_job(
        session,
        job_type=JobType.antidengue_report.value,
        title=f"Manual AntiDengue report: {item.original_name}",
        parameters={
            "dry_run": True,
            "input_source": "manual_upload",
            "source_file_id": str(item.id),
            "source_filename": item.original_name,
        },
    )
    session.add(SourceFileRun(source_file_id=item.id, job_id=job.id))
    session.commit()
    try:
        task = run_antidengue_job.apply_async(args=[str(job.id)], queue="antidengue")
    except Exception as exc:
        append_log(session, job.id, f"Queue unavailable: {exc}", level="error")
        mark_job_failed(session, job.id, f"Queue unavailable: {exc}")
        raise HTTPException(status_code=503, detail="Queue unavailable") from exc
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, f"Queued immutable manual report {item.original_name} for dry-run processing.")
    queued = get_job(session, job.id)
    assert queued is not None
    return queued


@router.post(
    "/runs",
    response_model=JobPublic,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_antidengue_run(
    request: AntiDengueRunRequest,
    session: Session = Depends(get_session),
) -> JobPublic:
    active_job = get_active_job(session, JobType.antidengue_report.value)
    if active_job is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"AntiDengue job {active_job.id} is already {active_job.status}",
        )

    parameters = request.model_dump()
    mode_label = "dry run" if request.dry_run else "live send"
    job = create_job(
        session,
        job_type=JobType.antidengue_report.value,
        title=f"AntiDengue report ({mode_label})",
        parameters=parameters,
    )
    try:
        task = run_antidengue_job.apply_async(args=[str(job.id)], queue="antidengue")
    except Exception as exc:
        append_log(session, job.id, f"Queue unavailable: {exc}", level="error")
        mark_job_failed(session, job.id, f"Queue unavailable: {exc}")
        raise HTTPException(status_code=503, detail="Queue unavailable") from exc

    set_task_id(session, job.id, task.id)
    append_log(session, job.id, f"Queued AntiDengue {mode_label}.")
    queued = get_job(session, job.id)
    assert queued is not None
    return queued
