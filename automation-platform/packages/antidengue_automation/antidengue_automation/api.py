from __future__ import annotations

import asyncio
import hashlib
import json
import shutil
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlmodel import Session, col, func, or_, select

from antidengue_automation.intake import ALLOWED_EXTENSIONS, safe_filename, validate_antidengue_report
from antidengue_automation.models import (
    AntiDengueSchedule,
    AntiDengueScheduleEvent,
    AntiDengueScheduleExecution,
)
from antidengue_automation.schemas import (
    AntiDengueDeadlinePolicyUpdate,
    AntiDengueExecutionCreate,
    AntiDengueRunNowRequest,
    AntiDengueRunRequest,
    AntiDengueScheduleCreate,
    AntiDengueScheduleUpdate,
)
from antidengue_automation.deadline_policy import (
    get_or_create_deadline_policy,
    normalize_deadline,
    policy_dict,
    update_deadline_policy,
)
from antidengue_automation.scheduling import (
    ACTIVE_EXECUTION_STATUSES,
    TERMINAL_EXECUTION_STATUSES,
    advance_execution,
    cancel_execution,
    combined_execution_logs,
    create_execution,
    execution_stage_summary,
    find_equivalent_active_execution,
    lock_execution_scope,
    next_occurrence_after,
    normalize_dispatch_profile_ids,
    normalize_times,
    normalize_weekdays,
    schedule_spec,
    validate_dispatch_profiles,
    validate_recurrence,
)
from antidengue_automation.tasks import run_antidengue_job
from antidengue_automation.storage_lifecycle import (
    antidengue_storage_counts,
    evict_verified_antidengue_cache,
)
from antidengue_automation.notifications import NOTIFICATION_EVENT_TYPES
from automation_core.database import get_session, session_scope
from automation_core.job_service import (
    add_job,
    add_job_log,
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
from automation_core.storage_catalog import archive_source_file, ensure_source_file_local
from automation_core.config import get_settings
from automation_core.notifications import ntfy_health
from automation_core.task_outbox import publish_pending_tasks, stage_task
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppActivity,
    WhatsAppDelivery,
    WhatsAppDispatchApproval,
    WhatsAppDispatchPreview,
    WhatsAppDispatchProfile,
)

router = APIRouter(prefix="/api/v1/antidengue", tags=["antidengue"])


class AntiDengueRetentionInput(BaseModel):
    apply: bool = False
    older_than_hours: int | None = Field(default=None, ge=0, le=24 * 365)
    limit: int = Field(default=5000, ge=1, le=20000)


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
        "storage_status": item.storage_status,
        "storage_error": item.storage_error,
        "stored_object_id": str(item.stored_object_id) if item.stored_object_id else None,
        "archived_at": item.archived_at,
        "local_evicted_at": item.local_evicted_at,
        "last_hydrated_at": item.last_hydrated_at,
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
    storage_counts = antidengue_storage_counts(session)
    return {
        "project_available": root.is_dir(),
        "active_job_id": str(active_job.id) if active_job else None,
        "counts": {
            "raw_files": storage_counts["raw_files"],
            "output_files": storage_counts["output_files"],
            "archived_files": storage_counts["rustfs_ready"],
            "unmapped_reports": session.scalar(
                select(func.count())
                .select_from(Artifact)
                .where(
                    Artifact.module_key == "antidengue",
                    col(Artifact.name).ilike("%officer mapping audit%"),
                )
            )
            or 0,
        },
        "storage": storage_counts,
    }


@router.post("/storage/retention")
def apply_antidengue_storage_retention(
    data: AntiDengueRetentionInput,
    session: Session = Depends(get_session),
) -> dict:
    result = evict_verified_antidengue_cache(
        session,
        settings=get_settings(),
        older_than_hours=data.older_than_hours,
        apply=data.apply,
        limit=data.limit,
    )
    return {"dry_run": not data.apply, **result}


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
    archive_source_file(session, item)
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
    try:
        path = ensure_source_file_local(session, item)
        session.commit()
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Stored report file is unavailable: {exc}") from exc
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
    try:
        source_path = ensure_source_file_local(session, item)
        session.commit()
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail=f"The immutable source file is unavailable: {exc}",
        ) from exc
    if not source_path.is_relative_to(get_settings().source_file_root):
        raise HTTPException(status_code=422, detail="The immutable source file is unavailable")
    active_job = get_active_job(session, JobType.antidengue_report.value)
    if active_job is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"AntiDengue job {active_job.id} is already {active_job.status}",
        )

    job = add_job(
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
    add_job_log(session, job.id, f"Staged immutable manual report {item.original_name} for dry-run processing.")
    stage_task(
        session, job=job, task_name="antidengue_automation.run_report",
        queue="antidengue", args=[str(job.id)], idempotency_key=f"manual-report:{job.id}",
    )
    session.commit()
    publish_pending_tasks(session, limit=10)
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
    if not request.dry_run:
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=(
                "Direct AntiDengue live mode is retired. Use Send now so report generation "
                "creates an exact send plan before any WhatsApp dispatch."
            ),
        )
    active_job = get_active_job(session, JobType.antidengue_report.value)
    if active_job is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"AntiDengue job {active_job.id} is already {active_job.status}",
        )

    parameters = request.model_dump()
    parameters["legacy_direct_live_disabled"] = True
    mode_label = "dry run"
    job = add_job(
        session,
        job_type=JobType.antidengue_report.value,
        title=f"AntiDengue report ({mode_label})",
        parameters=parameters,
    )
    add_job_log(session, job.id, f"Staged AntiDengue {mode_label} in the durable task outbox.")
    stage_task(
        session, job=job, task_name="antidengue_automation.run_report",
        queue="antidengue", args=[str(job.id)], idempotency_key=f"legacy-dry-run:{job.id}",
    )
    session.commit()
    publish_pending_tasks(session, limit=10)
    queued = get_job(session, job.id)
    assert queued is not None
    return queued


# ---------------------------------------------------------------------------
# Server-owned AntiDengue schedules and safe preview/send orchestration
# ---------------------------------------------------------------------------


def _schedule_dict(item: AntiDengueSchedule) -> dict:
    profile_ids = item.dispatch_profile_ids or [str(item.dispatch_profile_id)]
    return {
        "id": str(item.id),
        "name": item.name,
        "enabled": item.enabled,
        "recurrence_type": item.recurrence_type,
        "run_date": item.run_date,
        "weekdays": item.weekdays,
        "times": item.times,
        "timezone": item.timezone,
        "submission_deadline_override": item.submission_deadline_override,
        "login_mode": item.login_mode,
        "dispatch_policy": item.dispatch_policy,
        "dispatch_profile_id": str(item.dispatch_profile_id),
        "dispatch_profile_ids": profile_ids,
        "missed_run_grace_minutes": item.missed_run_grace_minutes,
        "overlap_grace_minutes": item.overlap_grace_minutes,
        "next_run_at": item.next_run_at,
        "last_run_at": item.last_run_at,
        "last_run_status": item.last_run_status,
        "archived_at": item.archived_at,
        "created_by": item.created_by,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
    }


def _execution_dict(
    session: Session,
    item: AntiDengueScheduleExecution,
    *,
    detail: bool = False,
) -> dict:
    schedule = session.get(AntiDengueSchedule, item.schedule_id) if item.schedule_id else None
    preview = session.get(WhatsAppDispatchPreview, item.preview_id) if item.preview_id else None
    profile_ids = item.dispatch_profile_ids or [str(item.dispatch_profile_id)]
    data = {
        "id": str(item.id),
        "execution_key": item.execution_key,
        "execution_code": item.execution_code,
        "schedule_id": str(item.schedule_id) if item.schedule_id else None,
        "schedule_name": schedule.name if schedule else None,
        "trigger_type": item.trigger_type,
        "scheduled_for": item.scheduled_for,
        "status": item.status,
        "dispatch_policy": item.dispatch_policy,
        "login_mode": item.login_mode,
        "dispatch_profile_id": str(item.dispatch_profile_id),
        "dispatch_profile_ids": profile_ids,
        "submission_deadline": item.submission_deadline,
        "submission_deadline_label": item.submission_deadline_label,
        "deadline_timezone": item.deadline_timezone,
        "deadline_policy_version": item.deadline_policy_version,
        "deadline_source": item.deadline_source,
        "source_job_id": str(item.source_job_id) if item.source_job_id else None,
        "preview_job_id": str(item.preview_job_id) if item.preview_job_id else None,
        "preview_id": str(item.preview_id) if item.preview_id else None,
        "preview_key": preview.preview_key if preview else None,
        "send_job_id": str(item.send_job_id) if item.send_job_id else None,
        "source_summary": item.source_summary,
        "preview_summary": item.preview_summary,
        "dispatch_summary": item.dispatch_summary,
        "error": item.error,
        "created_by": item.created_by,
        "created_at": item.created_at,
        "started_at": item.started_at,
        "finished_at": item.finished_at,
        "updated_at": item.updated_at,
    }
    if detail:
        data["stages"] = execution_stage_summary(session, item)
        data["logs"] = combined_execution_logs(session, item)
        data["preview_url"] = f"/whatsapp/previews/{item.preview_id}" if item.preview_id else None
    return data


@router.get("/deadline-policy")
def read_antidengue_deadline_policy(session: Session = Depends(get_session)) -> dict:
    policy = get_or_create_deadline_policy(session)
    session.commit()
    session.refresh(policy)
    return policy_dict(policy)


@router.put("/deadline-policy")
def replace_antidengue_deadline_policy(
    data: AntiDengueDeadlinePolicyUpdate,
    session: Session = Depends(get_session),
) -> dict:
    try:
        policy = update_deadline_policy(
            session,
            submission_deadline=data.submission_deadline,
            timezone=data.timezone,
            updated_by=data.updated_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return policy_dict(policy)


@router.get("/schedules")
def list_antidengue_schedules(
    include_archived: bool = False,
    session: Session = Depends(get_session),
) -> dict:
    statement = select(AntiDengueSchedule)
    if not include_archived:
        statement = statement.where(AntiDengueSchedule.archived_at.is_(None))
    items = session.exec(statement.order_by(col(AntiDengueSchedule.created_at).desc())).all()
    return {"items": [_schedule_dict(item) for item in items], "total": len(items)}


@router.get("/schedules/status")
def antidengue_scheduler_status(session: Session = Depends(get_session)) -> dict:
    enabled = session.scalar(
        select(func.count()).select_from(AntiDengueSchedule).where(
            AntiDengueSchedule.enabled.is_(True),
            AntiDengueSchedule.archived_at.is_(None),
        )
    ) or 0
    active = session.scalar(
        select(func.count()).select_from(AntiDengueScheduleExecution).where(
            AntiDengueScheduleExecution.status.in_(sorted(ACTIVE_EXECUTION_STATUSES))
        )
    ) or 0
    next_item = session.exec(
        select(AntiDengueSchedule)
        .where(
            AntiDengueSchedule.enabled.is_(True),
            AntiDengueSchedule.archived_at.is_(None),
            AntiDengueSchedule.next_run_at.is_not(None),
        )
        .order_by(col(AntiDengueSchedule.next_run_at))
        .limit(1)
    ).first()
    return {
        "enabled_schedules": enabled,
        "active_executions": active,
        "next_run_at": next_item.next_run_at if next_item else None,
        "next_schedule_name": next_item.name if next_item else None,
        "scheduler_enabled": get_settings().antidengue_scheduler_enabled,
        "poll_interval_seconds": get_settings().antidengue_scheduler_interval_seconds,
        "direct_live_mode_enabled": False,
    }


@router.post("/schedules", status_code=status.HTTP_201_CREATED)
def create_antidengue_schedule(
    data: AntiDengueScheduleCreate,
    session: Session = Depends(get_session),
) -> dict:
    try:
        spec = validate_recurrence(
            recurrence_type=data.recurrence_type,
            run_date=data.run_date,
            weekdays=data.weekdays,
            times=data.times,
            timezone=data.timezone,
        )
        profile_ids = normalize_dispatch_profile_ids(data.dispatch_profile_ids, data.dispatch_profile_id)
        validate_dispatch_profiles(session, profile_ids)
        deadline_override = (
            normalize_deadline(data.submission_deadline_override)
            if data.submission_deadline_override
            else None
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    item = AntiDengueSchedule(
        name=data.name.strip(),
        enabled=data.enabled,
        recurrence_type=spec.recurrence_type,
        run_date=spec.run_date,
        weekdays=list(spec.weekdays),
        times=list(spec.times),
        timezone=spec.timezone,
        submission_deadline_override=deadline_override,
        login_mode=data.login_mode,
        dispatch_policy=data.dispatch_policy,
        dispatch_profile_id=profile_ids[0],
        dispatch_profile_ids=[str(value) for value in profile_ids],
        missed_run_grace_minutes=data.missed_run_grace_minutes,
        overlap_grace_minutes=data.overlap_grace_minutes,
        created_by=data.created_by.strip() or "web-operator",
    )
    item.next_run_at = next_occurrence_after(item, utcnow()) if item.enabled else None
    session.add(item)
    session.commit()
    session.refresh(item)
    return _schedule_dict(item)


@router.get("/schedules/{schedule_id}")
def get_antidengue_schedule(
    schedule_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(AntiDengueSchedule, schedule_id)
    if item is None:
        raise HTTPException(status_code=404, detail="AntiDengue schedule not found")
    return _schedule_dict(item)


@router.patch("/schedules/{schedule_id}")
def update_antidengue_schedule(
    schedule_id: uuid.UUID,
    data: AntiDengueScheduleUpdate,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(AntiDengueSchedule, schedule_id)
    if item is None or item.archived_at is not None:
        raise HTTPException(status_code=404, detail="AntiDengue schedule not found")
    values = data.model_dump(exclude_unset=True)
    if "name" in values:
        values["name"] = values["name"].strip()
    if "dispatch_profile_id" in values or "dispatch_profile_ids" in values:
        try:
            profile_ids = normalize_dispatch_profile_ids(
                values.get("dispatch_profile_ids"), values.get("dispatch_profile_id", item.dispatch_profile_id)
            )
            validate_dispatch_profiles(session, profile_ids)
            values["dispatch_profile_id"] = profile_ids[0]
            values["dispatch_profile_ids"] = [str(value) for value in profile_ids]
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    if "submission_deadline_override" in values:
        try:
            values["submission_deadline_override"] = (
                normalize_deadline(values["submission_deadline_override"])
                if values["submission_deadline_override"]
                else None
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    recurrence_type = values.get("recurrence_type", item.recurrence_type)
    run_date = values.get("run_date", item.run_date)
    weekdays = values.get("weekdays", item.weekdays)
    times = values.get("times", item.times)
    timezone = values.get("timezone", item.timezone)
    try:
        spec = validate_recurrence(
            recurrence_type=recurrence_type,
            run_date=run_date,
            weekdays=weekdays,
            times=times,
            timezone=timezone,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    for key, value in values.items():
        setattr(item, key, value)
    item.recurrence_type = spec.recurrence_type
    item.run_date = spec.run_date
    item.weekdays = list(spec.weekdays)
    item.times = list(spec.times)
    item.timezone = spec.timezone
    item.next_run_at = next_occurrence_after(item, utcnow()) if item.enabled else None
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    session.refresh(item)
    return _schedule_dict(item)


@router.delete("/schedules/{schedule_id}")
def archive_antidengue_schedule(
    schedule_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(AntiDengueSchedule, schedule_id)
    if item is None:
        raise HTTPException(status_code=404, detail="AntiDengue schedule not found")
    item.enabled = False
    item.archived_at = utcnow()
    item.next_run_at = None
    item.updated_at = utcnow()
    session.add(item)
    session.commit()
    return {"id": str(item.id), "archived": True}


@router.post("/schedules/{schedule_id}/run-now", status_code=status.HTTP_202_ACCEPTED)
def run_antidengue_schedule_now(
    schedule_id: uuid.UUID,
    data: AntiDengueRunNowRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: Session = Depends(get_session),
) -> dict:
    schedule = session.get(AntiDengueSchedule, schedule_id)
    if schedule is None or schedule.archived_at is not None:
        raise HTTPException(status_code=404, detail="AntiDengue schedule not found")
    lock_execution_scope(session, f"schedule-run-now:{schedule.id}")
    try:
        profile_ids = normalize_dispatch_profile_ids(
            schedule.dispatch_profile_ids, schedule.dispatch_profile_id
        )
        validate_dispatch_profiles(session, profile_ids)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    item = find_equivalent_active_execution(
        session,
        dispatch_profile_ids=profile_ids,
        dispatch_policy=schedule.dispatch_policy,
        login_mode=schedule.login_mode,
        trigger_type="schedule_run_now",
        schedule_id=schedule.id,
    )
    reused = item is not None
    try:
        if item is None:
            item = create_execution(
                session,
                schedule=schedule,
                scheduled_for=utcnow(),
                trigger_type="schedule_run_now",
                dispatch_policy=schedule.dispatch_policy,
                login_mode=schedule.login_mode,
                dispatch_profile_id=schedule.dispatch_profile_id,
                dispatch_profile_ids=profile_ids,
                created_by=data.created_by.strip() or "web-operator",
                idempotency_key=idempotency_key,
            )
        advance_execution(session, item)
        publish_pending_tasks(session, limit=10)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    session.refresh(item)
    result = _execution_dict(session, item, detail=True)
    result["reused_active_execution"] = reused
    return result


@router.post("/executions", status_code=status.HTTP_202_ACCEPTED)
def create_antidengue_execution(
    data: AntiDengueExecutionCreate,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: Session = Depends(get_session),
) -> dict:
    trigger = "manual_send" if data.dispatch_policy == "auto_send_when_clean" else "manual_preview"
    try:
        profile_ids = normalize_dispatch_profile_ids(data.dispatch_profile_ids, data.dispatch_profile_id)
        validate_dispatch_profiles(session, profile_ids)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    profile_key = ",".join(str(value) for value in profile_ids)
    lock_execution_scope(
        session,
        f"manual:{trigger}:{profile_key}:{data.dispatch_policy}:{data.login_mode}",
    )
    item = find_equivalent_active_execution(
        session,
        dispatch_profile_ids=profile_ids,
        dispatch_policy=data.dispatch_policy,
        login_mode=data.login_mode,
        trigger_type=trigger,
    )
    reused = item is not None
    try:
        if item is None:
            item = create_execution(
                session,
                schedule=None,
                scheduled_for=utcnow(),
                trigger_type=trigger,
                dispatch_policy=data.dispatch_policy,
                login_mode=data.login_mode,
                dispatch_profile_id=profile_ids[0],
                dispatch_profile_ids=profile_ids,
                created_by=data.created_by.strip() or "web-operator",
                idempotency_key=idempotency_key,
            )
        advance_execution(session, item)
        publish_pending_tasks(session, limit=10)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    session.refresh(item)
    result = _execution_dict(session, item, detail=True)
    result["reused_active_execution"] = reused
    return result


@router.get("/executions")
def list_antidengue_executions(
    schedule_id: uuid.UUID | None = None,
    execution_status: str = "",
    trigger_type: str = "",
    limit: int = Query(default=50, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict:
    statement = select(AntiDengueScheduleExecution)
    if schedule_id:
        statement = statement.where(AntiDengueScheduleExecution.schedule_id == schedule_id)
    if execution_status:
        statement = statement.where(AntiDengueScheduleExecution.status == execution_status)
    if trigger_type:
        statement = statement.where(AntiDengueScheduleExecution.trigger_type == trigger_type)
    items = session.exec(
        statement.order_by(col(AntiDengueScheduleExecution.created_at).desc()).limit(limit)
    ).all()
    return {"items": [_execution_dict(session, item) for item in items], "total": len(items)}


@router.get("/dispatch-plans")
def list_antidengue_dispatch_plans(
    limit: int = Query(default=100, ge=1, le=200),
    session: Session = Depends(get_session),
) -> dict:
    executions = session.exec(
        select(AntiDengueScheduleExecution)
        .where(AntiDengueScheduleExecution.preview_id.is_not(None))
        .order_by(col(AntiDengueScheduleExecution.created_at).desc())
        .limit(limit)
    ).all()
    items = []
    for execution in executions:
        preview = session.get(WhatsAppDispatchPreview, execution.preview_id)
        if preview is None:
            continue
        profile_ids = execution.dispatch_profile_ids or [str(execution.dispatch_profile_id)]
        profiles = [
            profile for value in profile_ids
            if (profile := session.get(WhatsAppDispatchProfile, uuid.UUID(value))) is not None
        ]
        approval = session.exec(
            select(WhatsAppDispatchApproval).where(WhatsAppDispatchApproval.preview_id == preview.id)
        ).first()
        deliveries = [] if approval is None else session.exec(
            select(WhatsAppDelivery).where(WhatsAppDelivery.approval_id == approval.id)
        ).all()
        totals = {
            "sent": sum(item.status in {"sent", "sent_pending_confirmation"} for item in deliveries),
            "delivered": sum(item.status == "delivered" for item in deliveries),
            "failed": sum(item.status == "failed" for item in deliveries),
            "timed_out": sum(item.status == "timed_out" for item in deliveries),
        }
        items.append({
            "id": str(preview.id),
            "preview_key": preview.preview_key,
            "execution_id": str(execution.id),
            "execution_code": execution.execution_code,
            "execution_status": execution.status,
            "run_type": execution.trigger_type,
            "dispatch_policy": execution.dispatch_policy,
            "created_at": execution.created_at,
            "finished_at": execution.finished_at,
            "error": execution.error,
            "routing_profiles": [{"id": str(item.id), "name": item.name} for item in profiles],
            "report": preview.report_type_name,
            "wing": preview.wing_name,
            "planned_deliveries": preview.delivery_count,
            "warnings": preview.warning_count,
            "blockers": preview.blocked_count,
            "status": preview.status if approval is None else approval.status,
            "approval_id": str(approval.id) if approval else None,
            "issues": list(preview.issues or []),
            **totals,
        })
    return {"items": items, "total": len(items)}


@router.post("/dispatch-plans/{preview_id}/retry-failed", status_code=status.HTTP_202_ACCEPTED)
def retry_antidengue_failed_deliveries(
    preview_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    execution = session.exec(select(AntiDengueScheduleExecution).where(
        AntiDengueScheduleExecution.preview_id == preview_id
    )).first()
    approval = session.exec(
        select(WhatsAppDispatchApproval)
        .where(WhatsAppDispatchApproval.preview_id == preview_id)
        .with_for_update()
    ).first()
    if execution is None or approval is None or execution.send_job_id is None:
        raise HTTPException(status_code=404, detail="AntiDengue delivery attempt not found")
    if approval.status in {"queued", "sending"}:
        raise HTTPException(
            status_code=409,
            detail="A delivery attempt for this approval is already active",
        )
    failed = session.exec(
        select(WhatsAppDelivery)
        .where(
            WhatsAppDelivery.approval_id == approval.id,
            WhatsAppDelivery.status == "failed",
        )
        .with_for_update()
    ).all()
    if not failed:
        ambiguous = session.exec(
            select(WhatsAppDelivery.id).where(
                WhatsAppDelivery.approval_id == approval.id,
                WhatsAppDelivery.status == "timed_out",
            )
        ).first()
        raise HTTPException(
            status_code=409,
            detail=(
                "Timed-out deliveries have an ambiguous send outcome and cannot be "
                "automatically retried; reconcile them before resending."
                if ambiguous
                else "This plan has no explicitly failed deliveries to retry"
            ),
        )
    previous_job = session.get(Job, execution.send_job_id)
    if previous_job is None or previous_job.status in {
        JobStatus.queued.value,
        JobStatus.running.value,
    }:
        raise HTTPException(status_code=409, detail="The dispatch job is unavailable or already active")
    retry_ids = [str(item.id) for item in failed]
    retry_job = add_job(
        session,
        job_type=JobType.whatsapp_dispatch_send.value,
        title=f"Retry failed AntiDengue deliveries from {execution.execution_code}",
        parameters={
            "approval_id": str(approval.id),
            "retry_delivery_ids": retry_ids,
            "retry_of_job_id": str(previous_job.id),
            "retry_requested_by": "web-operator",
        },
    )
    for delivery in failed:
        session.add(
            WhatsAppActivity(
                account_id=delivery.account_id,
                level="info",
                event_type="approved_delivery_retry_requested",
                message=(
                    f"Retry requested for failed approved delivery to "
                    f"{delivery.recipient_name}"
                ),
                details={
                    "delivery_id": str(delivery.id),
                    "approval_id": str(approval.id),
                    "retry_job_id": str(retry_job.id),
                    "previous_status": delivery.status,
                    "previous_error": delivery.error,
                    "previous_provider_result": delivery.provider_result,
                    "requested_by": "web-operator",
                },
            )
        )
        delivery.status = "queued"
        delivery.error = None
        delivery.provider_result = None
        delivery.queue_stream = None
        delivery.queue_sequence = None
        delivery.completed_at = None
        session.add(delivery)
    approval.status = "queued"
    approval.error = None
    approval.completed_at = None
    execution.status = "dispatch_queued"
    execution.send_job_id = retry_job.id
    execution.finished_at = None
    execution.error = None
    execution.updated_at = utcnow()
    session.add(approval)
    session.add(retry_job)
    session.add(execution)
    add_job_log(
        session,
        retry_job.id,
        f"Staged retry for {len(failed)} explicitly failed AntiDengue delivery(ies).",
    )
    stage_task(
        session, job=retry_job, task_name="whatsapp_gateway.send_approved_preview",
        queue="antidengue", args=[str(retry_job.id)],
        idempotency_key=f"execution:{execution.id}:dispatch-retry-job:{retry_job.id}",
    )
    session.commit()
    publish_pending_tasks(session, limit=10)
    return {
        "execution_id": str(execution.id),
        "preview_id": str(preview_id),
        "retry_job_id": str(retry_job.id),
        "retried": len(failed),
    }


@router.get("/executions/{execution_id}")
def get_antidengue_execution(
    execution_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(AntiDengueScheduleExecution, execution_id)
    if item is None:
        raise HTTPException(status_code=404, detail="AntiDengue execution not found")
    return _execution_dict(session, item, detail=True)


@router.post("/executions/{execution_id}/cancel")
def cancel_antidengue_execution(
    execution_id: uuid.UUID,
    session: Session = Depends(get_session),
) -> dict:
    item = session.get(AntiDengueScheduleExecution, execution_id)
    if item is None:
        raise HTTPException(status_code=404, detail="AntiDengue execution not found")
    cancel_execution(session, item, reason="Cancelled by web operator.")
    session.refresh(item)
    return _execution_dict(session, item, detail=True)


@router.get("/executions/{execution_id}/events")
async def stream_antidengue_execution(
    execution_id: uuid.UUID,
    request: Request,
) -> StreamingResponse:
    async def event_stream():
        previous = request.headers.get("last-event-id", "")
        idle = 0
        while True:
            if await request.is_disconnected():
                break
            with session_scope() as session:
                item = session.get(AntiDengueScheduleExecution, execution_id)
                if item is None:
                    payload = {"error": "AntiDengue execution not found", "terminal": True}
                    encoded = json.dumps(payload)
                else:
                    payload = _execution_dict(session, item, detail=True)
                    payload["terminal"] = item.status in TERMINAL_EXECUTION_STATUSES
                    encoded = json.dumps(payload, default=str, separators=(",", ":"))
            digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
            if digest != previous:
                previous = digest
                idle = 0
                yield f"id: {digest}\nevent: snapshot\ndata: {encoded}\n\n"
                if payload.get("terminal"):
                    break
            else:
                idle += 1
                if idle % 15 == 0:
                    heartbeat = json.dumps({"execution_id": str(execution_id), "at": str(utcnow())})
                    yield f"event: heartbeat\ndata: {heartbeat}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/notification-events")
async def stream_antidengue_notifications(request: Request) -> StreamingResponse:
    """Stream new durable AntiDengue milestones without replaying old sounds."""

    async def event_stream():
        last_event_id = request.headers.get("last-event-id")
        with session_scope() as session:
            try:
                cursor = session.get(AntiDengueScheduleEvent, uuid.UUID(last_event_id)) if last_event_id else None
            except ValueError:
                cursor = None
            cursor_at = cursor.created_at if cursor else utcnow()
            cursor_id = str(cursor.id) if cursor else ""
        yield f"event: ready\ndata: {json.dumps({'at': str(cursor_at)})}\n\n"
        idle = 0
        while True:
            if await request.is_disconnected():
                break
            with session_scope() as session:
                statement = (
                    select(AntiDengueScheduleEvent)
                    .where(
                        AntiDengueScheduleEvent.event_type.in_(sorted(NOTIFICATION_EVENT_TYPES)),
                        AntiDengueScheduleEvent.created_at >= cursor_at,
                    )
                    .order_by(col(AntiDengueScheduleEvent.created_at), col(AntiDengueScheduleEvent.id))
                    .limit(100)
                )
                events = [
                    item for item in session.exec(statement)
                    if item.created_at > cursor_at or str(item.id) > cursor_id
                ]
                for item in events:
                    execution = session.get(AntiDengueScheduleExecution, item.execution_id)
                    payload = {
                        "id": str(item.id),
                        "type": item.event_type,
                        "message": item.message,
                        "created_at": item.created_at,
                        "execution_id": str(item.execution_id),
                        "execution_code": execution.execution_code if execution else None,
                        "trigger_type": execution.trigger_type if execution else None,
                        "details": item.details,
                    }
                    yield f"id: {item.id}\nevent: notification\ndata: {json.dumps(payload, default=str, separators=(',', ':'))}\n\n"
                    cursor_at = item.created_at
                    cursor_id = str(item.id)
                    idle = 0
            if not events:
                idle += 1
                if idle % 15 == 0:
                    yield f"event: heartbeat\ndata: {json.dumps({'at': str(utcnow())})}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/notification-settings")
def get_antidengue_notification_settings() -> dict:
    settings = get_settings()
    health = ntfy_health(settings)
    channel_enabled = (
        settings.ntfy_enabled
        and settings.antidengue_ntfy_enabled
        and bool(settings.antidengue_ntfy_topic.strip())
    )
    return {
        "browser_audio": True,
        "ntfy_enabled": channel_enabled,
        "ntfy_reachable": health["reachable"],
        "ntfy_server": settings.ntfy_public_base_url if channel_enabled else None,
        "ntfy_exposure_mode": settings.ntfy_exposure_mode,
        "ntfy_topic": settings.antidengue_ntfy_topic if channel_enabled else None,
    }
