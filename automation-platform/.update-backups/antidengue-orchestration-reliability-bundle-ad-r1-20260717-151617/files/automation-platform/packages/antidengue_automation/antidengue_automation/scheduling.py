from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from antidengue_automation.models import (
    AntiDengueSchedule,
    AntiDengueScheduleEvent,
    AntiDengueScheduleExecution,
)
from antidengue_automation.tasks import run_antidengue_job
from automation_core.job_service import (
    append_log,
    create_job,
    get_active_job,
    get_job,
    mark_job_failed,
    set_task_id,
)
from automation_core.models import Job, JobLog, JobStatus, JobType
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppApplication,
    WhatsAppDispatchPreview,
    WhatsAppDispatchProfile,
)
from whatsapp_gateway.previews.approval import approve_preview
from whatsapp_gateway.previews.schemas import PreviewApprovalInput
from whatsapp_gateway.tasks import compile_dispatch_preview_job

TERMINAL_EXECUTION_STATUSES = {
    "preview_ready",
    "completed",
    "completed_with_delivery_errors",
    "blocked",
    "failed",
    "skipped",
    "cancelled",
}
ACTIVE_EXECUTION_STATUSES = {
    "due",
    "waiting_overlap",
    "dry_run_queued",
    "dry_run_running",
    "preview_queued",
    "preview_compiling",
    "auto_approving",
    "dispatch_queued",
    "dispatch_running",
}


@dataclass(frozen=True)
class RecurrenceSpec:
    recurrence_type: str
    run_date: date | None
    weekdays: tuple[int, ...]
    times: tuple[str, ...]
    timezone: str




def as_utc(value: datetime) -> datetime:
    """Normalize database datetimes from PostgreSQL or SQLite tests."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)

def normalize_times(values: Iterable[str]) -> list[str]:
    normalized: set[str] = set()
    for raw in values:
        value = str(raw).strip()
        try:
            parsed = datetime.strptime(value, "%H:%M").time()
        except ValueError as exc:
            raise ValueError(f"Invalid schedule time {value!r}; use 24-hour HH:MM") from exc
        normalized.add(f"{parsed.hour:02d}:{parsed.minute:02d}")
    if not normalized:
        raise ValueError("At least one schedule time is required")
    return sorted(normalized)


def normalize_weekdays(values: Iterable[int]) -> list[int]:
    normalized = sorted({int(value) for value in values})
    if any(value < 0 or value > 6 for value in normalized):
        raise ValueError("Weekdays must use Monday=0 through Sunday=6")
    return normalized


def timezone_or_error(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone: {name}") from exc


def validate_recurrence(
    *,
    recurrence_type: str,
    run_date: date | None,
    weekdays: Iterable[int],
    times: Iterable[str],
    timezone: str,
) -> RecurrenceSpec:
    recurrence = recurrence_type.strip().lower()
    if recurrence not in {"once", "daily", "weekly"}:
        raise ValueError("Recurrence must be once, daily, or weekly")
    normalized_times = normalize_times(times)
    normalized_weekdays = normalize_weekdays(weekdays)
    timezone_or_error(timezone)
    if recurrence == "once" and run_date is None:
        raise ValueError("A date is required for a one-time schedule")
    if recurrence == "weekly" and not normalized_weekdays:
        raise ValueError("Select at least one weekday for a weekly schedule")
    return RecurrenceSpec(
        recurrence_type=recurrence,
        run_date=run_date,
        weekdays=tuple(normalized_weekdays),
        times=tuple(normalized_times),
        timezone=timezone,
    )


def schedule_spec(schedule: AntiDengueSchedule) -> RecurrenceSpec:
    return validate_recurrence(
        recurrence_type=schedule.recurrence_type,
        run_date=schedule.run_date,
        weekdays=schedule.weekdays,
        times=schedule.times,
        timezone=schedule.timezone,
    )


def _date_matches(spec: RecurrenceSpec, candidate: date) -> bool:
    if spec.recurrence_type == "once":
        return candidate == spec.run_date
    if spec.recurrence_type == "weekly":
        return candidate.weekday() in spec.weekdays
    return True


def occurrences_between(
    spec: RecurrenceSpec,
    *,
    start_utc: datetime,
    end_utc: datetime,
) -> list[datetime]:
    if start_utc.tzinfo is None or end_utc.tzinfo is None:
        raise ValueError("Occurrence boundaries must be timezone-aware")
    if end_utc < start_utc:
        return []
    zone = timezone_or_error(spec.timezone)
    local_start = start_utc.astimezone(zone)
    local_end = end_utc.astimezone(zone)
    cursor = local_start.date() - timedelta(days=1)
    last_date = local_end.date() + timedelta(days=1)
    results: list[datetime] = []
    while cursor <= last_date:
        if _date_matches(spec, cursor):
            for raw_time in spec.times:
                hour, minute = (int(part) for part in raw_time.split(":", 1))
                local_value = datetime.combine(cursor, time(hour, minute), tzinfo=zone)
                utc_value = local_value.astimezone(UTC)
                if start_utc <= utc_value <= end_utc:
                    results.append(utc_value)
        cursor += timedelta(days=1)
    return sorted(set(results))


def next_occurrence_after(schedule: AntiDengueSchedule, after_utc: datetime) -> datetime | None:
    spec = schedule_spec(schedule)
    horizon = after_utc + timedelta(days=370)
    candidates = occurrences_between(
        spec,
        start_utc=after_utc + timedelta(seconds=1),
        end_utc=horizon,
    )
    return candidates[0] if candidates else None


def validate_dispatch_profile(session: Session, profile_id: uuid.UUID) -> WhatsAppDispatchProfile:
    profile = session.get(WhatsAppDispatchProfile, profile_id)
    if profile is None or not profile.enabled:
        raise ValueError("Select an enabled AntiDengue dispatch profile")
    application = session.get(WhatsAppApplication, profile.application_id)
    if application is None or application.key != "antidengue":
        raise ValueError("The dispatch profile must belong to the AntiDengue application")
    return profile


def execution_code(now: datetime | None = None) -> str:
    stamp = (now or utcnow()).astimezone(UTC).strftime("%Y%m%d-%H%M%S")
    return f"ADS-{stamp}-{uuid.uuid4().hex[:6].upper()}"


def append_execution_event(
    session: Session,
    execution: AntiDengueScheduleExecution | uuid.UUID,
    event_type: str,
    message: str,
    *,
    level: str = "info",
    details: dict | None = None,
) -> AntiDengueScheduleEvent:
    execution_id = execution.id if isinstance(execution, AntiDengueScheduleExecution) else execution
    item = AntiDengueScheduleEvent(
        execution_id=execution_id,
        level=level,
        event_type=event_type,
        message=message,
        details=details or {},
    )
    session.add(item)
    session.commit()
    session.refresh(item)
    return item


def create_execution(
    session: Session,
    *,
    schedule: AntiDengueSchedule | None,
    scheduled_for: datetime,
    trigger_type: str,
    dispatch_policy: str,
    login_mode: str,
    dispatch_profile_id: uuid.UUID,
    created_by: str,
) -> AntiDengueScheduleExecution:
    validate_dispatch_profile(session, dispatch_profile_id)
    if dispatch_policy not in {"preview_only", "auto_send_when_clean"}:
        raise ValueError("Unsupported dispatch policy")
    if login_mode not in {"auto", "manual", "remote_approve"}:
        raise ValueError("Unsupported portal login mode")
    scheduled = as_utc(scheduled_for)
    if schedule:
        key = f"schedule:{schedule.id}:{scheduled.isoformat()}"
        overlap_minutes = schedule.overlap_grace_minutes
    else:
        key = f"manual:{trigger_type}:{uuid.uuid4()}"
        overlap_minutes = 15
    item = AntiDengueScheduleExecution(
        execution_key=key,
        execution_code=execution_code(),
        schedule_id=schedule.id if schedule else None,
        trigger_type=trigger_type,
        scheduled_for=scheduled,
        status="due",
        dispatch_policy=dispatch_policy,
        login_mode=login_mode,
        dispatch_profile_id=dispatch_profile_id,
        overlap_deadline_at=scheduled + timedelta(minutes=overlap_minutes),
        created_by=created_by,
    )
    session.add(item)
    session.commit()
    session.refresh(item)
    append_execution_event(
        session,
        item,
        "execution_created",
        "AntiDengue orchestration occurrence created.",
        details={
            "trigger_type": trigger_type,
            "dispatch_policy": dispatch_policy,
            "scheduled_for": scheduled.isoformat(),
        },
    )
    return item


def ensure_due_executions(session: Session, now: datetime | None = None) -> list[AntiDengueScheduleExecution]:
    current = as_utc(now or utcnow())
    schedules = session.exec(
        select(AntiDengueSchedule).where(
            AntiDengueSchedule.enabled.is_(True),
            AntiDengueSchedule.archived_at.is_(None),
        )
    ).all()
    created: list[AntiDengueScheduleExecution] = []
    for schedule in schedules:
        try:
            spec = schedule_spec(schedule)
        except ValueError as exc:
            schedule.enabled = False
            schedule.last_run_status = f"invalid: {exc}"
            schedule.updated_at = current
            session.add(schedule)
            session.commit()
            continue
        grace = timedelta(minutes=max(0, schedule.missed_run_grace_minutes))
        candidates = occurrences_between(spec, start_utc=current - grace, end_utc=current)
        for scheduled_for in reversed(candidates):
            key = f"schedule:{schedule.id}:{scheduled_for.isoformat()}"
            if session.exec(
                select(AntiDengueScheduleExecution.id).where(
                    AntiDengueScheduleExecution.execution_key == key
                )
            ).first():
                continue
            try:
                item = create_execution(
                    session,
                    schedule=schedule,
                    scheduled_for=scheduled_for,
                    trigger_type="scheduled",
                    dispatch_policy=schedule.dispatch_policy,
                    login_mode=schedule.login_mode,
                    dispatch_profile_id=schedule.dispatch_profile_id,
                    created_by="scheduler",
                )
            except IntegrityError:
                session.rollback()
                break
            created.append(item)
            schedule.last_run_at = scheduled_for
            schedule.last_run_status = "due"
            if schedule.recurrence_type == "once":
                schedule.enabled = False
            break  # Only the latest missed occurrence inside the grace window.
        schedule.next_run_at = next_occurrence_after(schedule, current) if schedule.enabled else None
        schedule.updated_at = current
        session.add(schedule)
        session.commit()
    return created


def _update_schedule_terminal_status(session: Session, execution: AntiDengueScheduleExecution) -> None:
    if not execution.schedule_id or execution.status not in TERMINAL_EXECUTION_STATUSES:
        return
    schedule = session.get(AntiDengueSchedule, execution.schedule_id)
    if schedule is None:
        return
    schedule.last_run_at = execution.finished_at or utcnow()
    schedule.last_run_status = execution.status
    schedule.next_run_at = next_occurrence_after(schedule, utcnow()) if schedule.enabled else None
    schedule.updated_at = utcnow()
    session.add(schedule)
    session.commit()


def mark_execution_terminal(
    session: Session,
    execution: AntiDengueScheduleExecution,
    status: str,
    *,
    error: str | None = None,
    event_type: str | None = None,
    message: str | None = None,
    level: str | None = None,
) -> None:
    execution.status = status
    execution.error = error
    execution.finished_at = utcnow()
    execution.updated_at = execution.finished_at
    session.add(execution)
    session.commit()
    if message:
        append_execution_event(
            session,
            execution,
            event_type or status,
            message,
            level=level or ("error" if status in {"failed", "blocked"} else "info"),
            details={"error": error} if error else {},
        )
    _update_schedule_terminal_status(session, execution)


def _queue_source_job(session: Session, execution: AntiDengueScheduleExecution) -> None:
    active = get_active_job(session, JobType.antidengue_report.value)
    if active is not None:
        deadline = as_utc(execution.overlap_deadline_at or execution.scheduled_for)
        if as_utc(utcnow()) > deadline:
            mark_execution_terminal(
                session,
                execution,
                "skipped",
                error=f"Overlapped active AntiDengue job {active.id}",
                message="Skipped because another AntiDengue run remained active beyond the overlap window.",
                level="warning",
            )
            return
        if execution.status != "waiting_overlap":
            execution.status = "waiting_overlap"
            execution.updated_at = utcnow()
            session.add(execution)
            session.commit()
            append_execution_event(
                session,
                execution,
                "waiting_overlap",
                f"Waiting for active AntiDengue job {active.id} to finish.",
                level="warning",
            )
        return

    job = create_job(
        session,
        job_type=JobType.antidengue_report.value,
        title=f"AntiDengue planned run {execution.execution_code}",
        parameters={
            "dry_run": True,
            "input_source": "portal",
            "login_mode": execution.login_mode,
            "orchestration_execution_id": str(execution.id),
            "trigger_type": execution.trigger_type,
            "dispatch_policy": execution.dispatch_policy,
        },
    )
    execution.source_job_id = job.id
    execution.status = "dry_run_queued"
    execution.started_at = execution.started_at or utcnow()
    execution.updated_at = utcnow()
    session.add(execution)
    session.commit()
    try:
        task = run_antidengue_job.apply_async(args=[str(job.id)], queue="antidengue")
    except Exception as exc:
        mark_job_failed(session, job.id, f"Queue unavailable: {exc}")
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=f"Dry-run queue unavailable: {exc}",
            message="Could not queue the proven AntiDengue dry-run planner.",
        )
        return
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, f"Queued by orchestration execution {execution.execution_code}; direct live mode is disabled.")
    append_execution_event(
        session,
        execution,
        "dry_run_queued",
        "Queued the proven dry-run planner. No WhatsApp messages can be sent by this stage.",
        details={"job_id": str(job.id)},
    )


def _queue_preview_job(session: Session, execution: AntiDengueScheduleExecution) -> None:
    if execution.source_job_id is None:
        raise ValueError("Source job is missing")
    job = create_job(
        session,
        job_type=JobType.whatsapp_dispatch_preview.value,
        title=f"Compile frozen AntiDengue preview {execution.execution_code}",
        parameters={
            "source_job_id": str(execution.source_job_id),
            "dispatch_profile_id": str(execution.dispatch_profile_id),
            "created_by": f"antidengue-orchestrator:{execution.execution_code}",
            "orchestration_execution_id": str(execution.id),
        },
    )
    execution.preview_job_id = job.id
    execution.status = "preview_queued"
    execution.updated_at = utcnow()
    session.add(execution)
    session.commit()
    try:
        task = compile_dispatch_preview_job.apply_async(args=[str(job.id)], queue="antidengue")
    except Exception as exc:
        mark_job_failed(session, job.id, f"Queue unavailable: {exc}")
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=f"Preview queue unavailable: {exc}",
            message="Dry run succeeded, but the immutable preview could not be queued.",
        )
        return
    set_task_id(session, job.id, task.id)
    append_log(session, job.id, f"Queued by orchestration execution {execution.execution_code}.")
    append_execution_event(
        session,
        execution,
        "preview_queued",
        "Dry run succeeded; compiling the exact frozen dispatch preview.",
        details={"job_id": str(job.id)},
    )


def _auto_approve(session: Session, execution: AntiDengueScheduleExecution, preview: WhatsAppDispatchPreview) -> None:
    if preview.status != "ready" or preview.blocked_count:
        mark_execution_terminal(
            session,
            execution,
            "blocked",
            error="The frozen preview contains blocking issues.",
            message="Automatic sending was blocked because the frozen preview is not clean.",
            level="warning",
        )
        return
    if preview.warning_count:
        mark_execution_terminal(
            session,
            execution,
            "blocked",
            error=f"The frozen preview contains {preview.warning_count} warning(s).",
            message="Automatic sending requires a warning-free preview. Review it manually instead.",
            level="warning",
        )
        return
    if preview.delivery_count <= 0 or preview.ready_count <= 0:
        mark_execution_terminal(
            session,
            execution,
            "blocked",
            error="The frozen preview contains no sendable deliveries.",
            message="Automatic sending was blocked because no validated delivery exists.",
            level="warning",
        )
        return

    execution.status = "auto_approving"
    execution.updated_at = utcnow()
    session.add(execution)
    session.commit()
    append_execution_event(
        session,
        execution,
        "auto_approving",
        "All safety gates passed; authorizing the exact frozen payloads.",
        details={
            "preview_id": str(preview.id),
            "delivery_count": preview.delivery_count,
            "content_sha256": preview.content_sha256,
        },
    )
    try:
        send_job = asyncio.run(
            approve_preview(
                preview.id,
                PreviewApprovalInput(
                    acknowledge_warnings=False,
                    approved_by=f"antidengue-scheduler:{execution.execution_code}",
                ),
                session,
            )
        )
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
        mark_execution_terminal(
            session,
            execution,
            "blocked",
            error=detail,
            message=f"Automatic dispatch safety gate blocked sending: {detail}",
            level="warning",
        )
        return
    except Exception as exc:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=str(exc),
            message="Unexpected error while authorizing the frozen preview.",
        )
        return
    execution.send_job_id = send_job.id
    execution.status = "dispatch_queued"
    execution.updated_at = utcnow()
    session.add(execution)
    session.commit()
    append_execution_event(
        session,
        execution,
        "dispatch_queued",
        "Queued the exact approved frozen deliveries through the platform WhatsApp sender.",
        details={"job_id": str(send_job.id)},
    )


def advance_execution(session: Session, execution: AntiDengueScheduleExecution) -> None:
    if execution.status in TERMINAL_EXECUTION_STATUSES:
        return
    if execution.source_job_id is None:
        _queue_source_job(session, execution)
        return

    source_job = get_job(session, execution.source_job_id)
    if source_job is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The linked dry-run job no longer exists.",
            message="The linked dry-run job could not be found.",
        )
        return
    if source_job.status in {JobStatus.queued.value, JobStatus.running.value}:
        desired = "dry_run_running" if source_job.status == JobStatus.running.value else "dry_run_queued"
        if execution.status != desired:
            execution.status = desired
            execution.updated_at = utcnow()
            session.add(execution)
            session.commit()
            append_execution_event(
                session,
                execution,
                desired,
                "The proven dry-run planner is running." if desired == "dry_run_running" else "The dry-run planner is waiting for the worker.",
            )
        return
    if source_job.status != JobStatus.succeeded.value:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=source_job.error or "The AntiDengue dry run failed.",
            message="Report generation failed; no preview or WhatsApp dispatch was attempted.",
        )
        return
    if not bool(source_job.parameters.get("dry_run")):
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="Safety invariant violated: source job was not a dry run.",
            message="Safety invariant blocked orchestration because the source job was not dry-run only.",
        )
        return
    execution.source_summary = dict(source_job.result or {})

    if execution.preview_job_id is None:
        _queue_preview_job(session, execution)
        return
    preview_job = get_job(session, execution.preview_job_id)
    if preview_job is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The linked preview job no longer exists.",
            message="The linked immutable-preview compilation job could not be found.",
        )
        return
    if preview_job.status in {JobStatus.queued.value, JobStatus.running.value}:
        desired = "preview_compiling" if preview_job.status == JobStatus.running.value else "preview_queued"
        if execution.status != desired:
            execution.status = desired
            execution.updated_at = utcnow()
            session.add(execution)
            session.commit()
            append_execution_event(
                session,
                execution,
                desired,
                "Compiling the immutable dispatch preview." if desired == "preview_compiling" else "The preview compiler is queued.",
            )
        return
    if preview_job.status != JobStatus.succeeded.value:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=preview_job.error or "The immutable preview compilation failed.",
            message="The dry run succeeded, but preview compilation failed. Nothing was sent.",
        )
        return

    preview_id_raw = (preview_job.result or {}).get("preview_id")
    if not preview_id_raw:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="Preview job completed without a preview identifier.",
            message="Preview compilation returned an invalid result. Nothing was sent.",
        )
        return
    preview = session.get(WhatsAppDispatchPreview, uuid.UUID(str(preview_id_raw)))
    if preview is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The compiled preview record is unavailable.",
            message="The compiled preview could not be loaded. Nothing was sent.",
        )
        return
    execution.preview_id = preview.id
    execution.preview_summary = {
        "preview_key": preview.preview_key,
        "status": preview.status,
        "ready_count": preview.ready_count,
        "warning_count": preview.warning_count,
        "blocked_count": preview.blocked_count,
        "skipped_count": preview.skipped_count,
        "delivery_count": preview.delivery_count,
        "content_sha256": preview.content_sha256,
    }
    session.add(execution)
    session.commit()

    if execution.dispatch_policy == "preview_only":
        if preview.status == "blocked" or preview.blocked_count:
            mark_execution_terminal(
                session,
                execution,
                "blocked",
                error="The frozen preview contains blocking issues.",
                message="Preview is ready for manual correction but contains blocking issues. Nothing was sent.",
                level="warning",
            )
        else:
            mark_execution_terminal(
                session,
                execution,
                "preview_ready",
                message="Frozen preview is ready for manual review. Nothing was sent.",
            )
        return

    if execution.send_job_id is None:
        _auto_approve(session, execution, preview)
        return
    send_job = get_job(session, execution.send_job_id)
    if send_job is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The linked dispatch job no longer exists.",
            message="The approved dispatch job could not be found.",
        )
        return
    if send_job.status in {JobStatus.queued.value, JobStatus.running.value}:
        desired = "dispatch_running" if send_job.status == JobStatus.running.value else "dispatch_queued"
        if execution.status != desired:
            execution.status = desired
            execution.updated_at = utcnow()
            session.add(execution)
            session.commit()
            append_execution_event(
                session,
                execution,
                desired,
                "WhatsApp delivery is running." if desired == "dispatch_running" else "Approved WhatsApp deliveries are queued.",
            )
        return
    if send_job.status != JobStatus.succeeded.value:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=send_job.error or "WhatsApp dispatch failed.",
            message="The frozen preview was approved, but the dispatch job failed.",
        )
        return
    result = dict(send_job.result or {})
    execution.dispatch_summary = result
    failed_count = int(result.get("failed") or 0)
    delivered_count = int(result.get("delivered") or 0)
    status = "completed_with_delivery_errors" if failed_count else "completed"
    mark_execution_terminal(
        session,
        execution,
        status,
        message=(
            f"Dispatch finished with {delivered_count} delivered and {failed_count} failed."
            if failed_count
            else f"Dispatch completed successfully: {delivered_count} delivered."
        ),
        level="warning" if failed_count else "info",
    )


def advance_pending_executions(session: Session, limit: int = 25) -> int:
    statement = (
        select(AntiDengueScheduleExecution)
        .where(AntiDengueScheduleExecution.status.in_(sorted(ACTIVE_EXECUTION_STATUSES)))
        .order_by(col(AntiDengueScheduleExecution.scheduled_for), col(AntiDengueScheduleExecution.created_at))
        .limit(limit)
    )
    executions = list(session.exec(statement))
    for execution in executions:
        try:
            advance_execution(session, execution)
        except Exception as exc:
            session.rollback()
            current = session.get(AntiDengueScheduleExecution, execution.id)
            if current is not None and current.status not in TERMINAL_EXECUTION_STATUSES:
                mark_execution_terminal(
                    session,
                    current,
                    "failed",
                    error=str(exc),
                    message="The schedule orchestrator encountered an unexpected error.",
                )
    return len(executions)


def combined_execution_logs(session: Session, execution: AntiDengueScheduleExecution, limit: int = 1000) -> list[dict]:
    entries: list[dict] = []
    for event in session.exec(
        select(AntiDengueScheduleEvent)
        .where(AntiDengueScheduleEvent.execution_id == execution.id)
        .order_by(col(AntiDengueScheduleEvent.created_at))
    ):
        entries.append(
            {
                "source": "orchestrator",
                "level": event.level,
                "message": event.message,
                "event_type": event.event_type,
                "created_at": as_utc(event.created_at),
            }
        )
    for stage, job_id in (
        ("dry_run", execution.source_job_id),
        ("preview", execution.preview_job_id),
        ("dispatch", execution.send_job_id),
    ):
        if not job_id:
            continue
        for log in session.exec(
            select(JobLog)
            .where(JobLog.job_id == job_id)
            .order_by(col(JobLog.created_at))
            .limit(limit)
        ):
            entries.append(
                {
                    "source": stage,
                    "level": log.level,
                    "message": log.message,
                    "event_type": "job_log",
                    "created_at": as_utc(log.created_at),
                }
            )
    entries.sort(key=lambda item: item["created_at"])
    return entries[-limit:]


def execution_stage_summary(session: Session, execution: AntiDengueScheduleExecution) -> list[dict]:
    stages: list[dict] = []
    for key, label, job_id in (
        ("dry_run", "Report generation", execution.source_job_id),
        ("preview", "Frozen dispatch preview", execution.preview_job_id),
        ("dispatch", "WhatsApp dispatch", execution.send_job_id),
    ):
        if key == "dispatch" and execution.dispatch_policy == "preview_only":
            stages.append({"key": key, "label": label, "status": "not_requested", "job_id": None})
            continue
        job = session.get(Job, job_id) if job_id else None
        stages.append(
            {
                "key": key,
                "label": label,
                "status": job.status if job else "pending",
                "job_id": str(job.id) if job else None,
                "error": job.error if job else None,
            }
        )
    return stages
