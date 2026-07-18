from __future__ import annotations

import asyncio
import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import HTTPException
from sqlalchemy import cast, select as sa_select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, col, select

from antidengue_automation.models import (
    AntiDengueSchedule,
    AntiDengueScheduleEvent,
    AntiDengueScheduleExecution,
)
from antidengue_automation.deadline_policy import resolve_deadline_policy
from automation_core.job_service import (
    add_job,
    add_job_log,
    cancel_job,
    get_active_job,
    get_job,
)
from automation_core.config import get_settings
from automation_core.models import Job, JobLog, JobStatus, JobType
from automation_core.task_outbox import cancel_outbox_for_job, requeue_job_task, stage_task
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppApplication,
    WhatsAppDispatchPreview,
    WhatsAppDispatchProfile,
    WhatsAppReportType,
)
from whatsapp_gateway.previews.approval import approve_preview
from whatsapp_gateway.previews.schemas import PreviewApprovalInput

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


def normalize_dispatch_profile_ids(
    dispatch_profile_ids: Iterable[uuid.UUID | str] | None,
    dispatch_profile_id: uuid.UUID | str | None = None,
) -> list[uuid.UUID]:
    values = list(dispatch_profile_ids or ([] if dispatch_profile_id is None else [dispatch_profile_id]))
    normalized = sorted({uuid.UUID(str(value)) for value in values}, key=str)
    if not normalized:
        raise ValueError("Select at least one AntiDengue routing profile")
    if len(normalized) > 50:
        raise ValueError("Select no more than 50 AntiDengue routing profiles")
    return normalized


def validate_dispatch_profiles(
    session: Session, profile_ids: Iterable[uuid.UUID | str]
) -> list[WhatsAppDispatchProfile]:
    profiles = [validate_dispatch_profile(session, uuid.UUID(str(value))) for value in profile_ids]
    report_keys = {
        profile.id: report.key
        for profile in profiles
        if (report := session.get(WhatsAppReportType, profile.report_type_id)) is not None
    }
    digest_profiles = [profile for profile in profiles if report_keys.get(profile.id) == "consolidated_action_digest"]
    detailed_keys = {
        "wing_summary", "tehsil_dormant_summary", "markaz_dormant_summary",
        "hotspot_distance_activity", "simple_activity_timing",
    }
    for digest in digest_profiles:
        overlaps = [
            profile for profile in profiles
            if profile.id != digest.id
            and report_keys.get(profile.id) in detailed_keys
            and profile.audience_id == digest.audience_id
            and profile.wing_id == digest.wing_id
            and profile.recipient_scope_id == digest.recipient_scope_id
            and profile.recipient_channel == digest.recipient_channel
        ]
        if overlaps:
            raise ValueError(
                "Choose either the Consolidated Action Digest or detailed profiles for the same audience; selecting both would send duplicate issue messages."
            )
    return profiles


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


def has_execution_event(
    session: Session,
    execution: AntiDengueScheduleExecution | uuid.UUID,
    event_type: str,
) -> bool:
    """Return whether a durable execution milestone has already been emitted."""
    execution_id = execution.id if isinstance(execution, AntiDengueScheduleExecution) else execution
    return session.exec(
        select(AntiDengueScheduleEvent.id).where(
            AntiDengueScheduleEvent.execution_id == execution_id,
            AntiDengueScheduleEvent.event_type == event_type,
        ).limit(1)
    ).first() is not None


def append_milestone_once(
    session: Session,
    execution: AntiDengueScheduleExecution,
    event_type: str,
    message: str,
    *,
    details: dict | None = None,
) -> AntiDengueScheduleEvent | None:
    """Persist a user-facing notification milestone at most once per execution."""
    if has_execution_event(session, execution, event_type):
        return None
    settings = get_settings()
    ntfy_pending = (
        settings.ntfy_enabled
        and settings.antidengue_ntfy_enabled
        and bool(settings.antidengue_ntfy_topic.strip())
    )
    return append_execution_event(
        session,
        execution,
        event_type,
        message,
        details={
            "notification": True,
            "execution_code": execution.execution_code,
            "trigger_type": execution.trigger_type,
            **({"ntfy_delivery": {"status": "pending", "attempts": 0}} if ntfy_pending else {}),
            **(details or {}),
        },
    )


def create_execution(
    session: Session,
    *,
    schedule: AntiDengueSchedule | None,
    scheduled_for: datetime,
    trigger_type: str,
    dispatch_policy: str,
    login_mode: str,
    dispatch_profile_id: uuid.UUID | None,
    created_by: str,
    dispatch_profile_ids: Iterable[uuid.UUID | str] | None = None,
    idempotency_key: str | None = None,
) -> AntiDengueScheduleExecution:
    profile_ids = normalize_dispatch_profile_ids(dispatch_profile_ids, dispatch_profile_id)
    validate_dispatch_profiles(session, profile_ids)
    dispatch_profile_id = profile_ids[0]
    if dispatch_policy not in {"preview_only", "auto_send_when_clean"}:
        raise ValueError("Unsupported dispatch policy")
    if login_mode not in {"auto", "manual", "remote_approve"}:
        raise ValueError("Unsupported portal login mode")
    scheduled = as_utc(scheduled_for)
    deadline = resolve_deadline_policy(session, schedule=schedule)
    if schedule:
        key = f"schedule:{schedule.id}:{scheduled.isoformat()}"
        overlap_minutes = schedule.overlap_grace_minutes
    else:
        stable_key = (idempotency_key or str(uuid.uuid4())).strip()
        key = f"manual:{trigger_type}:{stable_key}"
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
        dispatch_profile_ids=[str(value) for value in profile_ids],
        submission_deadline=deadline.time,
        submission_deadline_label=deadline.label,
        deadline_timezone=deadline.timezone,
        deadline_policy_version=deadline.policy_version,
        deadline_source=deadline.source,
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
            "submission_deadline": deadline.label,
            "deadline_timezone": deadline.timezone,
            "deadline_policy_version": deadline.policy_version,
            "deadline_source": deadline.source,
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
                    dispatch_profile_ids=schedule.dispatch_profile_ids or [str(schedule.dispatch_profile_id)],
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


def _queue_source_job(session: Session, execution: AntiDengueScheduleExecution) -> bool:
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
            return True
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
            return True
        return False

    job = add_job(
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
            "report_cutoff": execution.scheduled_for.isoformat(),
            "portal_reports": "dormant_users,hotspot_distance,simple_activity_list",
            "deadline_snapshot": {
                "time": execution.submission_deadline,
                "label": execution.submission_deadline_label,
                "timezone": execution.deadline_timezone,
                "policy_version": execution.deadline_policy_version,
                "source": execution.deadline_source,
            },
        },
    )
    execution.source_job_id = job.id
    execution.status = "dry_run_queued"
    execution.started_at = execution.started_at or utcnow()
    execution.updated_at = utcnow()
    session.add(execution)
    add_job_log(
        session,
        job.id,
        f"Staged by orchestration execution {execution.execution_code}; direct live mode is disabled.",
    )
    stage_task(
        session,
        job=job,
        task_name="antidengue_automation.run_report",
        queue="antidengue",
        args=[str(job.id)],
        idempotency_key=f"execution:{execution.id}:dry-run",
    )
    session.add(
        AntiDengueScheduleEvent(
            execution_id=execution.id,
            level="info",
            event_type="dry_run_queued",
            message="Committed the proven dry-run planner and its broker outbox atomically.",
            details={"job_id": str(job.id)},
        )
    )
    session.commit()
    return True

def _queue_preview_job(session: Session, execution: AntiDengueScheduleExecution) -> bool:
    if execution.source_job_id is None:
        raise ValueError("Source job is missing")
    from automation_core.worker_runtime import require_compatible_workers
    from whatsapp_gateway.previews.compiler.capabilities import (
        PREVIEW_COMPILER_QUEUE, PREVIEW_COMPILER_TASK, required_compiler_contract,
    )

    profile_ids = execution.dispatch_profile_ids or [str(execution.dispatch_profile_id)]
    profiles = [
        profile for value in profile_ids
        if (profile := session.get(WhatsAppDispatchProfile, uuid.UUID(str(value)))) is not None
    ]
    if len(profiles) != len(profile_ids):
        raise ValueError("One or more selected routing profiles no longer exist.")
    compiler_contract = required_compiler_contract(session, profiles)
    require_compatible_workers(session, compiler_contract)
    job = add_job(
        session,
        job_type=JobType.whatsapp_dispatch_preview.value,
        title=f"Prepare exact AntiDengue send plan {execution.execution_code}",
        parameters={
            "source_job_id": str(execution.source_job_id),
            "dispatch_profile_id": str(execution.dispatch_profile_id),
            "dispatch_profile_ids": execution.dispatch_profile_ids or [str(execution.dispatch_profile_id)],
            "created_by": f"antidengue-orchestrator:{execution.execution_code}",
            "orchestration_execution_id": str(execution.id),
            "compiler_contract": compiler_contract,
            "deadline_snapshot": {
                "time": execution.submission_deadline,
                "label": execution.submission_deadline_label,
                "timezone": execution.deadline_timezone,
                "policy_version": execution.deadline_policy_version,
                "source": execution.deadline_source,
            },
        },
    )
    execution.preview_job_id = job.id
    execution.status = "preview_queued"
    execution.updated_at = utcnow()
    session.add(execution)
    add_job_log(session, job.id, f"Staged by orchestration execution {execution.execution_code}.")
    stage_task(
        session,
        job=job,
        task_name=PREVIEW_COMPILER_TASK,
        queue=PREVIEW_COMPILER_QUEUE,
        args=[str(job.id)],
        idempotency_key=f"execution:{execution.id}:preview",
    )
    session.add(
        AntiDengueScheduleEvent(
            execution_id=execution.id,
            level="info",
            event_type="preview_queued",
            message="Dry run succeeded; committed the exact send-plan compiler and outbox atomically.",
            details={"job_id": str(job.id)},
        )
    )
    session.commit()
    return True

def _auto_approve(session: Session, execution: AntiDengueScheduleExecution, preview: WhatsAppDispatchPreview) -> None:
    if preview.status != "ready" or preview.blocked_count:
        mark_execution_terminal(
            session,
            execution,
            "blocked",
            error="The exact send plan contains blocking issues.",
            message="Automatic sending was blocked because the exact send plan is not clean.",
            level="warning",
        )
        return
    if preview.warning_count:
        mark_execution_terminal(
            session,
            execution,
            "blocked",
            error=f"The exact send plan contains {preview.warning_count} warning(s).",
            message="Automatic sending requires a warning-free preview. Review it manually instead.",
            level="warning",
        )
        return
    if preview.ready_count <= 0 and preview.skipped_count > 0:
        mark_execution_terminal(
            session,
            execution,
            "completed",
            message=(
                "Nothing new to send: "
                f"{preview.skipped_count} once-daily acknowledgement(s) were already sent today."
            ),
        )
        return
    if preview.delivery_count <= 0 or preview.ready_count <= 0:
        mark_execution_terminal(
            session,
            execution,
            "blocked",
            error="The exact send plan contains no sendable deliveries.",
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
            message="Unexpected error while authorizing the exact send plan.",
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


def advance_execution(session: Session, execution: AntiDengueScheduleExecution) -> bool:
    if execution.status in TERMINAL_EXECUTION_STATUSES:
        return False
    if execution.source_job_id is None:
        return _queue_source_job(session, execution)

    source_job = get_job(session, execution.source_job_id)
    if source_job is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The linked dry-run job no longer exists.",
            message="The linked dry-run job could not be found.",
        )
        return True
    if source_job.status in {JobStatus.queued.value, JobStatus.running.value}:
        desired = "dry_run_running" if source_job.status == JobStatus.running.value else "dry_run_queued"
        changed = execution.status != desired
        if changed:
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
        return changed
    if source_job.status != JobStatus.succeeded.value:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=source_job.error or "The AntiDengue dry run failed.",
            message="Report generation failed; no preview or WhatsApp dispatch was attempted.",
        )
        return True
    if not bool(source_job.parameters.get("dry_run")):
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="Safety invariant violated: source job was not a dry run.",
            message="Safety invariant blocked orchestration because the source job was not dry-run only.",
        )
        return True
    execution.source_summary = dict(source_job.result or {})

    if execution.preview_job_id is None:
        summary = execution.source_summary.get("summary") or {}
        append_milestone_once(
            session,
            execution,
            "antidengue.report.downloaded",
            "AntiDengue portal report downloaded and validated.",
            details={
                "input_source": execution.source_summary.get("input_source", "portal"),
                "artifact_count": execution.source_summary.get("artifact_count", 0),
                "run_id": summary.get("run_id"),
            },
        )
        return _queue_preview_job(session, execution)
    preview_job = get_job(session, execution.preview_job_id)
    if preview_job is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The linked preview job no longer exists.",
            message="The linked immutable-preview compilation job could not be found.",
        )
        return True
    if preview_job.status in {JobStatus.queued.value, JobStatus.running.value}:
        desired = "preview_compiling" if preview_job.status == JobStatus.running.value else "preview_queued"
        changed = execution.status != desired
        if changed:
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
        return changed
    if preview_job.status != JobStatus.succeeded.value:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=preview_job.error or "The immutable preview compilation failed.",
            message="The dry run succeeded, but preview compilation failed. Nothing was sent.",
        )
        return True

    preview_id_raw = (preview_job.result or {}).get("preview_id")
    if not preview_id_raw:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="Preview job completed without a preview identifier.",
            message="Preview compilation returned an invalid result. Nothing was sent.",
        )
        return True
    preview = session.get(WhatsAppDispatchPreview, uuid.UUID(str(preview_id_raw)))
    if preview is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The compiled preview record is unavailable.",
            message="The compiled preview could not be loaded. Nothing was sent.",
        )
        return True
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
                error="The exact send plan contains blocking issues.",
                message="Preview is ready for manual correction but contains blocking issues. Nothing was sent.",
                level="warning",
            )
        else:
            mark_execution_terminal(
                session,
                execution,
                "preview_ready",
                message="Exact send plan is ready for manual review. Sending skipped — test mode.",
            )
        return True

    if execution.send_job_id is None:
        _auto_approve(session, execution, preview)
        return True
    send_job = get_job(session, execution.send_job_id)
    if send_job is None:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error="The linked dispatch job no longer exists.",
            message="The approved dispatch job could not be found.",
        )
        return True
    if send_job.status in {JobStatus.queued.value, JobStatus.running.value}:
        desired = "dispatch_running" if send_job.status == JobStatus.running.value else "dispatch_queued"
        changed = execution.status != desired
        if changed:
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
        return changed
    if send_job.status != JobStatus.succeeded.value:
        mark_execution_terminal(
            session,
            execution,
            "failed",
            error=send_job.error or "WhatsApp dispatch failed.",
            message="The exact send plan was approved, but the dispatch job failed.",
        )
        return True
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
    if not failed_count:
        append_milestone_once(
            session,
            execution,
            "antidengue.messages.sent",
            f"WhatsApp dispatch completed: {delivered_count} message(s) sent.",
            details={"delivered_count": delivered_count},
        )
    return True

def advance_pending_executions(session: Session, limit: int = 25) -> int:
    statement = (
        select(AntiDengueScheduleExecution)
        .where(AntiDengueScheduleExecution.status.in_(sorted(ACTIVE_EXECUTION_STATUSES)))
        .order_by(col(AntiDengueScheduleExecution.scheduled_for), col(AntiDengueScheduleExecution.created_at))
        .limit(limit)
    )
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        statement = statement.with_for_update(skip_locked=True)
    executions = list(session.exec(statement))
    changed = 0
    source_slot_used = False
    for execution in executions:
        # Only the oldest source-less occurrence is allowed to contend for the single
        # AntiDengue planner slot during one tick. Existing jobs may all advance.
        if execution.source_job_id is None and source_slot_used:
            continue
        try:
            did_change = advance_execution(session, execution)
            changed += int(did_change)
            if execution.source_job_id is None or execution.status in {"dry_run_queued", "waiting_overlap"}:
                source_slot_used = True
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
                changed += 1
    return changed



def lock_execution_scope(session: Session, scope: str) -> None:
    """Serialize equivalent execution creation across API processes on PostgreSQL."""
    if session.bind is None or session.bind.dialect.name != "postgresql":
        return
    raw = int.from_bytes(hashlib.sha256(scope.encode("utf-8")).digest()[:8], "big", signed=False)
    signed = raw if raw < 2**63 else raw - 2**64
    session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": signed})


def equivalent_profile_ids_clause(dialect_name: str, canonical_ids: list[str]):
    profile_ids_column = AntiDengueScheduleExecution.dispatch_profile_ids
    if dialect_name == "postgresql":
        # The existing portable schema uses PostgreSQL JSON, which has no
        # equality operator. JSONB has structural equality and preserves array
        # order, so compare canonical selections through JSONB on PostgreSQL.
        return cast(profile_ids_column, JSONB) == cast(canonical_ids, JSONB)
    return profile_ids_column == canonical_ids


def find_equivalent_active_execution(
    session: Session,
    *,
    dispatch_profile_id: uuid.UUID | None = None,
    dispatch_profile_ids: Iterable[uuid.UUID | str] | None = None,
    dispatch_policy: str,
    login_mode: str,
    trigger_type: str | None = None,
    schedule_id: uuid.UUID | None = None,
) -> AntiDengueScheduleExecution | None:
    profile_ids = normalize_dispatch_profile_ids(dispatch_profile_ids, dispatch_profile_id)
    canonical_ids = [str(value) for value in profile_ids]
    dialect_name = session.bind.dialect.name if session.bind is not None else ""
    profile_ids_match = equivalent_profile_ids_clause(dialect_name, canonical_ids)
    statement = (
        select(AntiDengueScheduleExecution)
        .where(
            AntiDengueScheduleExecution.status.in_(sorted(ACTIVE_EXECUTION_STATUSES)),
            profile_ids_match,
            AntiDengueScheduleExecution.dispatch_policy == dispatch_policy,
            AntiDengueScheduleExecution.login_mode == login_mode,
        )
        .order_by(col(AntiDengueScheduleExecution.created_at).desc())
        .limit(1)
    )
    if trigger_type is not None:
        statement = statement.where(AntiDengueScheduleExecution.trigger_type == trigger_type)
    if schedule_id is None:
        statement = statement.where(AntiDengueScheduleExecution.schedule_id.is_(None))
    else:
        statement = statement.where(AntiDengueScheduleExecution.schedule_id == schedule_id)
    return session.exec(statement).first()


def cancel_execution(
    session: Session,
    execution: AntiDengueScheduleExecution,
    *,
    reason: str = "Cancelled by operator.",
) -> None:
    if execution.status in TERMINAL_EXECUTION_STATUSES:
        return
    from automation_core.celery_app import celery_app

    for job_id in (execution.source_job_id, execution.preview_job_id, execution.send_job_id):
        if not job_id:
            continue
        job = cancel_job(session, job_id, reason)
        cancel_outbox_for_job(session, job_id)
        if job and job.task_id:
            try:
                celery_app.control.revoke(job.task_id, terminate=True, signal="SIGTERM")
            except Exception:
                # Database cancellation remains authoritative when the broker is unavailable.
                pass
    session.commit()
    mark_execution_terminal(
        session,
        execution,
        "cancelled",
        error=reason,
        message=reason,
        level="warning",
    )


def recover_orphaned_executions(session: Session) -> dict[str, int]:
    stats = {"failed": 0, "requeued": 0, "cancelled_outbox": 0, "duplicates": 0, "stale": 0}
    executions = list(
        session.exec(
            select(AntiDengueScheduleExecution).where(
                AntiDengueScheduleExecution.status.in_(sorted(ACTIVE_EXECUTION_STATUSES))
            )
        )
    )
    # A crash or an older API process may have bypassed the advisory-lock path.
    # Keep the oldest equivalent active occurrence and preserve the others as
    # cancelled audit records.
    equivalents: dict[tuple, list[AntiDengueScheduleExecution]] = {}
    for execution in executions:
        profile_ids = tuple(execution.dispatch_profile_ids or [str(execution.dispatch_profile_id)])
        key = (
            execution.trigger_type,
            execution.login_mode,
            execution.dispatch_policy,
            profile_ids,
            execution.schedule_id,
        )
        equivalents.setdefault(key, []).append(execution)
    for matches in equivalents.values():
        matches.sort(key=lambda item: (as_utc(item.created_at), str(item.id)))
        for duplicate in matches[1:]:
            cancel_execution(
                session,
                duplicate,
                reason=f"Recovery cancelled duplicate equivalent execution; retained {matches[0].execution_code}.",
            )
            stats["duplicates"] += 1
    executions = [item for item in executions if item.status in ACTIVE_EXECUTION_STATUSES]

    now = utcnow()
    for execution in executions:
        linked = [
            ("source", execution.source_job_id),
            ("preview", execution.preview_job_id),
            ("dispatch", execution.send_job_id),
        ]
        missing = [stage for stage, job_id in linked if job_id and session.get(Job, job_id) is None]
        if missing:
            mark_execution_terminal(
                session,
                execution,
                "failed",
                error=f"Missing persisted job record(s): {', '.join(missing)}",
                message="Recovered an orphaned execution whose linked job record was missing.",
            )
            stats["failed"] += 1
            continue
        current_job_id = execution.send_job_id or execution.preview_job_id or execution.source_job_id
        if current_job_id:
            job = session.get(Job, current_job_id)
            age = now - as_utc(job.updated_at) if job else timedelta(0)
            stale_limit = timedelta(minutes=45 if job and job.status == JobStatus.running.value else 20)
            if job and job.status in {JobStatus.queued.value, JobStatus.running.value} and age > stale_limit:
                mark_execution_terminal(
                    session,
                    execution,
                    "failed",
                    error=f"Stale {job.status} job {job.id} had no progress for {int(age.total_seconds() // 60)} minutes.",
                    message="Recovery failed a stale orchestration stage instead of advancing it unsafely.",
                )
                stats["failed"] += 1
                stats["stale"] += 1
                continue
            if job and job.status == JobStatus.queued.value:
                outbox = requeue_job_task(
                    session,
                    job,
                    reason="Recovered a queued job that had no durable pending broker publication.",
                )
                if outbox is not None:
                    session.commit()
                    stats["requeued"] += 1
    return stats

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
        ("dry_run", "Generate reports", execution.source_job_id),
        ("preview", "Prepare exact send plan", execution.preview_job_id),
        ("dispatch", "Send WhatsApp messages", execution.send_job_id),
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
