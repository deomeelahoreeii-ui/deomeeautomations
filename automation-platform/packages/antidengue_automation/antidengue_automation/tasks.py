from __future__ import annotations

import json
import logging
import time
import uuid
from pathlib import Path

from celery.exceptions import SoftTimeLimitExceeded
from sqlmodel import Session

from antidengue_automation.models import AntiDengueScheduleExecution
from antidengue_automation.runtime_state import (
    LAST_SCRAPE_STATE_KEY,
    materialize_runtime_state,
    persist_runtime_state,
)
from antidengue_automation.runtime_snapshot import write_runtime_snapshot
from antidengue_automation.scheduling import append_milestone_once
from antidengue_automation.storage_lifecycle import (
    antidengue_workspace,
    evict_verified_antidengue_cache,
)
from automation_core.celery_app import celery_app
from automation_core.command_runner import append_job_log, run_streamed_command
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.job_service import (
    claim_job_running,
    get_job,
    mark_job_failed,
    mark_job_succeeded,
    record_artifact,
)
from automation_core.models import SourceFile
from automation_core.storage_catalog import archive_job_artifacts, ensure_source_file_local
from automation_core.task_delivery import discarded_missing_job_delivery


logger = logging.getLogger(__name__)


def _archive_run_artifacts(job_id: str) -> dict[str, int]:
    with Session(engine) as session:
        return archive_job_artifacts(session, job_id)


def _archive_failed_run_artifacts(job_id: str) -> None:
    """Best-effort durability for outputs produced before a failed interruption."""

    try:
        storage_result = _archive_run_artifacts(job_id)
    except Exception as exc:
        append_job_log(
            job_id,
            f"Failed-run artifact archival could not complete: {exc}",
            level="error",
        )
        return
    if storage_result["errors"] or storage_result["local"]:
        append_job_log(
            job_id,
            "Failed-run artifact archival remains incomplete: "
            f"{storage_result['errors']} errors, {storage_result['local']} local-only.",
            level="error",
        )


def _latest_run_summary(output_dir: Path, modified_after: float) -> dict | None:
    candidates = [
        path
        for path in output_dir.rglob("run_summary.json")
        if path.is_file() and path.stat().st_mtime >= modified_after
    ]
    if not candidates:
        return None
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    try:
        return json.loads(latest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


@celery_app.task(
    name="antidengue_automation.run_report",
    soft_time_limit=60 * 30,
    time_limit=60 * 35,
)
def run_antidengue_job(job_id: str) -> dict:
    uuid.UUID(job_id)
    settings = get_settings()
    project_root = settings.antidengue_root
    started_at = time.time()
    workspace = antidengue_workspace(settings, job_id)
    output_dir = workspace / "output-files"
    raw_dir = workspace / "drop-raw-files"
    unmapped_report_dir = workspace / "unmapped-officer-reports"
    archive_dir = workspace / "archived-files"
    state_dir = workspace / "state"
    for directory in (
        output_dir,
        raw_dir,
        unmapped_report_dir,
        archive_dir,
        state_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    last_scrape_path = state_dir / "last_scrape_metadata.json"
    with Session(engine) as session:
        materialize_runtime_state(
            session,
            state_key=LAST_SCRAPE_STATE_KEY,
            destination=last_scrape_path,
        )

    with Session(engine) as session:
        job = claim_job_running(session, job_id)
        if job is None:
            existing = get_job(session, job_id)
            if existing is None:
                return discarded_missing_job_delivery(
                    job_id,
                    task_name="antidengue_automation.run_report",
                    logger=logger,
                )
            return {**dict(existing.result or {}), "deduplicated": True, "job_status": existing.status}
        parameters = dict(job.parameters)

    execution_id = parameters.get("orchestration_execution_id")
    dispatch_profile_ids = list(parameters.get("dispatch_profile_ids") or ())
    if execution_id and not dispatch_profile_ids:
        with Session(engine) as session:
            execution = session.get(AntiDengueScheduleExecution, uuid.UUID(str(execution_id)))
            if execution is not None:
                dispatch_profile_ids = list(
                    execution.dispatch_profile_ids or [str(execution.dispatch_profile_id)]
                )

    snapshot_path = state_dir / "runtime_snapshot.json"
    try:
        with Session(engine) as session:
            runtime_snapshot = write_runtime_snapshot(
                session,
                snapshot_path,
                job_id=job_id,
                dispatch_profile_ids=dispatch_profile_ids,
                deadline_snapshot=dict(parameters.get("deadline_snapshot") or {}),
            )
            record_artifact(
                session,
                job_id,
                snapshot_path,
                kind="manifest",
                name="runtime_snapshot.json",
                module_key="antidengue",
            )
    except Exception as exc:
        error = f"Could not freeze PostgreSQL runtime snapshot: {exc}"
        append_job_log(job_id, error, level="error")
        with Session(engine) as session:
            mark_job_failed(session, job_id, error)
        raise

    if execution_id:
        with Session(engine) as session:
            execution = session.get(AntiDengueScheduleExecution, uuid.UUID(str(execution_id)))
            if execution is not None:
                append_milestone_once(
                    session,
                    execution,
                    "antidengue.execution.started",
                    "AntiDengue run started.",
                )

    input_source = str(parameters.get("input_source") or "portal")
    if input_source == "manual_upload":
        source_file_id = parameters.get("source_file_id")
        with Session(engine) as session:
            source_file = session.get(SourceFile, uuid.UUID(str(source_file_id)))
        if source_file is None or source_file.module_key != "antidengue":
            error = "The manual source file record is unavailable"
            with Session(engine) as session:
                mark_job_failed(session, job_id, error)
            raise ValueError(error)
        with Session(engine) as session:
            attached_source = session.get(SourceFile, source_file.id)
            assert attached_source is not None
            source_path = ensure_source_file_local(session, attached_source)
        if (
            not source_path.is_relative_to(settings.source_file_root)
            or not source_path.is_file()
            or source_file.validation_status != "valid"
        ):
            error = "The validated immutable manual source file is unavailable"
            with Session(engine) as session:
                mark_job_failed(session, job_id, error)
            raise ValueError(error)
        with Session(engine) as session:
            record_artifact(
                session,
                job_id,
                source_path,
                kind="raw",
                name=source_file.original_name,
                module_key="antidengue",
            )
        command = [
            str(settings.antidengue_python),
            "main.py",
            "manual-file",
            "--file",
            str(source_path),
            "--dry-run",
        ]
    else:
        command = [str(settings.antidengue_python), "main.py", "portal"]
    if parameters.get("dry_run", True):
        if "--dry-run" not in command:
            command.append("--dry-run")

    portal_reports = parameters.get("portal_reports") or [
        "dormant_users",
        "hotspot_distance",
        "simple_activity_list",
    ]
    if isinstance(portal_reports, str):
        portal_report_keys = portal_reports
    else:
        portal_report_keys = ",".join(str(value) for value in portal_reports)
    env = {
        "PORTAL_LOGIN_MODE": str(parameters.get("login_mode", "auto")),
        "ANTIDENGUE_RUNTIME_SNAPSHOT": str(snapshot_path),
        "ANTIDENGUE_WORK_ROOT": str(workspace),
        "ANTIDENGUE_OUTPUT_DIR": str(output_dir),
        "ANTIDENGUE_RAW_DIR": str(raw_dir),
        "ANTIDENGUE_UNMAPPED_REPORT_DIR": str(unmapped_report_dir),
        "ANTIDENGUE_ARCHIVE_DIR": str(archive_dir),
        "ANTIDENGUE_DEBUG_DIR": str(workspace / "scraper-debug"),
        "ANTIDENGUE_LAST_SCRAPE_METADATA_PATH": str(last_scrape_path),
        "PORTAL_REPORT_CUTOFF": str(parameters.get("report_cutoff") or ""),
        "PORTAL_REPORTS": portal_report_keys,
        "ANTIDENGUE_SUBMISSION_DEADLINE": str(
            dict(parameters.get("deadline_snapshot") or {}).get("label") or ""
        ),
        "ANTIDENGUE_SUBMISSION_DEADLINE_TIMEZONE": str(
            dict(parameters.get("deadline_snapshot") or {}).get("timezone") or ""
        ),
    }
    append_job_log(
        job_id,
        "Starting immutable manual-report adapter."
        if input_source == "manual_upload"
        else "Starting existing AntiDengue pipeline.",
    )

    try:
        result = run_streamed_command(
            job_id,
            command,
            cwd=project_root,
            output_dir=output_dir,
            additional_output_dirs=(
                unmapped_report_dir,
                raw_dir,
                state_dir,
            ),
            env=env,
            module_key="antidengue",
        )
    except SoftTimeLimitExceeded:
        error = "AntiDengue exceeded the 30 minute execution limit"
        append_job_log(job_id, error, level="error")
        _archive_failed_run_artifacts(job_id)
        with Session(engine) as session:
            mark_job_failed(session, job_id, error)
        raise
    except Exception as exc:
        append_job_log(job_id, str(exc), level="error")
        _archive_failed_run_artifacts(job_id)
        with Session(engine) as session:
            mark_job_failed(session, job_id, str(exc))
        raise

    try:
        summary = _latest_run_summary(output_dir, started_at)
        with Session(engine) as session:
            persisted_runtime_state = persist_runtime_state(
                session,
                state_key=LAST_SCRAPE_STATE_KEY,
                source=last_scrape_path,
                job_id=uuid.UUID(job_id),
            )
    except Exception as exc:
        error = f"Could not validate and persist PostgreSQL runtime state: {exc}"
        append_job_log(job_id, error, level="error")
        _archive_failed_run_artifacts(job_id)
        with Session(engine) as session:
            mark_job_failed(session, job_id, error)
        raise
    try:
        storage_result = _archive_run_artifacts(job_id)
    except Exception as exc:
        error = f"Durable RustFS archival failed: {exc}"
        append_job_log(job_id, error, level="error")
        with Session(engine) as session:
            mark_job_failed(session, job_id, error)
        raise
    if storage_result["errors"]:
        append_job_log(
            job_id,
            f"Object storage archived {storage_result['ready']} of {storage_result['total']} artifacts; "
            f"{storage_result['errors']} remain retryable.",
            level="warning",
        )
    elif storage_result["ready"]:
        append_job_log(
            job_id,
            f"Verified {storage_result['ready']} AntiDengue artifacts in durable object storage.",
        )
    job_result = {
        "return_code": result.return_code,
        "artifact_count": result.artifact_count,
        "summary": summary,
        "input_source": input_source,
        "source_file_id": parameters.get("source_file_id"),
        "runtime_snapshot": runtime_snapshot,
        "runtime_state": persisted_runtime_state,
        "workspace": str(workspace),
        "storage": storage_result,
    }
    with Session(engine) as session:
        if storage_result["errors"] or storage_result["local"]:
            job = get_job(session, job_id)
            assert job is not None
            job.result = job_result
            session.add(job)
            session.commit()
            mark_job_failed(
                session,
                job_id,
                "AntiDengue processing finished, but durable RustFS archival is incomplete; "
                "retry storage before treating this run as complete.",
            )
        elif result.return_code == 0:
            mark_job_succeeded(session, job_id, job_result)
        else:
            mark_job_failed(session, job_id, f"Command exited with code {result.return_code}")
    if settings.antidengue_retention_enabled:
        with Session(engine) as session:
            retention = evict_verified_antidengue_cache(
                session,
                settings=settings,
                apply=True,
            )
        if retention["evicted"]:
            append_job_log(
                job_id,
                "Evicted "
                f"{retention['evicted']} verified AntiDengue cache files "
                f"({retention['bytes_evicted']} bytes); RustFS remains authoritative.",
            )
    return job_result
