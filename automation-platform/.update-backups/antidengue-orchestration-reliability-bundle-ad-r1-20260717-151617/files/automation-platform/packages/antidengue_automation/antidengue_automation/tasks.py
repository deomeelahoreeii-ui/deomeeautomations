from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from celery.exceptions import SoftTimeLimitExceeded
from sqlmodel import Session

from automation_core.celery_app import celery_app
from automation_core.command_runner import append_job_log, run_streamed_command
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.job_service import (
    mark_job_failed,
    mark_job_running,
    mark_job_succeeded,
    require_job,
)
from automation_core.models import SourceFile


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

    with Session(engine) as session:
        job = require_job(session, job_id)
        parameters = dict(job.parameters)
        mark_job_running(session, job_id)

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
        source_path = Path(source_file.stored_path).resolve()
        if (
            not source_path.is_relative_to(settings.source_file_root)
            or not source_path.is_file()
            or source_file.validation_status != "valid"
        ):
            error = "The validated immutable manual source file is unavailable"
            with Session(engine) as session:
                mark_job_failed(session, job_id, error)
            raise ValueError(error)
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

    env = {"PORTAL_LOGIN_MODE": str(parameters.get("login_mode", "auto"))}
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
            output_dir=project_root / "output-files",
            additional_output_dirs=(project_root / "unmapped-officer-reports",),
            env=env,
        )
    except SoftTimeLimitExceeded:
        error = "AntiDengue exceeded the 30 minute execution limit"
        append_job_log(job_id, error, level="error")
        with Session(engine) as session:
            mark_job_failed(session, job_id, error)
        raise
    except Exception as exc:
        append_job_log(job_id, str(exc), level="error")
        with Session(engine) as session:
            mark_job_failed(session, job_id, str(exc))
        raise

    summary = _latest_run_summary(project_root / "output-files", started_at)
    job_result = {
        "return_code": result.return_code,
        "artifact_count": result.artifact_count,
        "summary": summary,
        "input_source": input_source,
        "source_file_id": parameters.get("source_file_id"),
    }
    with Session(engine) as session:
        if result.return_code == 0:
            mark_job_succeeded(session, job_id, job_result)
        else:
            mark_job_failed(session, job_id, f"Command exited with code {result.return_code}")
    return job_result
