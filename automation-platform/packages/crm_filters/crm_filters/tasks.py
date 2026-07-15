from __future__ import annotations

import shutil
import uuid
from pathlib import Path

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
from crm_filters.paths import resolve_project_path


def legacy_uv_command() -> str:
    settings = get_settings()
    configured = Path(settings.legacy_uv_bin)
    if configured.exists():
        return str(configured)
    discovered = shutil.which("uv")
    return discovered or settings.legacy_uv_bin


def start_job(job_id: str) -> dict:
    with Session(engine) as session:
        job = require_job(session, job_id)
        mark_job_running(session, job_id)
        return dict(job.parameters)


def finish_success(job_id: str, *, return_code: int, artifact_count: int) -> None:
    with Session(engine) as session:
        mark_job_succeeded(
            session,
            job_id,
            {"return_code": return_code, "artifact_count": artifact_count},
        )


def finish_failure(job_id: str, error: str) -> None:
    with Session(engine) as session:
        mark_job_failed(session, job_id, error)


@celery_app.task(name="crm_filters.run_sheet_filter_job")
def run_sheet_filter_job(job_id: str) -> dict[str, int]:
    uuid.UUID(job_id)
    settings = get_settings()
    project_root = settings.crm_root
    parameters = start_job(job_id)

    command = [
        legacy_uv_command(),
        "run",
        "python",
        "filter_crm_excel_duplicates.py",
        "--input-dir",
        parameters.get("input_dir", "phase1-crm/unprocessed-crm/sheets"),
        "--output-dir",
        parameters.get("output_dir", "phase1-crm/unprocessed-crm/filtered"),
    ]
    input_file = parameters.get("input_file")
    if input_file:
        command.extend(["--input-file", input_file])

    output_dir = resolve_project_path(project_root, parameters["output_dir"])
    try:
        result = run_streamed_command(
            job_id,
            command,
            cwd=project_root,
            output_dir=output_dir,
        )
    except Exception as exc:
        append_job_log(job_id, str(exc), level="error")
        finish_failure(job_id, str(exc))
        raise

    if result.return_code == 0:
        finish_success(
            job_id,
            return_code=result.return_code,
            artifact_count=result.artifact_count,
        )
    else:
        finish_failure(job_id, f"Command exited with code {result.return_code}")
    return {"return_code": result.return_code, "artifact_count": result.artifact_count}


@celery_app.task(name="crm_filters.run_pdf_filter_job")
def run_pdf_filter_job(job_id: str) -> dict[str, int]:
    uuid.UUID(job_id)
    settings = get_settings()
    project_root = settings.crm_root
    parameters = start_job(job_id)

    command = [
        legacy_uv_command(),
        "run",
        "python",
        "filter_crm_pdf_duplicates.py",
        "--input-dir",
        parameters.get("input_dir", "crm-main-complaints"),
        "--output-dir",
        parameters.get("output_dir", "phase1-crm/unprocessed-crm/filtered"),
        "--db",
        parameters.get("db", "crm-cache.sqlite"),
    ]
    if parameters.get("skip_paperless_refresh"):
        command.append("--skip-paperless-refresh")
    if parameters.get("paperless_limit"):
        command.extend(["--paperless-limit", str(parameters["paperless_limit"])])

    output_dir = resolve_project_path(project_root, parameters["output_dir"])
    try:
        result = run_streamed_command(
            job_id,
            command,
            cwd=project_root,
            output_dir=output_dir,
        )
    except Exception as exc:
        append_job_log(job_id, str(exc), level="error")
        finish_failure(job_id, str(exc))
        raise

    if result.return_code == 0:
        finish_success(
            job_id,
            return_code=result.return_code,
            artifact_count=result.artifact_count,
        )
    else:
        finish_failure(job_id, f"Command exited with code {result.return_code}")
    return {"return_code": result.return_code, "artifact_count": result.artifact_count}

