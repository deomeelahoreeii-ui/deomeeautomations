from __future__ import annotations

import shutil
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
    record_artifact,
    require_job,
)
from automation_core.models import SourceFile
from crm_filters.paperless import PaperlessClient
from crm_filters.paths import resolve_project_path
from crm_filters.sheet_filter import run_sheet_filter


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


def finish_success(job_id: str, result: dict) -> None:
    with Session(engine) as session:
        mark_job_succeeded(session, job_id, result)


def finish_failure(job_id: str, error: str) -> None:
    with Session(engine) as session:
        mark_job_failed(session, job_id, error)


def _artifact_kind(path: Path) -> str:
    name = path.name.lower()
    if name == "run_summary.json":
        return "manifest"
    if "audit" in name:
        return "audit"
    if path.suffix.lower() == ".zip":
        return "bundle"
    if path.suffix.lower() in {".xlsx", ".xls", ".csv"}:
        return "report"
    return "file"


@celery_app.task(
    name="crm_filters.run_sheet_filter_job",
    soft_time_limit=60 * 30,
    time_limit=60 * 35,
)
def run_sheet_filter_job(job_id: str) -> dict:
    uuid.UUID(job_id)
    settings = get_settings()
    parameters = start_job(job_id)
    try:
        source_file_id = uuid.UUID(str(parameters.get("source_file_id")))
    except (TypeError, ValueError, AttributeError) as exc:
        error = "The CRM job does not contain a valid source_file_id."
        finish_failure(job_id, error)
        raise ValueError(error) from exc

    with Session(engine) as session:
        source_file = session.get(SourceFile, source_file_id)
    if source_file is None or source_file.module_key != "crm":
        error = "The CRM source-file record is unavailable."
        finish_failure(job_id, error)
        raise ValueError(error)

    source_path = Path(source_file.stored_path).resolve()
    source_root = settings.source_file_root.resolve()
    if (
        not source_path.is_relative_to(source_root)
        or not source_path.is_file()
        or source_file.validation_status != "valid"
    ):
        error = "The validated immutable CRM source file is unavailable."
        finish_failure(job_id, error)
        raise ValueError(error)

    output_dir = (settings.crm_filter_artifact_root / job_id).resolve()
    if output_dir.exists():
        error = f"The output directory already exists for job {job_id}."
        finish_failure(job_id, error)
        raise FileExistsError(error)

    client = PaperlessClient(
        base_url=settings.paperless_url,
        username=settings.paperless_username,
        password=settings.paperless_password,
        token=settings.paperless_token,
        verify_ssl=settings.paperless_verify_ssl,
        timeout_seconds=settings.paperless_timeout_seconds,
        document_type_name=settings.paperless_document_type_complaint,
        max_pages=settings.paperless_max_pages,
    )

    append_job_log(job_id, f"Starting native CRM sheet filter for {source_file.original_name}.")
    append_job_log(job_id, f"Connecting to Paperless at {client.base_url}.")
    try:
        metadata = client.connect()
        append_job_log(job_id, "Paperless authentication and metadata discovery succeeded.")
        for warning in metadata.warnings:
            append_job_log(job_id, warning, level="warning")
        summary = run_sheet_filter(
            source_path=source_path,
            output_dir=output_dir,
            client=client,
            log=lambda message: append_job_log(job_id, message),
        )
    except SoftTimeLimitExceeded:
        error = "CRM sheet filtering exceeded the 30 minute execution limit."
        shutil.rmtree(output_dir, ignore_errors=True)
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise
    except Exception as exc:
        error = str(exc)
        shutil.rmtree(output_dir, ignore_errors=True)
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise

    try:
        artifact_paths = [Path(path) for path in summary.pop("artifact_paths")]
        with Session(engine) as session:
            for path in artifact_paths:
                record_artifact(
                    session,
                    job_id,
                    path.resolve(),
                    kind=_artifact_kind(path),
                    name=path.name,
                )
        result = {
            **summary,
            "artifact_count": len(artifact_paths),
            "source_file_id": str(source_file_id),
        }
        finish_success(job_id, result)
        return result
    except Exception as exc:
        error = f"Failed to register CRM artifacts: {exc}"
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise


@celery_app.task(name="crm_filters.run_pdf_filter_job")
def run_pdf_filter_job(job_id: str) -> dict[str, int]:
    """Temporary legacy adapter retained until the CRM PDF slice is ported."""
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
            {"return_code": result.return_code, "artifact_count": result.artifact_count},
        )
    else:
        finish_failure(job_id, f"Command exited with code {result.return_code}")
    return {"return_code": result.return_code, "artifact_count": result.artifact_count}
