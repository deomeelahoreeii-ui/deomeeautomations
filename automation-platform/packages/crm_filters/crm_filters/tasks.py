from __future__ import annotations

import shutil
import tempfile
import uuid
from pathlib import Path

import requests
from celery.exceptions import SoftTimeLimitExceeded
from sqlmodel import Session

from automation_core.celery_app import celery_app
from automation_core.command_runner import append_job_log
from automation_core.config import Settings, get_settings
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
from crm_filters.pdf_filter import run_pdf_filter
from crm_filters.pdf_intake import extract_pdf_batch
from crm_filters.sheet_filter import run_sheet_filter


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
    if "audit" in name or "report" in name:
        return "audit"
    if path.suffix.lower() == ".zip":
        return "bundle"
    if path.suffix.lower() in {".xlsx", ".xls", ".csv"}:
        return "report"
    return "file"


def _paperless_client(settings: Settings) -> PaperlessClient:
    return PaperlessClient(
        base_url=settings.paperless_url,
        username=settings.paperless_username,
        password=settings.paperless_password,
        token=settings.paperless_token,
        verify_ssl=settings.paperless_verify_ssl,
        ca_bundle=settings.paperless_ca_bundle,
        allow_insecure_fallback=settings.paperless_allow_insecure_fallback,
        timeout_seconds=settings.paperless_timeout_seconds,
        document_type_name=settings.paperless_document_type_complaint,
        max_pages=settings.paperless_max_pages,
    )


def _paperless_failure_message(exc: Exception) -> str:
    if isinstance(exc, requests.exceptions.SSLError):
        return (
            "Paperless TLS certificate verification failed. Install/export the internal root CA and set "
            "PAPERLESS_CA_BUNDLE, or allow the internal-host fallback with "
            "PAPERLESS_ALLOW_INSECURE_FALLBACK=true. "
            f"Original error: {exc}"
        )
    return str(exc)


def _load_source_file(source_file_id: uuid.UUID, *, expected_kind: str) -> SourceFile:
    with Session(engine) as session:
        source_file = session.get(SourceFile, source_file_id)
    if (
        source_file is None
        or source_file.module_key != "crm"
        or source_file.source_kind != expected_kind
    ):
        raise ValueError("The CRM source-file record is unavailable.")
    return source_file


def _validate_source_path(source_file: SourceFile, settings: Settings) -> Path:
    source_path = Path(source_file.stored_path).resolve()
    source_root = settings.source_file_root.resolve()
    if (
        not source_path.is_relative_to(source_root)
        or not source_path.is_file()
        or source_file.validation_status != "valid"
    ):
        raise ValueError("The validated immutable CRM source file is unavailable.")
    return source_path


def _register_artifacts(job_id: str, artifact_paths: list[Path]) -> None:
    with Session(engine) as session:
        for path in artifact_paths:
            record_artifact(
                session,
                job_id,
                path.resolve(),
                kind=_artifact_kind(path),
                name=path.name,
            )


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
        source_file = _load_source_file(
            source_file_id,
            expected_kind="crm_sheet_upload",
        )
        source_path = _validate_source_path(source_file, settings)
    except Exception as exc:
        error = str(exc)
        finish_failure(job_id, error)
        raise

    output_dir = (settings.crm_filter_artifact_root / job_id).resolve()
    if output_dir.exists():
        error = f"The output directory already exists for job {job_id}."
        finish_failure(job_id, error)
        raise FileExistsError(error)

    client = _paperless_client(settings)
    append_job_log(job_id, f"Starting native CRM sheet filter for {source_file.original_name}.")
    append_job_log(
        job_id,
        f"Connecting to Paperless at {client.base_url}; TLS mode: {client.verification_mode}.",
    )
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
        error = _paperless_failure_message(exc)
        shutil.rmtree(output_dir, ignore_errors=True)
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise

    try:
        artifact_paths = [Path(path) for path in summary.pop("artifact_paths")]
        _register_artifacts(job_id, artifact_paths)
        result = {
            **summary,
            "artifact_count": len(artifact_paths),
            "source_file_id": str(source_file_id),
            "paperless_insecure_fallback_used": client.insecure_fallback_used,
        }
        finish_success(job_id, result)
        return result
    except Exception as exc:
        error = f"Failed to register CRM artifacts: {exc}"
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise


@celery_app.task(
    name="crm_filters.run_pdf_filter_job",
    soft_time_limit=60 * 75,
    time_limit=60 * 85,
)
def run_pdf_filter_job(job_id: str) -> dict:
    uuid.UUID(job_id)
    settings = get_settings()
    parameters = start_job(job_id)
    try:
        source_file_id = uuid.UUID(str(parameters.get("source_file_id")))
        source_file = _load_source_file(
            source_file_id,
            expected_kind="crm_pdf_batch_upload",
        )
        source_path = _validate_source_path(source_file, settings)
    except Exception as exc:
        error = str(exc)
        finish_failure(job_id, error)
        raise

    output_dir = (settings.crm_pdf_filter_artifact_root / job_id).resolve()
    if output_dir.exists():
        error = f"The output directory already exists for job {job_id}."
        finish_failure(job_id, error)
        raise FileExistsError(error)

    client = _paperless_client(settings)
    append_job_log(job_id, f"Starting native CRM PDF filter for {source_file.original_name}.")
    append_job_log(
        job_id,
        f"Connecting to Paperless at {client.base_url}; TLS mode: {client.verification_mode}.",
    )
    try:
        metadata = client.connect()
        append_job_log(job_id, "Paperless authentication and metadata discovery succeeded.")
        for warning in metadata.warnings:
            append_job_log(job_id, warning, level="warning")
        paperless_limit = parameters.get("paperless_limit")
        candidates = client.fetch_complaint_index(
            limit=int(paperless_limit) if paperless_limit else None,
            log=lambda message: append_job_log(job_id, message),
        )
        append_job_log(job_id, f"Paperless CRM index contains {len(candidates)} candidate(s).")
        with tempfile.TemporaryDirectory(prefix=f"crm-pdf-{job_id}-") as temp_name:
            pdf_paths = extract_pdf_batch(source_path, Path(temp_name) / "input")
            summary = run_pdf_filter(
                pdf_paths=pdf_paths,
                output_dir=output_dir,
                candidates=candidates,
                log=lambda message: append_job_log(job_id, message),
            )
    except SoftTimeLimitExceeded:
        error = "CRM PDF filtering exceeded the 75 minute execution limit."
        shutil.rmtree(output_dir, ignore_errors=True)
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise
    except Exception as exc:
        error = _paperless_failure_message(exc)
        shutil.rmtree(output_dir, ignore_errors=True)
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise

    try:
        artifact_paths = [Path(path) for path in summary.pop("artifact_paths")]
        _register_artifacts(job_id, artifact_paths)
        result = {
            **summary,
            "artifact_count": len(artifact_paths),
            "source_file_id": str(source_file_id),
            "paperless_insecure_fallback_used": client.insecure_fallback_used,
        }
        finish_success(job_id, result)
        return result
    except Exception as exc:
        error = f"Failed to register CRM PDF artifacts: {exc}"
        append_job_log(job_id, error, level="error")
        finish_failure(job_id, error)
        raise
