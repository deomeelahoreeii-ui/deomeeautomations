from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlmodel import Session, select

from automation_core.config import Settings, get_settings
from automation_core.models import (
    Artifact,
    Job,
    JobStatus,
    SourceFile,
    SourceFileRun,
    StoredObject,
)
from automation_core.storage_catalog import sha256_file
from automation_core.time import utcnow
from whatsapp_gateway.models import (
    WhatsAppDelivery,
    WhatsAppDispatchPreviewArtifact,
)


TERMINAL_JOB_STATUSES = frozenset(
    {
        JobStatus.succeeded.value,
        JobStatus.failed.value,
        JobStatus.cancelled.value,
    }
)


def antidengue_workspace(settings: Settings, job_id: uuid.UUID | str) -> Path:
    return (
        settings.artifact_root.expanduser().resolve()
        / "antidengue-workspaces"
        / str(job_id)
    )


def _managed_roots(settings: Settings) -> tuple[Path, ...]:
    return (
        (settings.artifact_root.expanduser().resolve() / "antidengue-workspaces"),
        (settings.artifact_root.expanduser().resolve() / "antidengue"),
        (settings.artifact_root.expanduser().resolve() / "object-cache"),
        (settings.antidengue_root / "output-files").expanduser().resolve(),
        (settings.antidengue_root / "drop-raw-files").expanduser().resolve(),
        (settings.antidengue_root / "unmapped-officer-reports").expanduser().resolve(),
        (settings.antidengue_root / "archived-files").expanduser().resolve(),
        settings.source_file_root.expanduser().resolve(),
    )


def _inside_managed_root(path: Path, roots: tuple[Path, ...]) -> bool:
    return any(path.is_relative_to(root) for root in roots)


def _as_utc(value: datetime) -> datetime:
    """Normalize SQLite's naive timestamps before retention comparisons."""

    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _active_delivery_paths(session: Session) -> set[Path]:
    paths: set[Path] = set()
    deliveries = session.exec(
        select(WhatsAppDelivery).where(WhatsAppDelivery.status == "queued")
    ).all()
    for delivery in deliveries:
        for attachment in delivery.attachments or []:
            if value := attachment.get("path"):
                paths.add(Path(str(value)).expanduser().resolve(strict=False))
    return paths


def _unprotected_preview_paths(session: Session) -> set[Path]:
    return {
        Path(item.path_snapshot).expanduser().resolve(strict=False)
        for item in session.exec(select(WhatsAppDispatchPreviewArtifact)).all()
        if item.storage_status != "ready" or item.stored_object_id is None
    }


def _remove_empty_parents(path: Path, roots: tuple[Path, ...]) -> None:
    parent = path.parent
    while parent not in roots and _inside_managed_root(parent, roots):
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent


def evict_verified_antidengue_cache(
    session: Session,
    *,
    settings: Settings | None = None,
    older_than_hours: int | None = None,
    apply: bool = False,
    limit: int = 5000,
) -> dict[str, int]:
    """Evict only bytes that are verified in RustFS and safe to hydrate again."""

    settings = settings or get_settings()
    retention_hours = max(
        0,
        older_than_hours
        if older_than_hours is not None
        else settings.antidengue_local_retention_hours,
    )
    now = utcnow()
    all_artifacts = list(
        session.exec(
            select(Artifact)
            .where(Artifact.module_key == "antidengue")
            .order_by(Artifact.created_at)
        ).all()
    )
    all_by_path: dict[Path, list[Artifact]] = defaultdict(list)
    for artifact in all_artifacts:
        all_by_path[Path(artifact.path).expanduser().resolve(strict=False)].append(artifact)
    candidate_paths = list(
        dict.fromkeys(
            Path(artifact.path).expanduser().resolve(strict=False)
            for artifact in all_artifacts
            if artifact.storage_status == "ready" and artifact.stored_object_id is not None
        )
    )[: max(1, limit)]
    by_path = {path: all_by_path[path] for path in candidate_paths}
    source_files = list(
        session.exec(
            select(SourceFile).where(SourceFile.module_key == "antidengue")
        ).all()
    )
    source_by_path: dict[Path, list[SourceFile]] = defaultdict(list)
    for source_file in source_files:
        source_by_path[
            Path(source_file.stored_path).expanduser().resolve(strict=False)
        ].append(source_file)

    roots = _managed_roots(settings)
    protected_paths = _active_delivery_paths(session) | _unprotected_preview_paths(
        session
    )
    stats = {
        "paths_considered": len(by_path),
        "eligible": 0,
        "evicted": 0,
        "bytes_evicted": 0,
        "protected": 0,
        "unmanaged": 0,
        "cache_absent": 0,
        "cache_absence_reconciled": 0,
        "not_fully_durable": 0,
        "integrity_mismatch": 0,
        "source_files_eligible": 0,
        "source_files_evicted": 0,
    }
    for path, all_rows in by_path.items():
        matching_sources = source_by_path.get(path, [])
        if not _inside_managed_root(path, roots) or path.is_symlink():
            stats["unmanaged"] += 1
            continue
        if path in protected_paths:
            stats["protected"] += 1
            continue
        if not path.is_file():
            stats["cache_absent"] += 1
            durable_rows = [*all_rows, *matching_sources]
            if apply and durable_rows and all(
                row.storage_status == "ready" and row.stored_object_id is not None
                for row in durable_rows
            ):
                changed = False
                for row in durable_rows:
                    if row.local_evicted_at is None:
                        row.local_evicted_at = now
                        session.add(row)
                        changed = True
                if changed:
                    stats["cache_absence_reconciled"] += 1
            continue
        if any(
            row.storage_status != "ready" or row.stored_object_id is None
            for row in all_rows
        ) or any(
            row.storage_status != "ready" or row.stored_object_id is None
            for row in matching_sources
        ):
            stats["not_fully_durable"] += 1
            continue
        job_ids = {row.job_id for row in all_rows}
        jobs = [session.get(Job, job_id) for job_id in job_ids]
        source_runs = [
            run
            for source_file in matching_sources
            for run in session.exec(
                select(SourceFileRun).where(
                    SourceFileRun.source_file_id == source_file.id
                )
            ).all()
        ]
        jobs.extend(session.get(Job, run.job_id) for run in source_runs)
        if any(job is None or job.status not in TERMINAL_JOB_STATUSES for job in jobs):
            stats["protected"] += 1
            continue
        path_retention_hours = retention_hours
        if any(
            job is not None
            and job.status in {JobStatus.failed.value, JobStatus.cancelled.value}
            for job in jobs
        ):
            path_retention_hours = max(
                path_retention_hours,
                settings.antidengue_failed_workspace_retention_hours,
            )
        cutoff = now - timedelta(hours=path_retention_hours)
        most_recent_use = max(
            _as_utc(value)
            for row in all_rows
            for value in (row.created_at, row.archived_at, row.last_hydrated_at)
            if value is not None
        )
        if most_recent_use > cutoff:
            continue
        stored_rows = {
            row.id: session.get(StoredObject, row.stored_object_id)
            for row in all_rows
            if row.stored_object_id is not None
        }
        if any(
            stored is None
            or stored.status != "ready"
            or stored.verified_at is None
            or stored.sha256 != row.sha256
            for row in all_rows
            for stored in [stored_rows.get(row.id)]
        ):
            stats["not_fully_durable"] += 1
            continue
        source_stored_rows = [
            session.get(StoredObject, row.stored_object_id)
            for row in matching_sources
            if row.stored_object_id is not None
        ]
        if any(
            stored is None
            or stored.status != "ready"
            or stored.verified_at is None
            or stored.sha256 != row.sha256
            for row, stored in zip(matching_sources, source_stored_rows, strict=True)
        ):
            stats["not_fully_durable"] += 1
            continue
        current_sha256 = sha256_file(path)
        if any(
            row.sha256 != current_sha256
            for row in [*all_rows, *matching_sources]
        ):
            stats["integrity_mismatch"] += 1
            continue
        stats["eligible"] += 1
        if not apply:
            continue
        size_bytes = path.stat().st_size
        path.unlink()
        _remove_empty_parents(path, roots)
        for row in all_rows:
            row.local_evicted_at = now
            session.add(row)
        for source_file in matching_sources:
            source_file.local_evicted_at = now
            session.add(source_file)
        stats["evicted"] += 1
        if matching_sources:
            stats["source_files_evicted"] += 1
        stats["bytes_evicted"] += size_bytes

    for source_file in source_files:
        if source_file.storage_status != "ready" or source_file.stored_object_id is None:
            continue
        most_recent_use = max(
            value
            for value in (
                source_file.created_at,
                source_file.archived_at,
                source_file.last_hydrated_at,
            )
            if value is not None
        )
        path = Path(source_file.stored_path).expanduser().resolve(strict=False)
        if path in by_path:
            continue
        if (
            not path.is_relative_to(settings.source_file_root.expanduser().resolve())
            or path.is_symlink()
            or not path.is_file()
        ):
            continue
        runs = list(
            session.exec(
                select(SourceFileRun).where(
                    SourceFileRun.source_file_id == source_file.id
                )
            ).all()
        )
        jobs = [session.get(Job, run.job_id) for run in runs]
        if any(job is not None and job.status not in TERMINAL_JOB_STATUSES for job in jobs):
            stats["protected"] += 1
            continue
        source_retention_hours = retention_hours
        if any(
            job is not None
            and job.status in {JobStatus.failed.value, JobStatus.cancelled.value}
            for job in jobs
        ):
            source_retention_hours = max(
                source_retention_hours,
                settings.antidengue_failed_workspace_retention_hours,
            )
        cutoff = now - timedelta(hours=source_retention_hours)
        if _as_utc(most_recent_use) > cutoff:
            continue
        stored = session.get(StoredObject, source_file.stored_object_id)
        if (
            stored is None
            or stored.status != "ready"
            or stored.verified_at is None
            or stored.sha256 != source_file.sha256
        ):
            stats["not_fully_durable"] += 1
            continue
        if sha256_file(path) != source_file.sha256:
            stats["integrity_mismatch"] += 1
            continue
        stats["source_files_eligible"] += 1
        if not apply:
            continue
        size_bytes = path.stat().st_size
        path.unlink()
        _remove_empty_parents(path, roots)
        source_file.local_evicted_at = now
        session.add(source_file)
        stats["source_files_evicted"] += 1
        stats["bytes_evicted"] += size_bytes
    if apply:
        session.commit()
    return stats


def antidengue_storage_counts(session: Session) -> dict[str, int]:
    artifacts = list(
        session.exec(
            select(Artifact).where(Artifact.module_key == "antidengue")
        ).all()
    )
    return {
        "artifacts": len(artifacts),
        "raw_files": sum(item.kind == "raw" for item in artifacts),
        "output_files": sum(item.kind != "raw" for item in artifacts),
        "rustfs_ready": sum(item.storage_status == "ready" for item in artifacts),
        "storage_errors": sum(item.storage_status == "error" for item in artifacts),
        "historical_missing": sum(item.storage_status == "missing" for item in artifacts),
        "local_only": sum(item.storage_status == "local" for item in artifacts),
        "local_cached": sum(Path(item.path).is_file() for item in artifacts),
        "local_evicted": sum(item.local_evicted_at is not None for item in artifacts),
    }


__all__ = [
    "antidengue_storage_counts",
    "antidengue_workspace",
    "evict_verified_antidengue_cache",
]
