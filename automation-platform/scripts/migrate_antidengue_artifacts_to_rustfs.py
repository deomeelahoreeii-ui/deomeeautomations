#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import uuid
from collections import defaultdict
from pathlib import Path

from sqlmodel import Session, select

from antidengue_automation.models import AntiDengueRuntimeState
from antidengue_automation.runtime_state import LAST_SCRAPE_STATE_KEY
from antidengue_automation.storage_lifecycle import evict_verified_antidengue_cache
from automation_core.command_runner import artifact_kind
from automation_core.config import get_settings
from automation_core.database import engine
from automation_core.job_service import add_job, record_artifact
from automation_core.models import Artifact, JobStatus, JobType
from automation_core.object_storage import S3ObjectStorage
from automation_core.storage_catalog import archive_artifact, sha256_file
from automation_core.time import utcnow


def _legacy_roots() -> tuple[Path, ...]:
    root = get_settings().antidengue_root
    return (
        root / "drop-raw-files",
        root / "output-files",
        root / "unmapped-officer-reports",
        root / "archived-files",
    )


def _catalog_untracked_files(session: Session, *, apply: bool) -> dict[str, int]:
    known = {
        str(Path(value).expanduser().resolve(strict=False))
        for value in session.exec(
            select(Artifact.path).where(Artifact.module_key == "antidengue")
        ).all()
    }
    missing = [
        (root, path.resolve())
        for root in _legacy_roots()
        if root.is_dir()
        for path in sorted(root.rglob("*"))
        if path.is_file() and str(path.resolve()) not in known
    ]
    if not apply or not missing:
        return {"discovered": len(missing), "cataloged": 0}
    job = add_job(
        session,
        job_type=JobType.antidengue_report.value,
        title="Legacy AntiDengue RustFS inventory migration",
        parameters={"migration": "antidengue-rustfs-v1"},
    )
    job.status = JobStatus.succeeded.value
    job.started_at = utcnow()
    job.finished_at = utcnow()
    job.result = {"legacy_files": len(missing)}
    session.add(job)
    session.commit()
    for root, path in missing:
        record_artifact(
            session,
            job.id,
            path,
            kind=artifact_kind(path, output_dir=root),
            name=str(path.relative_to(root)),
            module_key="antidengue",
        )
    return {"discovered": len(missing), "cataloged": len(missing)}


def _migrate_last_scrape_state(session: Session, *, apply: bool) -> bool:
    source = get_settings().antidengue_root / "last_scrape_metadata.json"
    if not source.is_file():
        return False
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    if apply:
        row = session.get(AntiDengueRuntimeState, LAST_SCRAPE_STATE_KEY)
        if row is None:
            row = AntiDengueRuntimeState(state_key=LAST_SCRAPE_STATE_KEY)
        row.value_json = payload
        row.updated_at = utcnow()
        session.add(row)
        session.commit()
    return True


def _archive_catalog(session: Session, *, apply: bool, limit: int) -> dict[str, int]:
    rows = list(
        session.exec(
            select(Artifact)
            .where(
                Artifact.module_key == "antidengue",
                Artifact.storage_status.not_in(["ready", "missing"]),
            )
            .order_by(Artifact.created_at.desc(), Artifact.id.desc())
            .limit(limit)
        ).all()
    )
    by_path: dict[Path, list[Artifact]] = defaultdict(list)
    for row in rows:
        by_path[Path(row.path).expanduser().resolve(strict=False)].append(row)
    stats = {
        "candidates": len(rows),
        "ready": 0,
        "historical_missing": 0,
        "ambiguous_overwritten": 0,
        "errors": 0,
    }
    adapter = S3ObjectStorage(get_settings()) if apply else None
    for path, path_rows in by_path.items():
        if not path.is_file():
            stats["historical_missing"] += len(path_rows)
            if apply:
                for row in path_rows:
                    row.storage_status = "missing"
                    row.storage_error = "Legacy local artifact is missing and has no verified durable object."
                    session.add(row)
            continue
        current_sha256 = sha256_file(path)
        identified = [row for row in path_rows if row.sha256 == current_sha256]
        unidentified = [row for row in path_rows if not row.sha256]
        if not identified and unidentified:
            newest = max(unidentified, key=lambda row: (row.created_at, row.id or 0))
            newest.sha256 = current_sha256
            identified = [newest]
            ambiguous = [row for row in unidentified if row.id != newest.id]
            stats["ambiguous_overwritten"] += len(ambiguous)
            if apply:
                for row in ambiguous:
                    row.storage_status = "error"
                    row.storage_error = (
                        "Historical artifact reused a mutable local path before SHA-256 "
                        "registration; the original bytes cannot be proven."
                    )
                    session.add(row)
        mismatched = [
            row for row in path_rows if row.sha256 and row.sha256 != current_sha256
        ]
        stats["ambiguous_overwritten"] += len(mismatched)
        if apply:
            for row in mismatched:
                row.storage_status = "error"
                row.storage_error = (
                    "Local path content no longer matches the artifact's registered SHA-256."
                )
                session.add(row)
        for row in identified:
            if not apply:
                stats["ready"] += 1
            elif archive_artifact(session, row, storage=adapter):
                stats["ready"] += 1
            else:
                stats["errors"] += 1
    if apply:
        session.commit()
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Catalog, verify and migrate AntiDengue files into RustFS."
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--evict", action="store_true")
    parser.add_argument("--retention-hours", type=int)
    parser.add_argument("--limit", type=int, default=10000)
    args = parser.parse_args()
    settings = get_settings()
    if args.apply and not settings.object_storage_enabled:
        parser.error("OBJECT_STORAGE_ENABLED must be true when --apply is used")
    with Session(engine) as session:
        state_found = _migrate_last_scrape_state(session, apply=args.apply)
        catalog = _catalog_untracked_files(session, apply=args.apply)
        archived = _archive_catalog(session, apply=args.apply, limit=max(1, args.limit))
        eviction = (
            evict_verified_antidengue_cache(
                session,
                settings=settings,
                older_than_hours=args.retention_hours,
                apply=args.apply,
                limit=max(1, args.limit),
            )
            if args.evict
            else None
        )
    print(
        json.dumps(
            {
                "dry_run": not args.apply,
                "last_scrape_state_found": state_found,
                "catalog": catalog,
                "archive": archived,
                "eviction": eviction,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
