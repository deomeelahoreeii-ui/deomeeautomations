from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import func, or_, select
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.database import get_session
from automation_core.models import Artifact, SourceFile, StoredObject
from automation_core.object_storage import S3ObjectStorage
from automation_core.storage_catalog import archive_artifact, archive_source_file

router = APIRouter(prefix="/api/v1/storage", tags=["storage"])


class BackfillInput(BaseModel):
    apply: bool = False
    module_key: str | None = None
    limit: int = Field(default=200, ge=1, le=2000)


def _artifact_dict(item: Artifact, stored: StoredObject | None = None) -> dict[str, Any]:
    return {
        "id": item.id,
        "job_id": str(item.job_id),
        "module_key": item.module_key,
        "kind": item.kind,
        "name": item.name,
        "path": item.path,
        "size_bytes": item.size_bytes,
        "sha256": item.sha256,
        "content_type": item.content_type,
        "storage_status": item.storage_status,
        "storage_error": item.storage_error,
        "archived_at": item.archived_at,
        "local_present": Path(item.path).is_file(),
        "local_evicted_at": item.local_evicted_at,
        "last_hydrated_at": item.last_hydrated_at,
        "created_at": item.created_at,
        "stored_object": (
            {
                "id": str(stored.id),
                "backend": stored.backend,
                "bucket": stored.bucket,
                "object_key": stored.object_key,
                "verified_at": stored.verified_at,
            }
            if stored
            else None
        ),
    }


@router.get("/overview")
def storage_overview(
    settings: Settings = Depends(get_settings),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    adapter = S3ObjectStorage(settings)
    health = adapter.health(settings.platform_storage_buckets)
    legacy_buckets = list(dict.fromkeys([
        settings.object_storage_raw_bucket,
        settings.object_storage_manifest_bucket,
        settings.object_storage_derived_bucket,
    ]))
    module_rows = session.execute(
        select(
            Artifact.module_key,
            func.count(Artifact.id),
            func.coalesce(func.sum(Artifact.size_bytes), 0),
            func.count(Artifact.id).filter(Artifact.storage_status == "ready"),
            func.count(Artifact.id).filter(Artifact.storage_status == "error"),
        ).group_by(Artifact.module_key).order_by(Artifact.module_key)
    ).all()
    status_rows = session.execute(
        select(Artifact.storage_status, func.count(Artifact.id)).group_by(Artifact.storage_status)
    ).all()
    from antidengue_automation.storage_lifecycle import antidengue_storage_counts

    return {
        **health,
        "platform_buckets": settings.platform_storage_buckets,
        "legacy_buckets": legacy_buckets,
        "catalog": {
            "objects": session.scalar(select(func.count()).select_from(StoredObject)) or 0,
            "bytes": session.scalar(select(func.coalesce(func.sum(StoredObject.size_bytes), 0))) or 0,
            "artifacts": session.scalar(select(func.count()).select_from(Artifact)) or 0,
            "statuses": {str(key): count for key, count in status_rows},
        },
        "modules": [
            {"module_key": module, "artifacts": count, "bytes": size, "ready": ready, "errors": errors}
            for module, count, size, ready, errors in module_rows
        ],
        "antidengue_cache": antidengue_storage_counts(session),
        "local_cache_root": str(settings.artifact_root.expanduser().resolve()),
    }


@router.get("/artifacts")
def storage_artifacts(
    module_key: str = "",
    storage_status: str = "",
    search: str = "",
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    filters = []
    if module_key:
        filters.append(Artifact.module_key == module_key)
    if storage_status:
        filters.append(Artifact.storage_status == storage_status)
    if search.strip():
        term = f"%{search.strip()}%"
        filters.append(or_(Artifact.name.ilike(term), Artifact.kind.ilike(term), Artifact.sha256.ilike(term)))
    total = session.scalar(select(func.count()).select_from(Artifact).where(*filters)) or 0
    items = session.scalars(
        select(Artifact)
        .where(*filters)
        .order_by(Artifact.created_at.desc(), Artifact.id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ).all()
    stored_ids = {item.stored_object_id for item in items if item.stored_object_id}
    stored = {
        item.id: item
        for item in session.scalars(select(StoredObject).where(StoredObject.id.in_(stored_ids))).all()
    } if stored_ids else {}
    return {
        "items": [_artifact_dict(item, stored.get(item.stored_object_id)) for item in items],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.post("/artifacts/{artifact_id}/retry")
def retry_artifact_storage(
    artifact_id: int,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    artifact = session.get(Artifact, artifact_id)
    if artifact is None:
        raise HTTPException(status_code=404, detail="Artifact not found")
    if not Path(artifact.path).is_file():
        raise HTTPException(status_code=409, detail="The local source is missing; retry cannot upload it")
    archived = archive_artifact(session, artifact)
    session.commit()
    session.refresh(artifact)
    if not archived and artifact.storage_status == "error":
        raise HTTPException(status_code=503, detail=artifact.storage_error or "Object storage upload failed")
    stored = session.get(StoredObject, artifact.stored_object_id) if artifact.stored_object_id else None
    return _artifact_dict(artifact, stored)


@router.post("/backfill")
def storage_backfill(
    data: BackfillInput,
    session: Session = Depends(get_session),
) -> dict[str, Any]:
    artifact_filters = [Artifact.storage_status.in_(["local", "error", "uploading"])]
    source_filters = [SourceFile.storage_status != "ready"]
    if data.module_key:
        artifact_filters.append(Artifact.module_key == data.module_key)
        source_filters.append(SourceFile.module_key == data.module_key)
    artifacts = session.scalars(
        select(Artifact).where(*artifact_filters).order_by(Artifact.created_at.desc()).limit(data.limit)
    ).all()
    remaining = max(0, data.limit - len(artifacts))
    sources = session.scalars(
        select(SourceFile).where(*source_filters).order_by(SourceFile.created_at.desc()).limit(remaining)
    ).all() if remaining else []
    candidates = [
        {"type": "artifact", "id": item.id, "module_key": item.module_key, "name": item.name, "size_bytes": item.size_bytes, "local_available": Path(item.path).is_file()}
        for item in artifacts
    ] + [
        {"type": "source_file", "id": str(item.id), "module_key": item.module_key, "name": item.original_name, "size_bytes": item.size_bytes, "local_available": Path(item.stored_path).is_file()}
        for item in sources
    ]
    if not data.apply:
        return {"dry_run": True, "candidates": candidates, "count": len(candidates), "bytes": sum(item["size_bytes"] for item in candidates)}
    ready = errors = missing = 0
    for item in artifacts:
        if not Path(item.path).is_file():
            missing += 1
        elif archive_artifact(session, item):
            ready += 1
        else:
            errors += 1
    for item in sources:
        if not Path(item.stored_path).is_file():
            missing += 1
        elif archive_source_file(session, item):
            ready += 1
        else:
            errors += 1
    session.commit()
    return {"dry_run": False, "processed": len(candidates), "ready": ready, "errors": errors, "missing": missing}
