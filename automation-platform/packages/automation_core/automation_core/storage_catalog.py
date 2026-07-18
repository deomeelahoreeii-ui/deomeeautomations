from __future__ import annotations

import hashlib
import mimetypes
import re
import uuid
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session

from automation_core.config import Settings, get_settings
from automation_core.models import Artifact, SourceFile, StoredObject
from automation_core.object_storage import ObjectStorageError, S3ObjectStorage
from automation_core.time import utcnow

_SAFE_SEGMENT = re.compile(r"[^a-z0-9._-]+")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def storage_role(kind: str) -> str:
    normalized = kind.strip().lower()
    if normalized in {"raw", "source", "source_file"}:
        return "raw"
    if normalized in {"manifest", "summary_json", "manifest_csv", "audit"}:
        return "manifest"
    return "derived"


def bucket_for_role(settings: Settings, role: str) -> str:
    if role == "raw":
        return settings.platform_storage_raw_bucket
    if role == "manifest":
        return settings.platform_storage_manifest_bucket
    return settings.platform_storage_derived_bucket


def object_key_for(*, module_key: str, role: str, sha256: str, name: str) -> str:
    module = _SAFE_SEGMENT.sub("-", module_key.lower()).strip("-") or "platform"
    suffix = Path(name).suffix.lower()
    if not suffix or len(suffix) > 12 or not re.fullmatch(r"\.[a-z0-9]+", suffix):
        suffix = ".bin"
    return f"{module}/{role}/sha256/{sha256[:2]}/{sha256[2:4]}/{sha256}{suffix}"


def store_path(
    session: Session,
    path: Path,
    *,
    module_key: str,
    kind: str,
    name: str | None = None,
    content_type: str | None = None,
    settings: Settings | None = None,
    storage: S3ObjectStorage | None = None,
) -> StoredObject | None:
    settings = settings or get_settings()
    if not settings.object_storage_enabled:
        return None
    source = path.expanduser().resolve()
    if not source.is_file():
        raise ObjectStorageError(f"Local artifact is unavailable: {source}")
    digest = sha256_file(source)
    size = source.stat().st_size
    role = storage_role(kind)
    bucket = bucket_for_role(settings, role)
    display_name = name or source.name
    object_key = object_key_for(
        module_key=module_key,
        role=role,
        sha256=digest,
        name=display_name,
    )
    adapter = storage or S3ObjectStorage(settings)
    result = adapter.put_file_if_absent(
        bucket=bucket,
        object_key=object_key,
        source_path=source,
        sha256=digest,
        size_bytes=size,
        content_type=content_type or mimetypes.guess_type(display_name)[0] or "application/octet-stream",
        metadata={"module": module_key, "role": role, "name": display_name[:240]},
    )
    existing = session.scalar(
        select(StoredObject).where(
            StoredObject.backend == settings.object_storage_provider,
            StoredObject.bucket == result.bucket,
            StoredObject.object_key == result.object_key,
        )
    )
    item = existing
    if item is None:
        try:
            with session.begin_nested():
                item = StoredObject(
                    id=uuid.uuid4(),
                    backend=settings.object_storage_provider,
                    bucket=result.bucket,
                    object_key=result.object_key,
                    sha256=result.sha256,
                )
                session.add(item)
                session.flush()
        except IntegrityError:
            item = session.scalar(
                select(StoredObject).where(
                    StoredObject.backend == settings.object_storage_provider,
                    StoredObject.bucket == result.bucket,
                    StoredObject.object_key == result.object_key,
                )
            )
            if item is None:
                raise
    item.sha256 = result.sha256
    item.size_bytes = result.size_bytes
    item.content_type = result.content_type
    item.etag = result.etag
    item.version_id = result.version_id
    item.status = "ready"
    item.last_error = None
    item.verified_at = utcnow()
    session.add(item)
    session.flush()
    return item


def archive_artifact(
    session: Session,
    artifact: Artifact,
    *,
    settings: Settings | None = None,
    storage: S3ObjectStorage | None = None,
) -> bool:
    settings = settings or get_settings()
    path = Path(artifact.path)
    if not settings.object_storage_enabled:
        artifact.storage_status = "local"
        artifact.storage_error = None
        session.add(artifact)
        return False
    artifact.storage_status = "uploading"
    artifact.storage_error = None
    session.add(artifact)
    session.flush()
    try:
        stored = store_path(
            session,
            path,
            module_key=artifact.module_key,
            kind=artifact.kind,
            name=artifact.name,
            content_type=artifact.content_type,
            settings=settings,
            storage=storage,
        )
        assert stored is not None
        artifact.stored_object_id = stored.id
        artifact.sha256 = stored.sha256
        artifact.size_bytes = stored.size_bytes
        artifact.content_type = stored.content_type
        artifact.storage_status = "ready"
        artifact.storage_error = None
        artifact.archived_at = utcnow()
        session.add(artifact)
        return True
    except Exception as exc:
        artifact.storage_status = "error"
        artifact.storage_error = str(exc)[:4000]
        session.add(artifact)
        return False


def archive_source_file(
    session: Session,
    source_file: SourceFile,
    *,
    settings: Settings | None = None,
    storage: S3ObjectStorage | None = None,
) -> bool:
    settings = settings or get_settings()
    if not settings.object_storage_enabled:
        source_file.storage_status = "local"
        session.add(source_file)
        return False
    source_file.storage_status = "uploading"
    session.add(source_file)
    session.flush()
    try:
        stored = store_path(
            session,
            Path(source_file.stored_path),
            module_key=source_file.module_key,
            kind="raw",
            name=source_file.original_name,
            content_type=source_file.content_type,
            settings=settings,
            storage=storage,
        )
        assert stored is not None
        source_file.stored_object_id = stored.id
        source_file.storage_status = "ready"
        source_file.storage_error = None
        source_file.archived_at = utcnow()
        session.add(source_file)
        return True
    except Exception as exc:
        source_file.storage_status = "error"
        source_file.storage_error = str(exc)[:4000]
        session.add(source_file)
        return False


def hydrate_stored_object(
    stored: StoredObject,
    destination: Path,
    *,
    settings: Settings | None = None,
    storage: S3ObjectStorage | None = None,
) -> Path:
    settings = settings or get_settings()
    destination = destination.expanduser().resolve()
    if destination.is_file() and sha256_file(destination) == stored.sha256:
        return destination
    adapter = storage or S3ObjectStorage(settings)
    result = adapter.download_file(
        bucket=stored.bucket,
        object_key=stored.object_key,
        destination=destination,
    )
    if result["sha256"] != stored.sha256 or int(result["size_bytes"]) != stored.size_bytes:
        destination.unlink(missing_ok=True)
        raise ObjectStorageError(f"Hydrated object failed integrity verification: {stored.object_key}")
    return destination


def ensure_artifact_local(session: Session, artifact: Artifact) -> Path:
    local = Path(artifact.path).expanduser().resolve(strict=False)
    if local.is_file() and (not artifact.sha256 or sha256_file(local) == artifact.sha256):
        return local
    if artifact.stored_object_id is None:
        raise ObjectStorageError(f"Artifact has no durable object: {artifact.name}")
    stored = session.get(StoredObject, artifact.stored_object_id)
    if stored is None or stored.status != "ready":
        raise ObjectStorageError(f"Artifact durable object is unavailable: {artifact.name}")
    suffix = Path(artifact.name).suffix or ".bin"
    destination = (
        get_settings().artifact_root.expanduser().resolve()
        / "object-cache"
        / stored.sha256[:2]
        / f"{stored.sha256}{suffix}"
    )
    hydrate_stored_object(stored, destination)
    artifact.path = str(destination)
    session.add(artifact)
    session.flush()
    return destination


def ensure_source_file_local(session: Session, source_file: SourceFile) -> Path:
    local = Path(source_file.stored_path).expanduser().resolve(strict=False)
    if local.is_file() and sha256_file(local) == source_file.sha256:
        return local
    if source_file.stored_object_id is None:
        raise ObjectStorageError(f"Source file has no durable object: {source_file.original_name}")
    stored = session.get(StoredObject, source_file.stored_object_id)
    if stored is None or stored.status != "ready":
        raise ObjectStorageError(f"Source file durable object is unavailable: {source_file.original_name}")
    hydrate_stored_object(stored, local)
    return local


def ensure_stored_path(
    session: Session,
    *,
    stored_object_id: uuid.UUID | None,
    local_path: Path,
    name: str,
    expected_sha256: str,
) -> Path:
    local = local_path.expanduser().resolve(strict=False)
    if local.is_file() and sha256_file(local) == expected_sha256:
        return local
    if stored_object_id is None:
        raise ObjectStorageError(f"No durable object is registered for {name}")
    stored = session.get(StoredObject, stored_object_id)
    if stored is None or stored.status != "ready" or stored.sha256 != expected_sha256:
        raise ObjectStorageError(f"The durable object is invalid for {name}")
    suffix = Path(name).suffix or ".bin"
    artifact_root = get_settings().artifact_root.expanduser().resolve()
    try:
        local.relative_to(artifact_root)
        destination = local
    except ValueError:
        destination = artifact_root / "object-cache" / stored.sha256[:2] / f"{stored.sha256}{suffix}"
    return hydrate_stored_object(stored, destination)


def archive_job_artifacts(session: Session, job_id: uuid.UUID) -> dict[str, int]:
    artifacts = session.scalars(select(Artifact).where(Artifact.job_id == job_id)).all()
    ready = errors = local = 0
    adapter = S3ObjectStorage(get_settings()) if get_settings().object_storage_enabled else None
    for artifact in artifacts:
        if artifact.storage_status == "ready" and artifact.stored_object_id:
            ready += 1
        elif archive_artifact(session, artifact, storage=adapter):
            ready += 1
        elif artifact.storage_status == "error":
            errors += 1
        else:
            local += 1
    session.commit()
    return {"ready": ready, "errors": errors, "local": local, "total": len(artifacts)}


__all__ = [
    "archive_artifact",
    "archive_job_artifacts",
    "archive_source_file",
    "bucket_for_role",
    "ensure_artifact_local",
    "ensure_source_file_local",
    "ensure_stored_path",
    "hydrate_stored_object",
    "object_key_for",
    "sha256_file",
    "store_path",
]
