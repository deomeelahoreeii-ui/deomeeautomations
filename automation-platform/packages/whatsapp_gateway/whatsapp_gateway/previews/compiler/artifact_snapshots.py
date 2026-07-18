from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import Any

from sqlalchemy import select

from automation_core.models import Artifact
from automation_core.config import get_settings
from automation_core.storage_catalog import store_path
from whatsapp_gateway.models import WhatsAppDispatchPreview, WhatsAppDispatchPreviewArtifact
from whatsapp_gateway.previews.artifact_storage import freeze_artifact, sha256_file
from whatsapp_gateway.previews.compiler.attachments import _artifact_role, _attachment_paths
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.errors import issue

class ArtifactSnapshotStore:
    def __init__(self, ctx: CompileContext, preview: WhatsAppDispatchPreview, dispatch_plan: list[dict[str, Any]]):
        self.ctx = ctx
        self.preview = preview
        self.plans_with_paths = [(plan, _attachment_paths(plan)) for plan in dispatch_plan]
        self.referenced_paths = {str(path) for _, paths in self.plans_with_paths for path in paths}
        job_artifacts = ctx.session.scalars(
            select(Artifact).where(Artifact.job_id == ctx.source_job.id).order_by(Artifact.created_at)
        ).all()
        self.artifacts_by_path = {str(Path(item.path).expanduser().resolve(strict=False)): item for item in job_artifacts}
        self.snapshots: dict[str, WhatsAppDispatchPreviewArtifact] = {}

    def snapshot_artifact(self, path: Path, artifact: Artifact | None = None) -> WhatsAppDispatchPreviewArtifact:
        source_path = path.resolve(strict=False)
        key = str(source_path)
        existing = self.snapshots.get(key)
        if existing:
            if key in self.referenced_paths:
                existing.role = "delivery"
            return existing
        problems: list[dict[str, Any]] = []
        checksum = ""
        size = 0
        artifact_status = "ready"
        frozen_path = source_path
        if not path.exists() or not path.is_file():
            artifact_status = "blocked"
            problems.append(issue("missing_attachment", "blocked", f"Attachment is missing: {path.name}"))
        else:
            size = path.stat().st_size
            checksum = sha256_file(path)
            if size == 0:
                artifact_status = "blocked"
                problems.append(issue("empty_attachment", "blocked", f"Attachment is empty: {path.name}"))
        if artifact is None and key in self.referenced_paths:
            artifact_status = "blocked"
            problems.append(issue("untracked_attachment", "blocked",
                f"Attachment is not registered to the source dry run: {path.name}"))
        if artifact is not None and artifact_status == "ready":
            frozen_path = freeze_artifact(path, checksum)
        stored_object_id = None
        storage_status = "local"
        storage_error = None
        if artifact_status == "ready" and get_settings().object_storage_enabled:
            try:
                stored = store_path(
                    self.ctx.session,
                    frozen_path,
                    module_key=self.ctx.application.key,
                    kind="delivery" if key in self.referenced_paths else _artifact_role(path, self.referenced_paths),
                    name=artifact.name if artifact else path.name,
                    content_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                )
                stored_object_id = stored.id if stored else None
                storage_status = "ready" if stored else "local"
                if stored is not None and artifact is not None:
                    artifact.stored_object_id = stored.id
                    artifact.sha256 = stored.sha256
                    artifact.size_bytes = stored.size_bytes
                    artifact.content_type = stored.content_type
                    artifact.storage_status = "ready"
                    artifact.storage_error = None
                    artifact.archived_at = stored.verified_at
                    self.ctx.session.add(artifact)
            except Exception as exc:
                storage_status = "error"
                storage_error = str(exc)[:4000]
                artifact_status = "blocked"
                problems.append(issue(
                    "object_storage_failed",
                    "blocked",
                    f"Could not archive immutable attachment {path.name}: {exc}",
                ))
        snapshot = WhatsAppDispatchPreviewArtifact(
            preview_id=self.preview.id, artifact_id=artifact.id if artifact else None,
            stored_object_id=stored_object_id,
            report_type_id=self.ctx.report_type.id, wing_id=self.ctx.wing.id,
            role=_artifact_role(path, self.referenced_paths), name=artifact.name if artifact else path.name,
            path_snapshot=str(frozen_path), mime_type=mimetypes.guess_type(path.name)[0] or "application/octet-stream",
            size_bytes=size, checksum_sha256=checksum, status=artifact_status, issues=problems,
            storage_status=storage_status, storage_error=storage_error,
        )
        self.ctx.session.add(snapshot)
        self.ctx.session.flush()
        self.snapshots[key] = snapshot
        return snapshot

    def snapshot_all(self) -> None:
        for _, paths in self.plans_with_paths:
            for path in paths:
                self.snapshot_artifact(path, self.artifacts_by_path.get(str(path)))
