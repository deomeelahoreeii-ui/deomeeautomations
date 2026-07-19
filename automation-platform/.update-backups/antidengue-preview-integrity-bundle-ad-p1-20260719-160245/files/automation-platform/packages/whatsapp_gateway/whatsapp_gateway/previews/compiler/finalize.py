from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import select

from whatsapp_gateway.models import WhatsAppDispatchPreview, WhatsAppDispatchPreviewDelivery
from whatsapp_gateway.previews.compiler.artifact_snapshots import ArtifactSnapshotStore
from whatsapp_gateway.previews.compiler.context import CompileContext


def finalize_preview(
    ctx: CompileContext, preview: WhatsAppDispatchPreview, store: ArtifactSnapshotStore,
    batch_issues: list[dict[str, Any]], configuration_snapshot: dict[str, Any],
) -> WhatsAppDispatchPreview:
    ctx.session.flush()
    deliveries = ctx.session.scalars(select(WhatsAppDispatchPreviewDelivery).where(
        WhatsAppDispatchPreviewDelivery.preview_id == preview.id)).all()
    preview.ready_count = sum(item.status == "ready" for item in deliveries)
    preview.warning_count = sum(problem.get("severity") == "warning" for item in deliveries for problem in item.issues) \
        + sum(item["severity"] == "warning" for item in batch_issues)
    preview.blocked_count = sum(problem.get("severity") == "blocked" for item in deliveries for problem in item.issues) \
        + sum(item["severity"] == "blocked" for item in batch_issues)
    preview.skipped_count = sum(item.status == "skipped" for item in deliveries)
    preview.delivery_count = len(deliveries)
    preview.artifact_count = len(store.snapshots)
    active_attachment_ids = {artifact_id for delivery in deliveries if delivery.status != "skipped"
        for artifact_id in delivery.attachment_ids}
    artifact_blocked = any(str(item.id) in active_attachment_ids and item.status == "blocked"
        for item in store.snapshots.values())
    batch_blocked = any(item["severity"] == "blocked" for item in batch_issues)
    preview.status = "blocked" if preview.blocked_count or artifact_blocked or batch_blocked else "ready"
    artifacts_by_id = {str(item.id): item for item in store.snapshots.values()}
    frozen_content = {
        "configuration": configuration_snapshot,
        "deliveries": [{
            "sequence": item.sequence, "target": item.target_jid, "message": item.message,
            "attachments": [artifacts_by_id[artifact_id].checksum_sha256 for artifact_id in item.attachment_ids
                if artifact_id in artifacts_by_id],
            "status": item.status, "issues": item.issues, "routing": item.routing_snapshot,
        } for item in sorted(deliveries, key=lambda value: value.sequence)],
    }
    preview.content_sha256 = hashlib.sha256(
        json.dumps(frozen_content, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    ctx.session.add(preview)
    ctx.session.commit()
    ctx.session.refresh(preview)
    return preview
