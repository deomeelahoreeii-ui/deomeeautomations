from __future__ import annotations

import hashlib
import json
from typing import Any

from sqlalchemy import select

from whatsapp_gateway.models import WhatsAppDispatchPreview, WhatsAppDispatchPreviewDelivery
from whatsapp_gateway.previews.compiler.artifact_snapshots import ArtifactSnapshotStore
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.state import apply_preview_state, summarize_preview_state


def finalize_preview(
    ctx: CompileContext, preview: WhatsAppDispatchPreview, store: ArtifactSnapshotStore,
    batch_issues: list[dict[str, Any]], configuration_snapshot: dict[str, Any],
) -> WhatsAppDispatchPreview:
    ctx.session.flush()
    deliveries = ctx.session.scalars(select(WhatsAppDispatchPreviewDelivery).where(
        WhatsAppDispatchPreviewDelivery.preview_id == preview.id)).all()
    summary = summarize_preview_state(
        deliveries, batch_issues, store.snapshots.values()
    )
    apply_preview_state(preview, summary)
    preview.artifact_count = len(store.snapshots)
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
