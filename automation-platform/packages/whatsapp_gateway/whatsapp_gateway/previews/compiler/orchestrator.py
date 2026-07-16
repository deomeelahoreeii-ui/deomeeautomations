from __future__ import annotations

import uuid

from sqlmodel import Session

from whatsapp_gateway.models import WhatsAppDispatchPreview
from whatsapp_gateway.previews.compiler.artifact_snapshots import ArtifactSnapshotStore
from whatsapp_gateway.previews.compiler.context import load_compile_context
from whatsapp_gateway.previews.compiler.deliveries import persist_deliveries
from whatsapp_gateway.previews.compiler.finalize import finalize_preview
from whatsapp_gateway.previews.compiler.plans import build_dispatch_plan
from whatsapp_gateway.previews.compiler.preview_record import configuration_snapshot, create_preview_record

def compile_antidengue_preview(
    session: Session, *, source_job_id: uuid.UUID, dispatch_profile_id: uuid.UUID, created_by: str = "web"
) -> WhatsAppDispatchPreview:
    ctx = load_compile_context(session, source_job_id=source_job_id, dispatch_profile_id=dispatch_profile_id)
    plan_result = build_dispatch_plan(ctx)
    snapshot = configuration_snapshot(ctx)
    preview = create_preview_record(ctx, batch_issues=plan_result.batch_issues, snapshot=snapshot, created_by=created_by)
    artifact_store = ArtifactSnapshotStore(ctx, preview, plan_result.dispatch_plan)
    artifact_store.snapshot_all()
    persist_deliveries(ctx, preview, artifact_store)
    return finalize_preview(ctx, preview, artifact_store, plan_result.batch_issues, snapshot)
