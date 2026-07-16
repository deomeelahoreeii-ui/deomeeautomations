from whatsapp_gateway.models import WhatsAppDispatchPreview
from whatsapp_gateway.previews.compiler.artifact_snapshots import ArtifactSnapshotStore
from whatsapp_gateway.previews.compiler.context import CompileContext
from whatsapp_gateway.previews.compiler.delivery_builder import persist_delivery
from whatsapp_gateway.previews.compiler.delivery_state import initialize_delivery_state
from whatsapp_gateway.previews.compiler.delivery_validation import (
    render_delivery_message, resolve_recipient, validate_attachments, validate_source_status,
)

def persist_deliveries(ctx: CompileContext, preview: WhatsAppDispatchPreview, store: ArtifactSnapshotStore) -> None:
    seen_signatures: set[str] = set()
    for sequence, (plan, attachment_paths) in enumerate(store.plans_with_paths, start=1):
        state = initialize_delivery_state(sequence, plan, attachment_paths)
        validate_source_status(state)
        resolve_recipient(ctx, state)
        validate_attachments(ctx, state, store)
        render_delivery_message(ctx, state)
        persist_delivery(ctx, preview, state, seen_signatures)
