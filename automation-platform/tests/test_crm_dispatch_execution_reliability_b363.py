from __future__ import annotations

from pathlib import Path
import asyncio
import uuid

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_dispatch_queue_uses_the_datatable_load_page_contract() -> None:
    source = read("apps/web/src/pages/crm/dispatch/index.astro")
    assert "manualPagination:true,loadPage" in source
    assert "pageLoader:loadPage" not in source
    assert 'href="/crm/dispatch/batches/"' in source
    assert 'href="/crm/dispatch/deliveries/"' in source
    assert "Promise.allSettled" in source


def test_dispatch_has_real_batch_and_delivery_register_pages() -> None:
    batches = read("apps/web/src/pages/crm/dispatch/batches/index.astro")
    deliveries = read("apps/web/src/pages/crm/dispatch/deliveries/index.astro")
    api = read("packages/crm_domain/crm_domain/dispatch_api.py")
    service = read("packages/crm_domain/crm_domain/dispatch.py")
    assert 'manualPagination:true,loadPage' in batches
    assert '/api/v1/crm/dispatch/batches?' in batches
    assert 'manualPagination:true,loadPage' in deliveries
    assert '/api/v1/crm/dispatch/deliveries?' in deliveries
    assert '@router.get("/deliveries")' in api
    assert "def list_delivery_history(" in service


def test_crm_approval_uses_one_canonical_backend_operation_and_reports_real_queue_counts() -> None:
    page = read("apps/web/src/pages/crm/dispatch/batches/[id].astro")
    api = read("packages/crm_domain/crm_domain/dispatch_api.py")
    service = read("packages/crm_domain/crm_domain/dispatch.py")
    assert '/api/v1/crm/dispatch/batches/${batchId}/approve' in page
    assert 'for(const p of pending){await api(`/api/v1/whatsapp/previews/' not in page
    assert '@router.post("/batches/{batch_id}/approve")' in api
    assert "from whatsapp_gateway.previews.approval import approve_preview" in api
    assert "summary.eligible_delivery_count <= 0" in api
    assert '"queued": queued' in api
    assert "No new WhatsApp delivery was queued" in service
    assert 'target.business_status = "blocked"' in service


def test_whatsapp_preview_is_source_aware_and_null_safe_for_crm() -> None:
    detail = read("apps/web/src/pages/whatsapp/previews/[id]/index.astro")
    deliveries = read("apps/web/src/pages/whatsapp/previews/[id]/deliveries.astro")
    listing = read("apps/web/src/pages/whatsapp/previews/index.astro")
    assert "Immutable WhatsApp routing snapshot" in detail
    assert 'data.source_kind === "crm_dispatch_batch"' in detail
    assert 'data.source_job_id.slice' not in detail
    assert 'data.source_reference_id.slice' not in detail
    assert 'const shortIdentifier = (value) =>' in detail
    assert 'shortIdentifier(data.source_job_id)' in detail
    assert 'shortIdentifier(data.source_reference_id)' in detail
    assert "data.eligible_delivery_count > 0" in detail
    assert "No new delivery is eligible" in detail
    assert "item.attachments || []" in deliveries
    assert "checksum unavailable" in deliveries
    assert "supported platform workflows" in listing


def test_preview_artifact_download_validates_the_actual_source_domain() -> None:
    source = read("packages/whatsapp_gateway/whatsapp_gateway/previews/artifacts.py")
    assert "def _artifact_registered_to_source" in source
    assert 'preview.source_kind == "antidengue_job"' in source
    assert 'preview.source_kind == "crm_dispatch_batch"' in source
    assert "CrmDispatchArtifact.batch_id == preview.source_reference_id" in source
    assert "Artifact is not registered to the frozen preview source" in source
    assert "Artifact is not registered to the source dry run" not in source


def test_terminal_whatsapp_delivery_reconciles_the_immutable_source_workflow() -> None:
    approved = read("packages/whatsapp_gateway/whatsapp_gateway/dispatch/approved_delivery.py")
    retry = read("packages/whatsapp_gateway/whatsapp_gateway/dispatch/retry_delivery.py")
    router = read(
        "packages/whatsapp_gateway/whatsapp_gateway/dispatch/source_reconciliation.py"
    )
    dispatch = read("packages/crm_domain/crm_domain/dispatch.py")

    assert "reconcile_source_after_terminal_delivery(" in approved
    assert "reconcile_source_after_terminal_delivery(" in retry
    assert 'preview.source_kind != "crm_dispatch_batch"' in router
    assert "CrmDispatchService(session, Settings()).refresh" in router
    assert "def _sync_submitted_items_to_paperless(" in dispatch
    assert "client.set_document_status(" in dispatch


def test_terminal_delivery_wrapper_preserves_result_and_invokes_source(monkeypatch) -> None:
    from whatsapp_gateway.dispatch import source_reconciliation

    approval_id = uuid.uuid4()
    observed: list[uuid.UUID] = []

    async def publisher(value: uuid.UUID, _job_id: str) -> dict[str, int]:
        return {"delivered": 1, "failed": 0}

    monkeypatch.setattr(
        source_reconciliation,
        "reconcile_approval_source",
        observed.append,
    )
    wrapped = source_reconciliation.reconcile_source_after_terminal_delivery(publisher)

    result = asyncio.run(wrapped(approval_id, "job-1"))

    assert result == {"delivered": 1, "failed": 0}
    assert observed == [approval_id]
