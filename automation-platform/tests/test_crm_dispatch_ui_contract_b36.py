from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_b36_registers_crm_dispatch_api_and_polymorphic_preview_source() -> None:
    main = read("apps/api/automation_api/main.py")
    api = read("packages/crm_domain/crm_domain/dispatch_api.py")
    previews = read("packages/whatsapp_gateway/whatsapp_gateway/persistence/previews.py")
    assert "crm_dispatch_router" in main
    assert 'prefix="/api/v1/crm/dispatch"' in api
    for route in (
        '"/profiles"',
        '"/rules"',
        '"/routing-test/{case_id}"',
        '"/batches"',
        '"/batches/{batch_id}/compile"',
        '"/items/{item_id}/route"',
    ):
        assert route in api
    assert 'source_kind: str = Field(default="antidengue_job"' in previews
    assert "source_reference_id" in previews
    assert "source_job_id: uuid.UUID | None" in previews


def test_b36_keeps_crm_business_history_separate_from_whatsapp_transport() -> None:
    models = read("packages/crm_domain/crm_domain/models.py")
    service = read("packages/crm_domain/crm_domain/dispatch.py")
    for model in (
        "class CrmDispatchRule",
        "class CrmDispatchBatch",
        "class CrmDispatchItem",
        "class CrmDispatchTarget",
    ):
        assert model in models
    assert 'source_kind="crm_dispatch_batch"' in service
    assert "WhatsAppDispatchPreviewDelivery" in service
    assert "WhatsAppDispatchApproval" in service
    assert "WhatsAppDelivery" in service
    assert "CrmOfficialLetterArtifact.kind == \"complete_pdf\"" in service
    assert "duplicate_dispatch" in service
    assert "summarize_preview_state" in service


def test_b36_exposes_dispatch_dashboard_routing_and_delivery_review_ux() -> None:
    layout = read("apps/web/src/layouts/CrmLayout.astro")
    dashboard = read("apps/web/src/pages/crm/dispatch/index.astro")
    routing = read("apps/web/src/pages/crm/dispatch/routing.astro")
    detail = read("apps/web/src/pages/crm/dispatch/batches/[id].astro")
    styles = read("apps/web/src/styles/crm.css")
    assert '["dispatch", "Dispatch", "/crm/dispatch/"]' in layout
    assert "Ready to dispatch" in dashboard
    assert "Create Dispatch Batch" in dashboard
    assert "Destination Profiles" in routing
    assert "Routing Rules" in routing
    assert "Test Routing" in routing
    assert "Freeze Exact Preview" in detail
    assert "Approve exact WhatsApp deliveries" in detail
    assert "Delivery audit" in detail
    assert ".crm-dispatch" in styles


def test_b36_official_letter_and_reply_workspace_have_dispatch_entry_points() -> None:
    prepare = read("apps/web/src/pages/crm/replies/[id]/official-letter.astro")
    register = read("apps/web/src/pages/crm/replies/official-letters/index.astro")
    queue = read("apps/web/src/pages/crm/replies/index.astro")
    assert "Prepare Dispatch" in prepare
    assert "/crm/dispatch/?letter_id=" in prepare
    assert "Dispatch" in register
    assert 'href="/crm/dispatch/"' in queue


def test_b36_migration_is_linear_additive_and_reversible() -> None:
    migration = read("alembic/versions/fd07e9a1c408_crm_complaint_dispatch_routing.py")
    assert 'revision: str = "fd07e9a1c408"' in migration
    assert 'down_revision: Union[str, None] = "fbe5c7d9e286"' in migration
    for table in (
        "crm_dispatch_rules",
        "crm_dispatch_batches",
        "crm_dispatch_items",
        "crm_dispatch_targets",
    ):
        assert f'op.create_table(\n        "{table}"' in migration
        assert f'op.drop_table("{table}")' in migration
    assert '"source_job_id",\n        existing_type=sa.Uuid(),\n        nullable=True' in migration
    assert '"source_job_id",\n        existing_type=sa.Uuid(),\n        nullable=False' in migration
