from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FETCH_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-files.astro"
BATCH_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-batches.astro"
DETAIL_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-batches/[id].astro"
NAV = ROOT / "apps/web/src/components/WhatsAppInboundNav.astro"


def test_inbound_workspace_is_split_into_focused_subnavigation() -> None:
    nav = NAV.read_text(encoding="utf-8")
    assert "Fetch files" in nav
    assert "Inbound batches" in nav
    assert "Download packages" in nav
    assert "Object storage" in nav
    assert "/whatsapp/inbound-batches" in nav
    assert "/whatsapp/inbound-storage" in nav


def test_history_fetch_console_polls_persisted_batch_activity() -> None:
    source = FETCH_PAGE.read_text(encoding="utf-8")
    assert "Live batch activity" in source
    assert "actions.whatsapp.inboundBatchEvents" in source
    assert "actions.whatsapp.inboundBatch" in source
    assert "window.setInterval(()=>void loadBatch(),2000)" in source
    assert "Activity from the server will appear here" in source
    assert "data.batch_id" not in source  # the response is assigned before use
    assert "currentBatchId=d.batch_id" in source


def test_batch_management_pages_expose_files_activity_and_retry() -> None:
    listing = BATCH_PAGE.read_text(encoding="utf-8")
    detail = DETAIL_PAGE.read_text(encoding="utf-8")
    assert "Historical fetch runs" in listing
    assert "actions.whatsapp.inboundBatches" in listing
    assert "Live server log" in detail
    assert "actions.whatsapp.inboundBatchEvents" in detail
    assert "actions.whatsapp.retryInboundBatchStorage" in detail
    assert "/items/${escapeHtml(x.id)}/download" in detail


def test_fetch_page_requires_managed_visible_profile_bridge() -> None:
    source = FETCH_PAGE.read_text(encoding="utf-8")
    assert 'state.textContent=bridgeReady?"Managed browser ready"' in source
    assert "Managed Brave page ready" in source
    assert "Historical files are loaded by the managed whatsapp-web.js browser" in source
