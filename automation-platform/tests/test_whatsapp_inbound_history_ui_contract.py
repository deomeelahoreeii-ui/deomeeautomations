from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FETCH_PAGE = ROOT / "apps/web/src/pages/crm/intake/index.astro"
BATCH_PAGE = ROOT / "apps/web/src/pages/crm/intake/batches/index.astro"
DETAIL_PAGE = ROOT / "apps/web/src/pages/crm/intake/batches/[id].astro"
NAV = ROOT / "apps/web/src/components/CrmIntakeNav.astro"
LEGACY_FETCH_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-files.astro"
CRM_LAYOUT = ROOT / "apps/web/src/layouts/CrmLayout.astro"
WHATSAPP_LAYOUT = ROOT / "apps/web/src/layouts/WhatsAppLayout.astro"
APP_LAYOUT = ROOT / "apps/web/src/layouts/AppLayout.astro"


def test_crm_intake_workspace_is_split_into_focused_subnavigation() -> None:
    nav = NAV.read_text(encoding="utf-8")
    assert '"intake", "Capture"' in nav
    assert "Intake runs" in nav
    assert '"review", "Review"' in nav
    assert '"spreadsheets", "Spreadsheets"' in nav
    assert '"packages", "Packages"' in nav
    assert "/crm/intake/batches" in nav
    assert "/crm/intake/review" in nav


def test_legacy_whatsapp_intake_url_redirects_to_crm() -> None:
    source = LEGACY_FETCH_PAGE.read_text(encoding="utf-8")
    assert "Astro.redirect(`/crm/intake/${Astro.url.search}`, 308)" in source


def test_crm_owns_complaint_intake_and_whatsapp_retains_capture_health() -> None:
    crm_layout = CRM_LAYOUT.read_text(encoding="utf-8")
    whatsapp_layout = WHATSAPP_LAYOUT.read_text(encoding="utf-8")
    app_layout = APP_LAYOUT.read_text(encoding="utf-8")
    assert '["intake", "Complaint Intake", "/crm/intake/"]' in crm_layout
    assert '["inbound-health", "Inbound Health", "/whatsapp/inbound-storage"]' in whatsapp_layout
    assert "Inbound Files" not in whatsapp_layout
    assert 'active === "crm" }]} href="/crm/intake/"' in app_layout


def test_history_fetch_console_polls_persisted_batch_activity() -> None:
    source = FETCH_PAGE.read_text(encoding="utf-8")
    assert "Contact history" in source
    assert "actions.whatsapp.inboundBatchEvents" in source
    assert "actions.whatsapp.inboundBatch" in source
    assert "window.setInterval(() => void loadBatch(), 2000)" in source
    assert "Activity from the server will appear here" in source
    assert "const data = result.data as any" in source
    assert "currentBatchId = data.batch_id" in source


def test_batch_management_pages_expose_files_activity_and_retry() -> None:
    listing = BATCH_PAGE.read_text(encoding="utf-8")
    detail = DETAIL_PAGE.read_text(encoding="utf-8")
    assert "WhatsApp complaint captures" in listing
    assert "DataTable" in listing
    assert 'id="batch-search"' in listing
    assert 'id="batch-status"' in listing
    assert "actions.whatsapp.inboundBatches" in listing
    assert "Live server log" in detail
    assert "actions.whatsapp.inboundBatchEvents" in detail
    assert "actions.whatsapp.retryInboundBatchStorage" in detail
    assert "/items/${escapeHtml(x.id)}/download" in detail


def test_fetch_page_requires_managed_visible_profile_bridge() -> None:
    source = FETCH_PAGE.read_text(encoding="utf-8")
    assert 'state.textContent = bridgeReady ? "WhatsApp ready"' in source
    assert "Only messages and supported files received" in source
    assert "Sent-by-you messages will be excluded" in source


def test_fetch_page_supports_received_only_date_range_intake() -> None:
    source = FETCH_PAGE.read_text(encoding="utf-8")
    assert 'value="range"' in source
    assert 'id="inbound-date-from"' in source
    assert 'id="inbound-date-to"' in source
    assert "date_from: rangeFrom" in source
    assert "date_to: rangeTo" in source
    assert "bridgeSupportsDateRange" in source
    assert "files_stored" in source
    assert "files_failed" in source
    assert "terminalStatuses.has(batch.status)" in source
    assert 'id="inbound-media-pdf"' in source
    assert 'id="inbound-media-image"' in source
    assert 'id="inbound-media-spreadsheet"' in source
    assert "media_types: mediaTypes" in source


def test_spreadsheets_have_a_separate_row_level_review_workspace() -> None:
    listing = ROOT / "apps/web/src/pages/crm/intake/spreadsheets/index.astro"
    detail = ROOT / "apps/web/src/pages/crm/intake/spreadsheets/[id]/index.astro"
    list_source = listing.read_text(encoding="utf-8")
    detail_source = detail.read_text(encoding="utf-8")
    assert "DataTable" in list_source
    assert "spreadsheetIntakeBatches" in list_source
    assert "Rows are candidates, not cases" in list_source
    assert "spreadsheetIntakeBatch" in detail_source
    assert "reviewSpreadsheetIntakeRow" in detail_source
    assert "Promote to case" in detail_source
    assert "Nothing here is a complaint case yet" in detail_source
