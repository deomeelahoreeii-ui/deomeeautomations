from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIST_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-processing.astro"
DETAIL_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-processing/[id].astro"
BATCH_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-batches/[id].astro"
NAV = ROOT / "apps/web/src/components/WhatsAppInboundNav.astro"
DEV_SCRIPT = ROOT / "scripts/dev.sh"


def test_inbound_navigation_exposes_processing_workspace() -> None:
    source = NAV.read_text(encoding="utf-8")
    assert '"processing", "Processing & review"' in source
    assert '"/whatsapp/inbound-processing"' in source


def test_batch_page_can_start_dry_run_processing() -> None:
    source = BATCH_PAGE.read_text(encoding="utf-8")
    assert 'id="process-batch"' in source
    assert "createInboundProcessingRun" in source
    assert "paperless_check:true" in source


def test_processing_pages_show_crm_and_paperless_dry_run_contract() -> None:
    list_source = LIST_PAGE.read_text(encoding="utf-8")
    detail_source = DETAIL_PAGE.read_text(encoding="utf-8")
    assert "Dry-run only" in list_source
    assert "Confirmed CRM" in list_source
    assert "Paperless duplicates" in list_source
    assert "Live classifier log" in detail_source
    assert "104-6609317" in detail_source
    assert "Save review" in detail_source
    assert "Files grouped by CRM complaint number" in detail_source
    assert "complaint_groups" in detail_source
    assert "No Paperless uploads" in detail_source


def test_dev_script_requires_processing_worker_registration() -> None:
    source = DEV_SCRIPT.read_text(encoding="utf-8")
    assert "whatsapp_gateway.process_inbound_batch" in source
    assert "inbound processing tasks" in source
