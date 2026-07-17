from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIST_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/index.astro"
DETAIL_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/[id].astro"
BATCH_LIST_PAGE = ROOT / "apps/web/src/pages/crm/intake/batches/index.astro"
BATCH_PAGE = ROOT / "apps/web/src/pages/crm/intake/batches/[id].astro"
NAV = ROOT / "apps/web/src/components/CrmIntakeNav.astro"
LEGACY_REVIEW_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-processing.astro"
DEV_SCRIPT = ROOT / "scripts/dev.sh"
CASE_PAGE = ROOT / "apps/web/src/pages/crm/cases/[id].astro"
CASE_LIST_PAGE = ROOT / "apps/web/src/pages/crm/cases/index.astro"


def test_crm_intake_navigation_exposes_review_workspace() -> None:
    source = NAV.read_text(encoding="utf-8")
    assert '"review", "Review"' in source
    assert '"/crm/intake/review"' in source


def test_legacy_whatsapp_review_url_redirects_to_crm() -> None:
    source = LEGACY_REVIEW_PAGE.read_text(encoding="utf-8")
    assert 'Astro.redirect(`/crm/intake/review${Astro.url.search}`, 308)' in source


def test_batch_page_can_start_dry_run_processing() -> None:
    source = BATCH_PAGE.read_text(encoding="utf-8")
    assert 'id="process-batch"' in source
    assert "createInboundProcessingRun" in source
    assert "paperless_check:true" in source
    assert "`/crm/intake/review/${run.id}`" in source


def test_intake_and_review_pages_show_bidirectional_run_identity() -> None:
    batch_list_source = BATCH_LIST_PAGE.read_text(encoding="utf-8")
    batch_detail_source = BATCH_PAGE.read_text(encoding="utf-8")
    review_list_source = LIST_PAGE.read_text(encoding="utf-8")
    review_detail_source = DETAIL_PAGE.read_text(encoding="utf-8")

    assert "Review ${escapeHtml(x.processing_run_code)}" in batch_list_source
    assert "Open review →" in batch_list_source
    assert 'id="open-processing-run"' in batch_detail_source
    assert "Source intake ${escapeHtml(x.batch_code)}" in review_list_source
    assert "Open source intake" in review_list_source
    assert 'id="processing-source-batch"' in review_detail_source
    assert "`/crm/intake/batches/${d.batch_id}`" in review_detail_source


def test_processing_pages_show_crm_and_paperless_dry_run_contract() -> None:
    list_source = LIST_PAGE.read_text(encoding="utf-8")
    detail_source = DETAIL_PAGE.read_text(encoding="utf-8")
    assert "Safe review workspace" in list_source
    assert "Confirmed CRM" in list_source
    assert "Paperless duplicates" in list_source
    assert "Live classifier log" in detail_source
    assert "104-6609317" in detail_source
    assert "Save review" in detail_source
    assert "Files grouped by CRM complaint number" in detail_source
    assert "complaint_groups" in detail_source
    assert "No Paperless uploads" in detail_source


def test_manual_review_links_documents_to_resolved_complaint_cases() -> None:
    source = DETAIL_PAGE.read_text(encoding="utf-8")
    assert '<style is:global>' in source
    assert "Checking complaint registry" in source
    assert "Existing complaint" in source
    assert "Link attachment to case" in source
    assert "Create review case & link attachment" in source
    assert "Approve and create case" not in source


def test_crm_case_page_collapses_identical_captures_into_one_evidence_card() -> None:
    source = CASE_PAGE.read_text(encoding="utf-8")
    assert "duplicate_of_document_id" in source
    assert "unique file" in source
    assert "identical capture" in source


def test_approved_cases_can_be_verified_and_batch_published_to_crm_pending() -> None:
    list_source = CASE_LIST_PAGE.read_text(encoding="utf-8")
    detail_source = CASE_PAGE.read_text(encoding="utf-8")
    review_source = DETAIL_PAGE.read_text(encoding="utf-8")

    assert "publication_ready" in list_source
    assert "publishComplaintBatch" in list_source
    assert "Source: CRM Portal and Status: Pending" in list_source
    assert "Paperless publication center" in list_source
    assert "All unique cases" in list_source
    assert "open-publication-center" in list_source
    assert "Paperless publication center" in review_source
    assert "approved_in_other_runs" in review_source
    assert "caseStatistics" in review_source
    assert "publication_blockers" in detail_source
    assert "Ready for CRM Pending" in detail_source


def test_dev_script_requires_processing_worker_registration() -> None:
    source = DEV_SCRIPT.read_text(encoding="utf-8")
    assert "whatsapp_gateway.process_inbound_batch" in source
    assert "inbound processing tasks" in source
