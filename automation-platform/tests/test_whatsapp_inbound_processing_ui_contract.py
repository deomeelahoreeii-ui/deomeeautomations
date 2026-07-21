from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIST_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/index.astro"
DETAIL_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/[id].astro"
REVIEW_OVERVIEW_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/[id]/overview.astro"
REVIEW_READY_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/[id]/ready.astro"
REVIEW_MANUAL_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/[id]/manual.astro"
REVIEW_EXISTING_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/[id]/existing.astro"
REVIEW_ITEM_PAGE = ROOT / "apps/web/src/pages/crm/intake/review/[id]/items/[item].astro"
REVIEW_GROUP_TABLE = ROOT / "apps/web/src/components/CrmReviewGroupTable.astro"
BATCH_LIST_PAGE = ROOT / "apps/web/src/pages/crm/intake/batches/index.astro"
BATCH_PAGE = ROOT / "apps/web/src/pages/crm/intake/batches/[id].astro"
NAV = ROOT / "apps/web/src/components/CrmIntakeNav.astro"
LEGACY_REVIEW_PAGE = ROOT / "apps/web/src/pages/whatsapp/inbound-processing.astro"
DEV_SCRIPT = ROOT / "scripts/dev.sh"
CASE_PAGE = ROOT / "apps/web/src/pages/crm/cases/[id].astro"
CASE_LIST_PAGE = ROOT / "apps/web/src/pages/crm/cases/index.astro"
CASE_OVERVIEW_PAGE = ROOT / "apps/web/src/pages/crm/cases/[id]/overview.astro"
CASE_EVIDENCE_PAGE = ROOT / "apps/web/src/pages/crm/cases/[id]/evidence.astro"
CASE_PUBLICATION_PAGE = ROOT / "apps/web/src/pages/crm/cases/[id]/publication.astro"
CASE_TABLE = ROOT / "apps/web/src/components/CrmCaseTable.astro"
PUBLICATION_PAGE = ROOT / "apps/web/src/pages/crm/cases/publication/index.astro"
REPLY_PAGE = ROOT / "apps/web/src/pages/crm/replies/index.astro"
BULK_REPLY_PAGE = ROOT / "apps/web/src/pages/crm/replies/bulk/index.astro"


def test_crm_intake_navigation_exposes_review_workspace() -> None:
    source = NAV.read_text(encoding="utf-8")
    assert '"review", "Review"' in source
    assert '"/crm/intake/review"' in source


def test_legacy_whatsapp_review_url_redirects_to_crm() -> None:
    source = LEGACY_REVIEW_PAGE.read_text(encoding="utf-8")
    assert "Astro.redirect(`/crm/intake/review${Astro.url.search}`, 308)" in source


def test_batch_page_can_start_dry_run_processing() -> None:
    source = BATCH_PAGE.read_text(encoding="utf-8")
    assert 'id="process-batch"' in source
    assert "createInboundProcessingRun" in source
    assert "paperless_check:true" in source
    assert "`/crm/intake/review/${run.id}/overview`" in source


def test_intake_and_review_pages_show_bidirectional_run_identity() -> None:
    batch_list_source = BATCH_LIST_PAGE.read_text(encoding="utf-8")
    batch_detail_source = BATCH_PAGE.read_text(encoding="utf-8")
    review_list_source = LIST_PAGE.read_text(encoding="utf-8")
    review_detail_source = REVIEW_OVERVIEW_PAGE.read_text(encoding="utf-8")

    assert "row.original.processing_run_code" in batch_list_source
    assert "Open review →" in batch_list_source
    assert 'id="open-processing-run"' in batch_detail_source
    assert "`Source ${row.original.batch_code}`" in review_list_source
    assert 'id="run-source"' in review_detail_source
    assert "`/crm/intake/batches/${data.batch_id}`" in review_detail_source
    assert "legacyTab" in DETAIL_PAGE.read_text(encoding="utf-8")


def test_processing_pages_show_crm_and_paperless_dry_run_contract() -> None:
    list_source = LIST_PAGE.read_text(encoding="utf-8")
    overview_source = REVIEW_OVERVIEW_PAGE.read_text(encoding="utf-8")
    ready_source = REVIEW_READY_PAGE.read_text(encoding="utf-8")
    existing_source = REVIEW_EXISTING_PAGE.read_text(encoding="utf-8")
    table_source = REVIEW_GROUP_TABLE.read_text(encoding="utf-8")
    assert "Review is safe and reversible" in list_source
    assert "Confirmed CRM" in list_source
    assert "Existing in Paperless" in list_source
    assert "Classifier activity and technical details" in overview_source
    assert "complaint_groups" in overview_source
    assert "No Paperless upload occurs here" in ready_source
    assert "No new upload is required" in existing_source
    assert "status of that existing Paperless record" in existing_source
    assert "inboundComplaintGroups" in table_source
    assert "DataTable" in table_source
    assert "data-paperless-filter" in table_source


def test_manual_review_links_documents_to_resolved_complaint_cases() -> None:
    manual_source = REVIEW_MANUAL_PAGE.read_text(encoding="utf-8")
    source = REVIEW_ITEM_PAGE.read_text(encoding="utf-8")
    assert "CrmManualItemsTable" in manual_source
    assert "Confirm the complaint relationship" in source
    assert "Checking complaint registry…" in source
    assert "Existing CRM case" in source
    assert "Save and link evidence" in source
    assert "Approve and create case" not in source


def test_crm_case_page_collapses_identical_captures_into_one_evidence_card() -> None:
    redirect_source = CASE_PAGE.read_text(encoding="utf-8")
    overview_source = CASE_OVERVIEW_PAGE.read_text(encoding="utf-8")
    evidence_source = CASE_EVIDENCE_PAGE.read_text(encoding="utf-8")
    assert "/overview" in redirect_source
    assert "unique file" in overview_source
    assert "duplicate captures" in overview_source
    assert "duplicate_capture_count" in evidence_source
    assert "identical captures" in evidence_source
    assert "DataTable" in evidence_source


def test_approved_cases_can_be_verified_and_batch_published_to_crm_pending() -> None:
    list_source = CASE_LIST_PAGE.read_text(encoding="utf-8")
    table_source = CASE_TABLE.read_text(encoding="utf-8")
    detail_source = CASE_PUBLICATION_PAGE.read_text(encoding="utf-8")
    review_source = REVIEW_GROUP_TABLE.read_text(encoding="utf-8")
    publication_source = PUBLICATION_PAGE.read_text(encoding="utf-8")

    assert "CrmCaseTable" in list_source
    assert "publication_ready" in table_source
    assert "publishComplaintBatch" in table_source
    assert "Paperless Publication" in publication_source
    assert "Publication is explicit" in publication_source
    assert "approved_in_other_runs" in review_source
    assert "publication_blockers" in detail_source
    assert "Publish approved package" in detail_source
    assert "Paperless checks do not upload" in detail_source


def test_native_reply_workspace_exposes_batch_export_import_and_letter_generation() -> None:
    source = REPLY_PAGE.read_text(encoding="utf-8")
    bulk_source = BULK_REPLY_PAGE.read_text(encoding="utf-8")
    case_source = CASE_OVERVIEW_PAGE.read_text(encoding="utf-8")
    assert 'href="/crm/replies/bulk/"' in source
    assert "Export batch" in bulk_source
    assert "Validate before changing replies" in bulk_source
    assert "Formal-letter batch" in bulk_source
    assert "/api/v1/crm/bulk-operations/export-batches" in bulk_source
    assert "/api/v1/crm/bulk-operations/import-batches/validate" in bulk_source
    assert "/api/v1/crm/bulk-operations/letter-batches" in bulk_source
    assert "Write reply" in case_source


def test_dev_script_requires_processing_worker_registration() -> None:
    source = DEV_SCRIPT.read_text(encoding="utf-8")
    assert "whatsapp_gateway.process_inbound_batch" in source
    assert "inbound processing tasks" in source
