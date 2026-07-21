from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_dispatch_dashboard_separates_request_and_submission_workflows() -> None:
    source = read("apps/web/src/pages/crm/dispatch/index.astro")
    assert "Request Compliance" in source
    assert "Submit Compliance" in source
    assert "Submit compliance to CEO/higher office" in source
    assert "Select all" in source
    assert "case_ids" in source
    assert "official_letter_ids" in source
    assert "/eligible-sources" in source


def test_destination_profile_modal_is_opaque_scrollable_and_has_visible_save_footer() -> None:
    routing = read("apps/web/src/pages/crm/dispatch/routing.astro")
    styles = read("apps/web/src/styles/crm.css")
    assert 'class="crm-modal-shell dispatch-profile-dialog"' in routing
    assert "Save destination profile" in routing
    assert "Office level / use" in routing
    assert "Allowed workflow directions" in routing
    assert "dialog.crm-modal > .crm-modal-shell" in styles
    assert "grid-template-rows: auto minmax(0, 1fr) auto" in styles
    assert ".dispatch-profile-dialog .crm-modal-footer" in styles


def test_reply_editor_supports_lower_office_reports_policy_and_evidence() -> None:
    editor = read("apps/web/src/pages/crm/replies/[id].astro")
    api = read("packages/crm_domain/crm_domain/reply_workspace_api.py")
    service = read("packages/crm_domain/crm_domain/reply_workspace.py")
    official = read("packages/crm_domain/crm_domain/official_letters.py")
    assert "Field / lower-office report" in editor
    assert "Lower-office reply" in editor
    assert "Policy / circular" in editor
    assert "dispatch_item_id" in editor
    assert '"/cases/{case_id}/documents"' in api
    assert "source_dispatch_item_id" in service
    for role in ('"report"', '"reply"', '"policy"', '"attachment"'):
        assert role in official


def test_batch_detail_links_compliance_request_back_to_reply_editor() -> None:
    source = read("apps/web/src/pages/crm/dispatch/batches/[id].astro")
    assert "DOWNWARD COMPLIANCE REQUEST" in source
    assert "UPWARD COMPLIANCE SUBMISSION" in source
    assert "Upload / Review Compliance" in source
    assert "dispatch_item_id=" in source
    assert "View Complaint Packet" in source


def test_migration_adds_reversible_two_way_dispatch_and_manual_case_files() -> None:
    source = read("alembic/versions/f1c3e5a7b902_crm_two_way_dispatch_and_case_files.py")
    assert 'revision: str = "f1c3e5a7b902"' in source
    assert 'down_revision: Union[str, None] = "ff29cbe4a5f0"' in source
    assert '"crm_dispatch_artifacts"' in source
    assert '"direction"' in source
    assert '"source_kind"' in source
    assert "manual_upload" in source
    assert "fk_crm_complaint_documents_dispatch_item" in source
    assert 'op.drop_table("crm_dispatch_artifacts")' in source
