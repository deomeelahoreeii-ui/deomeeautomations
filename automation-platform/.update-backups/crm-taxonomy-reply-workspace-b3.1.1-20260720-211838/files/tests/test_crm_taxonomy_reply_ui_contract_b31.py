from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LAYOUT = ROOT / "apps/web/src/layouts/CrmLayout.astro"
ACTIONS = ROOT / "apps/web/src/actions/index.ts"
MAIN = ROOT / "apps/api/automation_api/main.py"
TAXONOMY = ROOT / "apps/web/src/pages/crm/taxonomy/index.astro"
REPLIES = ROOT / "apps/web/src/pages/crm/replies/index.astro"
EDITOR = ROOT / "apps/web/src/pages/crm/replies/[id].astro"
CASE = ROOT / "apps/web/src/pages/crm/cases/[id].astro"
HELPDESK = ROOT / "apps/web/src/pages/crm/helpdesk/index.astro"
KNOWLEDGE = ROOT / "apps/web/src/pages/crm/knowledge/index.astro"
MIGRATION = ROOT / "alembic/versions/e9b3c5d7f104_crm_taxonomy_and_reply_workspace.py"


def test_crm_navigation_centers_reply_workflow_without_removing_bulk_tools() -> None:
    source = LAYOUT.read_text(encoding="utf-8")
    for label, href in (
        ("Reply Workspace", "/crm/replies/"),
        ("Taxonomy", "/crm/taxonomy/"),
        ("AI Reply Archive", "/crm/knowledge/"),
        ("Helpdesk Sync", "/crm/helpdesk/"),
        ("Bulk Tools", "/crm/"),
    ):
        assert label in source
        assert href in source


def test_taxonomy_page_exposes_full_safe_management_flow() -> None:
    source = TAXONOMY.read_text(encoding="utf-8")
    for token in (
        'id="category-dialog"',
        'id="subcategory-dialog"',
        'id="merge-dialog"',
        "+ New category",
        "+ New subcategory",
        "Deactivate",
        "Reactivate",
        "Merge",
        "AI eligible by default after approval",
        "Reply guidance",
        "Policy notes",
        "/api/v1/crm/taxonomy/categories",
        "/api/v1/crm/taxonomy/subcategories",
        "/sync",
    ):
        assert token in source
    assert "hard delete" not in source.lower()


def test_reply_queue_preserves_legacy_tools_and_adds_bulk_classification() -> None:
    source = REPLIES.read_text(encoding="utf-8")
    for token in (
        "Reply Queue",
        "Bulk CSV",
        "Formal Letters",
        "Apply classification",
        "/api/v1/crm/reply-workspace/cases",
        "/api/v1/crm/reply-workspace/classifications/bulk",
        "/api/v1/crm/replies/complaints.csv",
        "/api/v1/crm/replies/imports",
        "/api/v1/crm/replies/letter-packages",
    ):
        assert token in source


def test_reply_editor_supports_every_status_and_verified_archive_rules() -> None:
    source = EDITOR.read_text(encoding="utf-8")
    for token in (
        "Inquiry findings",
        "School / institution version",
        "Applicable policy / rule",
        "Final reply",
        "Save Selected Status",
        "Save Draft",
        "Submit for Approval",
        "Reject Reply",
        "Approve Reply",
        "Mark as Issued",
        "immutable approved revision",
        "/api/v1/crm/reply-workspace/cases/${caseId}/reply",
        "/api/v1/crm/reply-workspace/cases/${caseId}/classification",
        "Helpdesk could not be read",
    ):
        assert token in source
    assert 'ai.disabled=!approved' in source
    assert 'if(!approved)ai.checked=false' in source


def test_case_page_has_controlled_classification_and_reply_entry_points() -> None:
    source = CASE.read_text(encoding="utf-8")
    assert "Authoritative classification" in source
    assert "Save and sync" in source
    assert "/crm/taxonomy/" in source
    assert "/crm/replies/${caseId}/" in source
    assert "saveCaseClassification" in source
    assert ".catch(()=>({categories:[]}))" in source


def test_helpdesk_page_makes_routing_optional_and_reply_sync_primary() -> None:
    source = HELPDESK.read_text(encoding="utf-8")
    assert "Primary purpose" in source
    assert "Preview and pull latest workflow" in source
    assert "Optional team, tehsil and officer routing" in source
    assert "Routing is not required" in source
    assert "Open Reply Workspace" in source
    assert "AI Reply Archive" in source


def test_knowledge_archive_links_back_to_operational_records() -> None:
    source = KNOWLEDGE.read_text(encoding="utf-8")
    for token in ("reply_editor_url", "case_url", "helpdesk_ticket_url", "paperless_url"):
        assert token in source
    assert "Reply Workspace" in source


def test_actions_and_api_register_taxonomy_and_reply_workspace() -> None:
    actions = ACTIONS.read_text(encoding="utf-8")
    main = MAIN.read_text(encoding="utf-8")
    for token in (
        "createTaxonomyCategory",
        "updateTaxonomyCategory",
        "mergeTaxonomyCategory",
        "createTaxonomySubcategory",
        "mergeTaxonomySubcategory",
        "replyWorkspaceCases",
        "saveCaseClassification",
        "bulkClassifyCases",
        "saveCaseReply",
    ):
        assert token in actions
    assert "app.include_router(crm_taxonomy_router)" in main
    assert "app.include_router(crm_reply_workspace_router)" in main


def test_bundle_31_migration_is_additive_and_reversible() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    assert 'revision: str = "e9b3c5d7f104"' in source
    assert 'down_revision: Union[str, None] = "d6e9f2a4b705"' in source
    for table in (
        "crm_complaint_categories",
        "crm_complaint_subcategories",
        "crm_complaint_audit_events",
    ):
        assert f'op.create_table(\n        "{table}"' in source
        assert f'op.drop_table("{table}")' in source
    assert "frappe_reply_text_snapshot" in source
    assert "SELECT id, category, sub_category, frappe_ticket_id" in source
