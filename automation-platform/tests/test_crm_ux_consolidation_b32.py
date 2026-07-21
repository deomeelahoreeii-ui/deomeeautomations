from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_LAYOUT = ROOT / "apps/web/src/layouts/AppLayout.astro"
DATA_TABLE = ROOT / "apps/web/src/components/DataTable.astro"
MODAL = ROOT / "apps/web/src/components/ui/ModalDialog.astro"
CHECKBOX = ROOT / "apps/web/src/components/ui/CheckboxField.astro"
DIALOGS = ROOT / "apps/web/src/scripts/dialogs.ts"
TAXONOMY = ROOT / "apps/web/src/pages/crm/taxonomy/index.astro"
QUEUE = ROOT / "apps/web/src/pages/crm/replies/index.astro"
EDITOR = ROOT / "apps/web/src/pages/crm/replies/[id].astro"
CRM_CSS = ROOT / "apps/web/src/styles/crm.css"


def test_shared_modal_uses_explicit_non_submitting_close_controls() -> None:
    source = MODAL.read_text(encoding="utf-8")
    assert "<dialog" in source
    assert 'class:list={["crm-modal"' in source
    assert 'type="button"' in source
    assert "data-dialog-close" in source
    assert 'method="dialog"' not in source

    behavior = DIALOGS.read_text(encoding="utf-8")
    for token in ("showModal()", "dialog.close(returnValue)", 'event.key === "Escape"'):
        if token == 'event.key === "Escape"':
            # Native dialog cancellation supplies Escape handling; busy dialogs prevent it.
            assert 'dialog.addEventListener("cancel"' in behavior
        else:
            assert token in behavior
    assert "focusOrigins" in behavior


def test_checkbox_component_has_one_ordered_control_and_copy_grid() -> None:
    source = CHECKBOX.read_text(encoding="utf-8")
    assert 'class="checkbox checkbox-sm"' in source
    assert 'class:list={["crm-check-field"' in source
    assert "crm-check-copy" in source
    assert "form-check" not in source


def test_sidebar_is_collapsible_persistent_and_mobile_accessible() -> None:
    source = APP_LAYOUT.read_text(encoding="utf-8")
    for token in (
        'id="sidebar-collapse-toggle"',
        'id="mobile-nav-toggle"',
        'id="sidebar-scrim"',
        'tabindex="-1"',
        'automation-sidebar-collapsed',
        'dataset.sidebar = collapsed ? "collapsed" : "expanded"',
        'aria-controls="app-sidebar"',
    ):
        assert token in source


def test_data_table_supports_server_pagination_page_size_and_custom_cells() -> None:
    source = DATA_TABLE.read_text(encoding="utf-8")
    for token in (
        "pageSizeOptions",
        "manualPagination",
        "loadPage",
        "initialPageIndex",
        "initialPageSize",
        "setPageSize",
        "getVisibleRows",
        "data-page-size-select",
        "rowClassName",
        "onRender",
    ):
        assert token in source


def test_taxonomy_is_split_tree_detail_table_and_safe_dialog_forms() -> None:
    source = TAXONOMY.read_text(encoding="utf-8")
    for token in (
        'class="taxonomy-workspace"',
        'class="taxonomy-tree-list"',
        'id="subcategory-table"',
        "DataTable",
        "ModalDialog",
        "CheckboxField",
        'id="category-dialog"',
        'id="subcategory-dialog"',
        'id="merge-dialog"',
        'data-dialog-close>Cancel',
        "Complaint count, order, status and actions",
    ):
        assert token in source
    assert 'method="dialog"' not in source
    assert "form-check" not in source


def test_reply_queue_is_tanstack_driven_with_separate_number_and_preview() -> None:
    source = QUEUE.read_text(encoding="utf-8")
    for token in (
        "DataTable",
        'id="reply-queue-table"',
        'header: "Complaint No."',
        'header: "Complaint text"',
        'button.textContent = "View"',
        'id="complaint-preview-dialog"',
        "manualPagination: true",
        "pageSizeOptions={[25, 50, 100]}",
        "filterParams",
        "history.replaceState",
        'id="bulk-classification"',
        "Apply classification",
    ):
        assert token in source


def test_reply_editor_keeps_page_workspace_and_uses_context_tables_and_state_actions() -> None:
    source = EDITOR.read_text(encoding="utf-8")
    for token in (
        'class="editor-workspace"',
        'data-context-tab="complaint"',
        'data-context-tab="evidence"',
        'data-context-tab="revisions"',
        'data-context-tab="activity"',
        'id="editor-evidence-table"',
        'id="editor-revisions-table"',
        'id="editor-activity-table"',
        'id="full-complaint-dialog"',
        'id="revision-dialog"',
        "renderWorkflowActions",
        '"save-draft": ["Not Prepared", "Draft", "Rejected"]',
        '"approve-reply": status === "Pending Approval"',
        '"issue-reply": status === "Approved"',
        "requestedReturn.startsWith(\"/crm/replies\")",
    ):
        assert token in source


def test_crm_css_centers_dialog_and_provides_responsive_dark_layouts() -> None:
    source = CRM_CSS.read_text(encoding="utf-8")
    for token in (
        "dialog.crm-modal[open] { display: grid; place-items: center; }",
        "max-height: min(90dvh, 920px)",
        ".crm-modal-body { min-height: 0; overflow: auto;",
        ".taxonomy-workspace",
        ".reply-queue-table .data-table",
        ".editor-workspace",
        '[data-theme="automation-dark"] .crm32-page',
        "@media (max-width: 1040px)",
        "@media (max-width: 560px)",
    ):
        assert token in source
