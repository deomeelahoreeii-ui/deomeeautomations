from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CRM_LAYOUT = ROOT / "apps/web/src/layouts/CrmLayout.astro"


def test_crm_layout_preserves_intake_ownership_contract_and_new_workspaces() -> None:
    source = CRM_LAYOUT.read_text(encoding="utf-8")
    assert '["intake", "Complaint Intake", "/crm/intake/"]' in source
    assert '["cases", "Complaint Cases", "/crm/cases/"]' in source
    assert '["replies", "Reply Workspace", "/crm/replies/"]' in source
    assert '["taxonomy", "Taxonomy", "/crm/taxonomy/"]' in source
    assert '["knowledge", "AI Reply Archive", "/crm/knowledge/"]' in source
    assert '["helpdesk", "Helpdesk Sync", "/crm/helpdesk/"]' in source
    assert 'bulkToolPages.includes(page as typeof bulkToolPages[number])' in source
