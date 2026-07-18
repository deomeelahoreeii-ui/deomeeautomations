from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_TABLE = ROOT / "apps/web/src/components/DataTable.astro"
JURISDICTIONS = ROOT / "apps/web/src/pages/master-data/jurisdictions.astro"
SCHOOLS = ROOT / "apps/web/src/pages/master-data/schools.astro"
ACTIONS = ROOT / "apps/web/src/actions/index.ts"
DEV = ROOT / "scripts/dev.sh"


def test_data_table_replaces_pagination_state_when_page_changes() -> None:
    source = DATA_TABLE.read_text(encoding="utf-8")

    assert "this.pagination.pageIndex = next;" not in source
    assert "this.pagination.pageIndex = 0;" not in source
    assert "this.pagination.pageIndex = lastPage;" not in source
    assert "this.pagination = { ...this.pagination, pageIndex: next }" in source


def test_jurisdiction_markaz_links_to_filtered_schools_page() -> None:
    jurisdictions = JURISDICTIONS.read_text(encoding="utf-8")
    schools = SCHOOLS.read_text(encoding="utf-8")
    actions = ACTIONS.read_text(encoding="utf-8")

    assert "/master-data/schools?markaz_ref=" in jurisdictions
    assert 'get("markaz_ref")' in schools
    assert 'id="school-markaz"' in schools
    assert "markazRef: el<HTMLSelectElement>(\"school-markaz\").value" in schools
    assert 'markaz_ref: input.markazRef' in actions


def test_dev_api_reload_has_a_bounded_graceful_shutdown() -> None:
    source = DEV.read_text(encoding="utf-8")

    assert "--timeout-graceful-shutdown 5" in source
