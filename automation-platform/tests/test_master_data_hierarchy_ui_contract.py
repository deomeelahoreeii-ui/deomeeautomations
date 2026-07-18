from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "apps/web/src/pages/master-data/hierarchy.astro"
LAYOUT = ROOT / "apps/web/src/layouts/MasterDataLayout.astro"
ACTIONS = ROOT / "apps/web/src/actions/index.ts"
PACKAGE = ROOT / "apps/web/package.json"
SCHOOLS = ROOT / "apps/web/src/pages/master-data/schools.astro"
MARKAZES = ROOT / "apps/web/src/pages/master-data/markazes.astro"
DIRECTORY = ROOT / "apps/web/src/pages/master-data/hierarchy/[view].astro"


def test_hierarchy_is_a_dedicated_master_data_page_using_d3() -> None:
    page = PAGE.read_text(encoding="utf-8")
    layout = LAYOUT.read_text(encoding="utf-8")
    package = PACKAGE.read_text(encoding="utf-8")

    assert '["hierarchy", "Hierarchy", "/master-data/hierarchy"]' in layout
    assert 'page="hierarchy"' in page
    assert 'from "d3"' in page
    assert '"d3"' in package
    assert 'id="hierarchy-chart"' in page
    assert 'id="hierarchy-list"' in page
    assert "Expand all" in page and "Collapse to DEOs" in page and "Fit chart" in page


def test_hierarchy_loads_authoritative_api_and_links_to_scoped_schools() -> None:
    page = PAGE.read_text(encoding="utf-8")
    actions = ACTIONS.read_text(encoding="utf-8")
    schools = SCHOOLS.read_text(encoding="utf-8")

    assert "actions.masterData.hierarchy" in page
    assert 'api(`/api/v1/master-data/hierarchy?${params}`)' in actions
    assert "vacant_ddeo_posts" in page
    assert "node.markazes" in page
    assert 'get("tehsil_ref")' in schools
    assert 'get("markaz_ref")' in schools


def test_every_hierarchy_metric_links_to_a_focused_directory() -> None:
    page = PAGE.read_text(encoding="utf-8")
    directory = DIRECTORY.read_text(encoding="utf-8")

    for href in (
        "/master-data/hierarchy/ceo-offices", "/master-data/hierarchy/deo-offices",
        "/master-data/hierarchy/ddeos", "/master-data/hierarchy/aeos",
        "/master-data/markazes", "/master-data/schools",
        "/master-data/hierarchy/vacant-ddeo-posts",
        "/master-data/markazes?coverage=unassigned",
    ):
        assert href in page
    assert "hierarchy-stat-link" in page
    assert '"ceo-offices"' in directory and '"vacant-ddeo-posts"' in directory
    assert "coverage_href" in directory


def test_markazes_page_has_stats_coverage_filters_and_school_links() -> None:
    page = MARKAZES.read_text(encoding="utf-8")
    actions = ACTIONS.read_text(encoding="utf-8")
    layout = LAYOUT.read_text(encoding="utf-8")

    assert '["markazes", "Markazes", "/master-data/markazes"]' in layout
    assert 'page="markazes"' in page
    assert "stats.by_wing" in page
    assert 'value="unassigned"' in page
    assert "row.original.href" in page
    assert "actions.masterData.markazes" in page
    assert 'api(`/api/v1/master-data/markazes?${params}`)' in actions
