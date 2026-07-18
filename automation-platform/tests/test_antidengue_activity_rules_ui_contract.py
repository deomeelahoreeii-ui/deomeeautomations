from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "apps/web/src/pages/antidengue/activity-rules.astro"
LAYOUT = ROOT / "apps/web/src/layouts/AntiDengueLayout.astro"
ACTIONS = ROOT / "apps/web/src/actions/index.ts"


def test_activity_rules_page_exposes_exact_distance_and_time_semantics() -> None:
    source = PAGE.read_text(encoding="utf-8")
    assert "Equal to or greater than (≥)" in source
    assert "Greater than (&gt;)" in source
    assert "Start — inclusive" in source
    assert "End — exclusive" in source
    assert "Test against latest report" in source
    assert "Automatic matches are not confirmed fake activities" in source


def test_activity_rules_are_reachable_and_api_backed() -> None:
    layout = LAYOUT.read_text(encoding="utf-8")
    actions = ACTIONS.read_text(encoding="utf-8")
    assert '["activity-rules", "Activity Rules", "/antidengue/activity-rules"]' in layout
    assert "previewActivityRule" in actions
    assert "/api/v1/antidengue/activity-rules/previews/evaluate" in actions
