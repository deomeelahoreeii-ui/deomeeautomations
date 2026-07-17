from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "apps/web/src/pages/index.astro"
SCHEDULES = ROOT / "apps/web/src/pages/antidengue/schedules.astro"
HISTORY = ROOT / "apps/web/src/pages/antidengue/run-history.astro"
LAYOUT = ROOT / "apps/web/src/layouts/AntiDengueLayout.astro"
DEV = ROOT / "scripts/dev.sh"


def test_dashboard_retires_direct_live_send_and_uses_safe_orchestration() -> None:
    source = DASHBOARD.read_text(encoding="utf-8")
    assert "Test run" in source
    assert "Send now" in source
    assert "Sending skipped — test mode" in source
    assert "Routing profiles:" in source
    assert "auto_send_when_clean" in source
    assert "/api/v1/antidengue/executions" in source
    assert 'value="live"' not in source
    assert "Start live send" not in source


def test_dashboard_persists_and_restores_profile_selection() -> None:
    source = DASHBOARD.read_text(encoding="utf-8")
    assert 'profileSelectionKey = "antidengue.dashboard.dispatch-profile-ids.v1"' in source
    assert "restoreProfileSelection();" in source
    assert "persistProfileSelection();" in source
    assert "executions[0].dispatch_profile_ids" in source
    assert "void loadProfiles()" in source


def test_schedule_and_run_history_navigation_is_present() -> None:
    layout = LAYOUT.read_text(encoding="utf-8")
    assert '"schedules", "Schedules", "/antidengue/schedules"' in layout
    assert '"run-history", "Run History", "/antidengue/run-history"' in layout
    assert '"dispatch-plans", "Dispatch Plans", "/antidengue/dispatch-plans"' in layout
    schedules = SCHEDULES.read_text(encoding="utf-8")
    assert "Preview only" in schedules
    assert "Auto-send when clean" in schedules
    assert "Run now" in schedules
    assert "Select all visible" in schedules
    assert "scheduler.scheduler_enabled" in schedules
    history = HISTORY.read_text(encoding="utf-8")
    assert "Persistent combined activity" in history


def test_dev_starts_server_owned_scheduler() -> None:
    source = DEV.read_text(encoding="utf-8")
    assert "antidengue_automation.scheduler_service" in source
    assert "PostgreSQL-backed AntiDengue scheduler" in source


def test_dev_restarts_owned_stack_before_port_validation() -> None:
    source = DEV.read_text(encoding="utf-8")
    call = source.index("\nstop_existing_platform_stack\n")
    api_check = source.index('port_in_use "$API_PORT" && fail')
    web_check = source.index('port_in_use "$WEB_PORT" && fail')
    assert call < api_check < web_check
    assert 'process_cwd" == "$ROOT"' in source
    assert 'process_cwd" == "$WEB_DIR"' in source
    assert 'process_args" == *"/astro dev"*' in source
    assert "exec env ASTRO_DEV_BACKGROUND=0 npm run dev" in source
