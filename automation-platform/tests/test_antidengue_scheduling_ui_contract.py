from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "apps/web/src/pages/index.astro"
SCHEDULES = ROOT / "apps/web/src/pages/antidengue/schedules.astro"
HISTORY = ROOT / "apps/web/src/pages/antidengue/run-history.astro"
ROUTING = ROOT / "apps/web/src/pages/antidengue/routing.astro"
MESSAGES = ROOT / "apps/web/src/pages/antidengue/messages.astro"
PLANS = ROOT / "apps/web/src/pages/antidengue/dispatch-plans.astro"
LAYOUT = ROOT / "apps/web/src/layouts/AntiDengueLayout.astro"
DEV = ROOT / "scripts/dev.sh"
ACTIVITY_RULES = ROOT / "apps/web/src/pages/antidengue/activity-rules.astro"
TIMING_RULES = ROOT / "apps/web/src/pages/antidengue/simple-activity-rules.astro"
WEB_ACTIONS = ROOT / "apps/web/src/actions/index.ts"
API = ROOT / "packages/antidengue_automation/antidengue_automation/api.py"
SCHEDULING = ROOT / "packages/antidengue_automation/antidengue_automation/scheduling.py"


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


def test_dashboard_uses_transactional_routing_profile_modal() -> None:
    source = DASHBOARD.read_text(encoding="utf-8")
    assert 'id="profile-dialog"' in source
    assert 'id="profile-selector-button"' in source
    assert 'id="apply-profiles"' in source
    assert 'id="profile-selector"' not in source
    assert "const draftProfileIds = new Set<string>();" in source
    assert "selectedProfileIds.clear(); draftProfileIds.forEach" in source
    assert "Select visible" in source
    assert "Generate reports → resolve selected routes" in source


def test_schedule_and_run_history_navigation_is_present() -> None:
    layout = LAYOUT.read_text(encoding="utf-8")
    assert '"routing", "Routing", "/antidengue/routing"' in layout
    assert '"schedules", "Schedules", "/antidengue/schedules"' in layout
    assert '"dispatch-plans", "Runs & Plans", "/antidengue/dispatch-plans"' in layout
    schedules = SCHEDULES.read_text(encoding="utf-8")
    assert "Prepare for review" in schedules
    assert "Send only when clean" in schedules
    assert "Run preview now" in schedules
    assert "Select all visible" in schedules
    assert "scheduler.scheduler_enabled" in schedules
    assert "Advanced execution settings" in schedules
    assert 'id="schedule-time-chips"' in schedules
    assert 'id="schedule-review"' in schedules
    assert 'toggle.textContent = item.enabled ? "Pause" : "Enable"' in schedules
    history = HISTORY.read_text(encoding="utf-8")
    assert "Persistent combined activity" in history


def test_antidengue_owns_routing_and_contextual_plan_actions() -> None:
    routing = ROUTING.read_text(encoding="utf-8")
    assert "New routing profile" in routing
    assert "/api/v1/whatsapp/dispatch-profiles" in routing
    assert "View recipients" in routing
    assert "Advanced recipient editor" in routing
    assert "Clone to hotspot" in routing
    assert "Clone to timing" in routing
    assert 'id="clone-dialog"' in routing
    assert "/api/v1/antidengue/routing-profiles/" in routing
    assert 'aria-label="Report type"' in routing
    assert 'data-category="dormant"' in routing
    assert 'data-category="hotspot"' in routing
    assert 'data-category="simple"' in routing
    assert 'data-category="digest"' in routing
    assert "consolidated_action_digest" in routing
    assert 'aria-label="Administrative level"' in routing
    assert 'data-level="wing"' in routing
    assert 'data-level="tehsil"' in routing
    assert 'data-level="markaz"' in routing
    assert "function profileLevel(profile: any)" in routing
    assert "function workspaceProfiles()" in routing
    assert "function reportsForWorkspace()" in routing
    assert "Message: ${template.name}" in routing

    plans = PLANS.read_text(encoding="utf-8")
    assert "Runs and their next action" in plans
    assert "Nothing to send" in plans
    assert "Fix blockers" in plans
    assert "Ready to review" in plans
    assert "item.planned_deliveries > 0" in plans
    assert "Send when allowed" not in plans


def test_rule_pages_do_not_own_whatsapp_routing() -> None:
    activity = ACTIVITY_RULES.read_text(encoding="utf-8")
    timing = TIMING_RULES.read_text(encoding="utf-8")
    actions = WEB_ACTIONS.read_text(encoding="utf-8")

    assert "Shared WhatsApp routing" not in activity
    assert "Reuse dormant-report audiences" not in activity
    assert 'href="/antidengue/routing?view=hotspot"' in activity
    assert "Simple Activity routing" not in timing
    assert "Reuse authorized dormant-report audiences" not in timing
    assert 'href="/antidengue/routing?view=simple"' in timing
    assert "configureHotspotRoutes" not in actions
    assert "configureSimpleActivityRoutes" not in actions


def test_messages_are_partitioned_by_report_and_administrative_level() -> None:
    messages = MESSAGES.read_text(encoding="utf-8")
    assert 'aria-label="Message report type"' in messages
    assert 'data-category="dormant"' in messages
    assert 'data-category="hotspot"' in messages
    assert 'data-category="simple"' in messages
    assert 'data-category="digest"' in messages
    assert "consolidated_action_digest" in messages
    assert 'aria-label="Message administrative level"' in messages
    assert 'data-level="wing"' in messages
    assert 'data-level="tehsil"' in messages
    assert 'data-level="markaz"' in messages
    assert "function reportCategory(profile: any)" in messages
    assert "function profileLevel(profile: any)" in messages
    assert "function templatesAssignedTo(assignedProfiles: any[])" in messages
    assert "const workspaceProfiles = categoryProfiles.filter" in messages
    assert 'id="acknowledgement-workspace"' in messages
    assert 'hidden={!showAcknowledgements}' in messages


def test_execution_uses_exact_selected_profiles_without_hidden_expansion() -> None:
    api = API.read_text(encoding="utf-8")
    scheduling = SCHEDULING.read_text(encoding="utf-8")
    for source in (api, scheduling):
        assert "expand_hotspot_profile_ids" not in source
        assert "expand_simple_activity_profile_ids" not in source
        assert "validate_dispatch_profiles(session, profile_ids)" in source


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
