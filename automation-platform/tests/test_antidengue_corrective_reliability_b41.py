from pathlib import Path


PLATFORM = Path(__file__).resolve().parents[1]
WORKSPACE = PLATFORM.parent


def test_runtime_snapshot_distinguishes_preview_from_delivered_execution() -> None:
    source = (
        PLATFORM
        / "packages"
        / "antidengue_automation"
        / "antidengue_automation"
        / "runtime_snapshot.py"
    ).read_text(encoding="utf-8")
    assert "AntiDengueScheduleExecution" in source
    assert "execution_by_source_job = {" in source
    assert '"status": execution.status if execution is not None else job.status' in source
    assert '"portal_acquisition": dict(summary.get("portal_acquisition") or {})' in source
    assert "(preview_ready) from a real completed Send Now/schedule run" in source


def test_scheduler_completes_no_change_without_creating_an_unknown_db_status() -> None:
    source = (
        PLATFORM
        / "packages"
        / "antidengue_automation"
        / "antidengue_automation"
        / "scheduling.py"
    ).read_text(encoding="utf-8")
    assert 'source_run_summary = execution.source_summary.get("summary") or {}' in source
    assert 'or str(whatsapp_summary.get("outcome") or "").strip().lower() == "no_change"' in source
    assert '            "completed",' in source
    assert "previous successfully delivered report bundle" in source
    assert "Duplicate preview compilation and WhatsApp delivery were safely skipped." in source


def test_schedule_run_now_shows_progress_and_opens_exact_execution() -> None:
    schedules = (PLATFORM / "apps/web/src/pages/antidengue/schedules.astro").read_text(encoding="utf-8")
    assert 'run.textContent = "Queueing…"' in schedules
    assert "run.disabled = true" in schedules
    assert 'location.href = `/antidengue/run-history?execution_id=${execution.id}`;' in schedules
    assert "execution.reused_active_execution" in schedules


def test_legacy_reconciliation_and_no_change_contracts() -> None:
    source = (WORKSPACE / "antidengue/compliance_reconciliation.py").read_text(encoding="utf-8")
    hotspot = (WORKSPACE / "antidengue/hotspot_analysis.py").read_text(encoding="utf-8")
    timing = (WORKSPACE / "antidengue/simple_activity_analysis.py").read_text(encoding="utf-8")
    main = (WORKSPACE / "antidengue/main.py").read_text(encoding="utf-8")
    assert 'POLICY_VERSION = "one_for_one_fifo.v1"' in source
    assert 'yield "Corrected Issues"' in source
    assert 'yield "Valid Unused Activities"' in source
    assert 'yield "All Activity Audit"' in source
    assert "reconcile_activities(" in hotspot
    assert "reconcile_activities(" in timing
    assert 'return "completed_no_change"' in main
    assert '"duplicate_scope": "complete_report_bundle"' in main
    assert 'result["duplicate_scope"] = "incomplete_report_bundle"' in main
    assert 'A preview must never consume the future' in main
    assert '"completed_with_delivery_errors",' in main
    assert 'portal_acquisition=portal_acquisition' in main


def test_schedule_repair_is_dry_run_and_never_enables_implicitly() -> None:
    source = (PLATFORM / "scripts/repair_antidengue_consolidated_schedules.py").read_text(encoding="utf-8")
    assert 'parser.add_argument("--apply"' in source
    assert 'parser.add_argument("--enable"' in source
    assert 'if args.enable and not args.apply:' in source
    assert 'if args.enable:' in source
    assert 'schedule.enabled = True' in source
    assert 'if args.apply:' in source
    assert 'if profile.key not in PRIORITY:' in source
