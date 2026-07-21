from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKER = ROOT.parent / "whatsappbot" / "worker.js"
DEV_SCRIPT = ROOT / "scripts/dev.sh"
HISTORY_MODULE = ROOT.parent / "whatsappbot" / "lib/inbound-history.js"


def test_worker_exposes_bounded_history_protocol_v2() -> None:
    worker = WORKER.read_text(encoding="utf-8")
    history = HISTORY_MODULE.read_text(encoding="utf-8")
    assert "protocolVersion: 2" in worker
    assert "...inboundHistorySocketOptions(config)" in worker
    assert "syncFullHistory: Boolean(config.inboundSyncFullHistory)" in history
    assert "shouldSyncHistoryMessage" in history


def test_dev_stack_restarts_workers_older_than_history_protocol_v2() -> None:
    source = DEV_SCRIPT.read_text(encoding="utf-8")
    assert 'int(history.get("protocolVersion") or 0) < 2' in source
    assert 'history.get("syncFullHistory") is not True' in source
    assert "without bounded history protocol v2" in source


def test_dev_stack_starts_whatsapp_web_history_bridge() -> None:
    source = DEV_SCRIPT.read_text(encoding="utf-8")
    assert "whatsapp-web-history-bridge" in source
    assert "whatsapp_web_bridge_responder_ready" in source
    assert 'int(payload.get("protocolVersion") or 0) < 4' in source
    assert "Starting whatsapp-web.js history bridge protocol v4" in source


def test_dev_stack_fails_early_for_unwritable_frontend_caches() -> None:
    source = DEV_SCRIPT.read_text(encoding="utf-8")
    assert "assert_frontend_generated_paths_writable" in source
    assert 'find "$path" ! -writable' in source
    assert "sudo chown -R" in source
