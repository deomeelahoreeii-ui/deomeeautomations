from pathlib import Path


PAGE = (
    Path(__file__).resolve().parents[1]
    / "apps/web/src/pages/whatsapp/inbound-files.astro"
)


def test_history_polling_is_single_route_bounded_and_timeout_driven() -> None:
    source = PAGE.read_text(encoding="utf-8")
    assert "const HISTORY_POLL_INTERVAL_MS = 5000;" in source
    assert "const HISTORY_POLL_MAX_MS = 190000;" in source
    assert "window.setTimeout(() => void pollHistoryRequest(), delayMs)" in source
    assert "setInterval(() => void pollHistoryRequest" not in source

    poll_body = source.split("async function pollHistoryRequest()", 1)[1].split(
        "function startHistoryRequestPolling", 1
    )[0]
    assert poll_body.count("actions.whatsapp.inboundHistoryRequest(") == 1
    assert "actions.whatsapp.inboundHistoryRequests(" not in poll_body
    assert "document.hidden" in poll_body
    assert "HISTORY_POLL_MAX_MS" in poll_body


def test_history_ui_explains_terminal_no_result_state() -> None:
    source = PAGE.read_text(encoding="utf-8")
    assert 'latest.status === "no_results"' in source
    assert "returned no older messages to this linked worker session" in source
    assert 'if (["succeeded", "no_results"].includes(item.status)) await scanFiles();' in source
