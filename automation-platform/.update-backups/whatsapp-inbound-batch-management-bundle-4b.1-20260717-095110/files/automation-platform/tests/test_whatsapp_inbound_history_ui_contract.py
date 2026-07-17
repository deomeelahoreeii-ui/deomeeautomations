from pathlib import Path


PAGE = (
    Path(__file__).resolve().parents[1]
    / "apps/web/src/pages/whatsapp/inbound-files.astro"
)


def test_history_polling_is_single_route_bounded_and_timeout_driven() -> None:
    source = PAGE.read_text(encoding="utf-8")
    assert "const HISTORY_POLL_INTERVAL_MS = 5000;" in source
    # The WhatsApp Web bridge can legitimately spend up to 600 seconds loading
    # a long visible-profile chat. The UI observes it for that window plus a
    # small grace period, then stops polling rather than looping forever.
    assert "const HISTORY_POLL_MAX_MS = 610000;" in source
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
    assert (
        "WhatsApp Web reached the available boundary without finding messages "
        "older than the current capture anchor."
    ) in source
    assert "returned no older messages to this linked worker session" not in source
    assert 'if (["succeeded", "no_results"].includes(item.status)) await scanFiles();' in source


def test_history_ui_requires_the_managed_visible_profile_bridge() -> None:
    source = PAGE.read_text(encoding="utf-8")
    assert 'state.textContent = bridgeReady ? "Managed browser ready"' in source
    assert 'if (!bridgeReady) throw new Error("The WhatsApp Web history bridge is not ready.")' in source
    assert "whatsapp-web.js owns one Brave page" in source
    assert "Wrong browser mode" in source
