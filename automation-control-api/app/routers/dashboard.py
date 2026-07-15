from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse


router = APIRouter(tags=["dashboard"])


@router.get("/", include_in_schema=False)
def root() -> HTMLResponse:
    return dashboard_response()


@router.get("/dashboard", include_in_schema=False)
def dashboard() -> HTMLResponse:
    return dashboard_response()


def dashboard_response() -> HTMLResponse:
    return HTMLResponse(
        DASHBOARD_HTML,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>Deomee Control</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #101214;
      --panel: #181b1f;
      --panel-2: #20242a;
      --line: #343a42;
      --text: #f2f4f7;
      --muted: #a6adb8;
      --ok: #30d158;
      --warn: #ffb020;
      --bad: #ff453a;
      --blue: #4da3ff;
      --radius: 8px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font: 15px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    button, input, select {
      font: inherit;
    }

    button {
      min-height: 40px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--panel-2);
      color: var(--text);
      padding: 8px 12px;
    }

    button:active {
      transform: translateY(1px);
    }

    button.primary {
      border-color: #2b75c9;
      background: #155a9f;
    }

    button.danger {
      border-color: #96312b;
      background: #5c1e1b;
    }

    button.ghost {
      background: transparent;
    }

    input, select {
      width: 100%;
      min-height: 40px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #12151a;
      color: var(--text);
      padding: 8px 10px;
    }

    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
    }

    label.checkbox {
      grid-template-columns: 22px 1fr;
      align-items: center;
      color: var(--text);
      font-size: 15px;
    }

    input[type="checkbox"] {
      width: 20px;
      min-height: 20px;
      accent-color: var(--blue);
    }

    .shell {
      max-width: 860px;
      margin: 0 auto;
      padding: max(14px, env(safe-area-inset-top)) 14px max(18px, env(safe-area-inset-bottom));
    }

    .topbar {
      position: sticky;
      top: 0;
      z-index: 10;
      margin: -14px -14px 14px;
      padding: max(12px, env(safe-area-inset-top)) 14px 10px;
      background: rgba(16, 18, 20, 0.94);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(12px);
    }

    .title-row {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }

    h1, h2 {
      margin: 0;
      line-height: 1.15;
    }

    h1 {
      font-size: 20px;
      font-weight: 700;
    }

    h2 {
      font-size: 16px;
      font-weight: 650;
    }

    .muted { color: var(--muted); }
    .ok { color: var(--ok); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }

    .status-line {
      display: flex;
      flex-wrap: wrap;
      gap: 8px 12px;
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }

    .tabs {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 6px;
      margin-top: 12px;
    }

    .tabs button {
      min-height: 36px;
      padding: 6px 8px;
    }

    .tabs button.active {
      border-color: #64748b;
      background: #303741;
    }

    .grid {
      display: grid;
      gap: 12px;
    }

    .card {
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: var(--panel);
      padding: 12px;
    }

    .card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 10px;
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }

    .metric {
      min-height: 70px;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #13161a;
      padding: 10px;
    }

    .metric span {
      display: block;
      color: var(--muted);
      font-size: 12px;
    }

    .metric strong {
      display: block;
      margin-top: 5px;
      font-size: 24px;
      line-height: 1.1;
    }

    .metric small {
      display: block;
      margin-top: 6px;
      color: var(--muted);
      overflow-wrap: anywhere;
    }

    .button-row {
      display: flex;
      flex-wrap: wrap;
      justify-content: flex-end;
      gap: 8px;
    }

    .actions {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
    }

    .form-grid {
      display: grid;
      gap: 10px;
    }

    .row {
      display: grid;
      gap: 8px;
    }

    .list {
      display: grid;
      gap: 8px;
    }

    .list-item {
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #13161a;
      padding: 10px;
    }

    pre {
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      min-height: 200px;
      max-height: 55vh;
      margin: 0;
      border: 1px solid var(--line);
      border-radius: var(--radius);
      background: #0b0d10;
      color: #d7dde6;
      padding: 10px;
      font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }

    .hidden {
      display: none !important;
    }

    .notice {
      border: 1px solid var(--line);
      border-left: 4px solid var(--warn);
      border-radius: var(--radius);
      background: #191713;
      color: var(--text);
      padding: 10px;
    }

    @media (min-width: 680px) {
      .metrics {
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }

      .actions {
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }

      .row.two {
        grid-template-columns: 1fr 1fr;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div class="title-row">
        <div>
          <h1>Deomee Control</h1>
          <div class="status-line">
            <span>API <strong id="apiStatus" class="warn">checking</strong></span>
            <span>Auth <strong id="authStatus" class="warn">not paired</strong></span>
          </div>
        </div>
        <button id="refreshBtn" class="ghost">Refresh</button>
      </div>
      <nav class="tabs">
        <button data-tab="status" class="active">Status</button>
        <button data-tab="run">Run</button>
        <button data-tab="login">Login</button>
        <button data-tab="logs">Logs</button>
      </nav>
    </header>

    <section id="message" class="notice hidden"></section>

    <section id="tab-status" class="grid">
      <div class="card">
        <div class="card-head">
          <h2>Latest Run</h2>
          <span id="latestTime" class="muted">-</span>
        </div>
        <div class="metrics">
          <div class="metric"><span>Dormant</span><strong id="dormantRows">-</strong></div>
          <div class="metric"><span>Sent</span><strong id="delivered">-</strong></div>
          <div class="metric"><span>Needs Check</span><strong id="failed">-</strong></div>
          <div class="metric"><span>Quality</span><strong id="quality">-</strong></div>
        </div>
        <div id="latestNote" class="muted" style="margin-top: 10px;"></div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2>Services</h2>
          <span id="servicesSummary" class="muted">checking</span>
        </div>
        <div id="serviceGrid" class="metrics"></div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2>Current Job</h2>
          <span id="jobState" class="muted">idle</span>
        </div>
        <div id="jobDetails" class="list"></div>
      </div>
    </section>

    <section id="tab-run" class="grid hidden">
      <div class="card">
        <div class="card-head">
          <h2>AntiDengue Run</h2>
          <button id="stopBtn" class="danger">Stop</button>
        </div>
        <div class="form-grid">
          <div class="row two">
            <label>Login mode
              <select id="loginMode">
                <option value="auto">Auto sign in</option>
                <option value="remote_approve">Remote approve</option>
                <option value="manual">Manual on laptop</option>
              </select>
            </label>
            <label>Command
              <select id="commandMode">
                <option value="portal">Download from portal</option>
                <option value="manual-unfiltered">Use latest raw file</option>
                <option value="manual-file">Use selected file path</option>
              </select>
            </label>
          </div>
          <label id="filePathWrap" class="hidden">Manual file path
            <input id="filePath" placeholder="/home/ahmad/.../report.xls">
          </label>
          <label class="checkbox"><input id="dryRun" type="checkbox"> Dry run</label>
          <div class="actions">
            <button id="startBtn" class="primary">Start</button>
            <button id="startDryBtn">Start Dry Run</button>
            <button id="copyStatusBtn">Copy Status</button>
            <button id="openDocsBtn">API Docs</button>
          </div>
        </div>
      </div>
    </section>

    <section id="tab-login" class="grid hidden">
      <div class="card">
        <div class="card-head">
          <h2>Portal Login</h2>
          <span id="loginRequestState" class="muted">-</span>
        </div>
        <div id="loginRequest" class="list"></div>
        <div class="actions" style="margin-top: 10px;">
          <button id="approveBtn" class="primary">Approve</button>
          <button id="denyBtn" class="danger">Deny</button>
          <button id="refreshLoginBtn">Refresh</button>
          <button id="clearTokenBtn">Clear Token</button>
        </div>
      </div>

      <div class="card">
        <div class="card-head">
          <h2>Pairing</h2>
          <span id="pairState" class="muted">local</span>
        </div>
        <div class="form-grid">
          <label>API token
            <input id="apiToken" type="password" autocomplete="off" placeholder="CONTROL_API_TOKEN">
          </label>
          <button id="saveTokenBtn" class="primary">Save Token</button>
        </div>
      </div>
    </section>

    <section id="tab-logs" class="grid hidden">
      <div class="card">
        <div class="card-head">
          <h2>Logs</h2>
          <div class="button-row">
            <button id="copyLogsBtn">Copy Logs</button>
            <button id="refreshLogsBtn">Refresh Logs</button>
          </div>
        </div>
        <pre id="logs">No logs loaded.</pre>
      </div>
    </section>
  </main>

  <script>
    const state = {
      status: null,
      token: localStorage.getItem("deomee_control_token") || "",
      tab: "status",
    };

    const $ = (id) => document.getElementById(id);
    const prefix = window.location.pathname.startsWith("/control-api") ? "/control-api" : "";
    const api = (path) => `${prefix}${path}`;

    function setText(id, value, className) {
      const el = $(id);
      el.textContent = value ?? "-";
      if (className !== undefined) el.className = className;
    }

    function showMessage(text, kind = "warn") {
      const el = $("message");
      if (!text) {
        el.classList.add("hidden");
        return;
      }
      el.textContent = text;
      el.style.borderLeftColor = kind === "bad" ? "var(--bad)" : kind === "ok" ? "var(--ok)" : "var(--warn)";
      el.classList.remove("hidden");
    }

    function authHeaders() {
      return state.token ? { "Authorization": `Bearer ${state.token}` } : {};
    }

    async function getJson(path) {
      const res = await fetch(api(path), { cache: "no-store" });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.json();
    }

    async function postJson(path, payload = {}) {
      const res = await fetch(api(path), {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify(payload),
      });
      const text = await res.text();
      let data = {};
      try { data = text ? JSON.parse(text) : {}; } catch (_) { data = { detail: text }; }
      if (!res.ok) throw new Error(data.detail || `${res.status} ${res.statusText}`);
      return data;
    }

    function renderStatus(payload) {
      state.status = payload;
      const latest = payload.latest_run || {};
      const active = payload.active_job;
      const portal = payload.portal_login || {};

      setText("apiStatus", "online", "ok");
      setText("authStatus", state.token ? "paired" : "not paired", state.token ? "ok" : "warn");
      setText("latestTime", latest.started_at || latest.modified_at || "-");
      setText("dormantRows", latest.dormant_rows ?? "-");
      setText("delivered", latest.sent ?? latest.delivered ?? "-");
      setText("failed", latest.failed ?? "-");
      setText("quality", latest.quality_passed ? "passed" : "check", latest.quality_passed ? "ok" : "warn");
      const pendingConfirmation = Number(latest.pending_confirmation || 0);
      const rawDormant = Number(latest.raw_dormant_rows || 0);
      const processedDormant = Number(latest.processed_school_count || latest.dormant_rows || 0);
      const notes = [];
      if (pendingConfirmation > 0) {
        notes.push(`${pendingConfirmation} group message${pendingConfirmation === 1 ? "" : "s"} sent; final WhatsApp acknowledgement is pending.`);
      }
      if (rawDormant > processedDormant && processedDormant > 0) {
        notes.push(`${rawDormant} raw portal dormant row${rawDormant === 1 ? "" : "s"} filtered to ${processedDormant} mapped school${processedDormant === 1 ? "" : "s"}.`);
      }
      $("latestNote").textContent = notes.join(" ");
      renderServices(payload.services || {});

      setText("jobState", active ? active.status || "running" : "idle", active ? "warn" : "muted");
      $("jobDetails").innerHTML = active
        ? item("Command", (active.command || []).join(" ")) + item("Started", active.started_at || "-")
        : item("Status", "No active AntiDengue job.");

      setText("loginRequestState", portal.request_exists ? "waiting" : "none", portal.request_exists ? "warn" : "muted");
      $("loginRequest").innerHTML = portal.request_exists
        ? item("Request", JSON.stringify(portal.request || {}, null, 2))
        : item("Status", "No portal login approval request.");
    }

    function renderServices(services) {
      const list = [services.nats, services.whatsapp_primary].filter(Boolean);
      if (!list.length) {
        setText("servicesSummary", "unknown", "warn");
        $("serviceGrid").innerHTML = `<div class="metric"><span>Status</span><strong class="warn">unknown</strong><small>No service health returned.</small></div>`;
        return;
      }
      const ready = list.every((service) => Boolean(service.healthy));
      setText("servicesSummary", ready ? "ready" : "attention", ready ? "ok" : "bad");
      $("serviceGrid").innerHTML = list.map(serviceMetric).join("");
    }

    function serviceMetric(service) {
      const state = service.healthy ? "ready" : service.running ? "starting" : "down";
      const className = service.healthy ? "ok" : service.running ? "warn" : "bad";
      const pid = service.pid ? `pid ${service.pid}` : "no pid";
      const detail = [service.detail || "-", pid].join(" · ");
      return `<div class="metric"><span>${escapeHtml(service.title || service.id || "Service")}</span><strong class="${className}">${state}</strong><small>${escapeHtml(detail)}</small></div>`;
    }

    function item(label, value) {
      return `<div class="list-item"><div class="muted">${escapeHtml(label)}</div><div>${escapeHtml(String(value))}</div></div>`;
    }

    function escapeHtml(value) {
      return value.replace(/[&<>"']/g, (char) => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#039;",
      }[char]));
    }

    async function refreshAll() {
      try {
        const health = await getJson("/health");
        setText("apiStatus", health.status || "online", "ok");
        const payload = await getJson("/antidengue/status");
        renderStatus(payload);
        showMessage("");
      } catch (err) {
        setText("apiStatus", "offline", "bad");
        showMessage(`Could not refresh: ${err.message}`, "bad");
      }
    }

    async function refreshLogs() {
      try {
        const payload = await getJson("/antidengue/logs?lines=800");
        const lines = payload.lines || [];
        $("logs").textContent = lines.length ? lines.join("\n") : "No logs found.";
      } catch (err) {
        $("logs").textContent = `Could not load logs: ${err.message}`;
      }
    }

    async function startRun(forceDryRun = false) {
      const command = $("commandMode").value;
      const payload = {
        login_mode: $("loginMode").value,
        dry_run: forceDryRun || $("dryRun").checked,
        command,
        file_path: $("filePath").value.trim(),
      };
      try {
        await postJson("/antidengue/run", payload);
        showMessage("AntiDengue run started.", "ok");
        await refreshAll();
      } catch (err) {
        showMessage(`Could not start run: ${err.message}`, "bad");
      }
    }

    async function stopRun() {
      try {
        await postJson("/antidengue/stop", {});
        showMessage("Stop requested.", "ok");
        await refreshAll();
      } catch (err) {
        showMessage(`Could not stop run: ${err.message}`, "bad");
      }
    }

    async function loginAction(action) {
      try {
        await postJson(`/portal-login/${action}`, { note: `from mobile dashboard ${new Date().toISOString()}` });
        showMessage(`Portal login ${action} sent.`, "ok");
        await refreshAll();
      } catch (err) {
        showMessage(`Could not ${action}: ${err.message}`, "bad");
      }
    }

    function setTab(tab) {
      state.tab = tab;
      for (const button of document.querySelectorAll(".tabs button")) {
        button.classList.toggle("active", button.dataset.tab === tab);
      }
      for (const section of document.querySelectorAll("[id^='tab-']")) {
        section.classList.toggle("hidden", section.id !== `tab-${tab}`);
      }
      if (tab === "logs") refreshLogs();
    }

    function saveToken() {
      state.token = $("apiToken").value.trim();
      if (state.token) {
        localStorage.setItem("deomee_control_token", state.token);
        showMessage("Token saved on this device.", "ok");
      } else {
        localStorage.removeItem("deomee_control_token");
        showMessage("Token cleared.", "ok");
      }
      setText("authStatus", state.token ? "paired" : "not paired", state.token ? "ok" : "warn");
    }

    function clearToken() {
      state.token = "";
      $("apiToken").value = "";
      localStorage.removeItem("deomee_control_token");
      setText("authStatus", "not paired", "warn");
      showMessage("Token cleared.", "ok");
    }

    async function copyStatus() {
      const latest = (state.status && state.status.latest_run) || {};
      const text = [
        "AntiDengue status",
        `Run: ${latest.started_at || "-"}`,
        `Dormant: ${latest.dormant_rows ?? "-"}`,
        `Sent: ${latest.sent ?? latest.delivered ?? "-"}`,
        `Needs check: ${latest.failed ?? "-"}`,
        `Pending confirmation: ${latest.pending_confirmation ?? 0}`,
        `Quality: ${latest.quality_passed ? "passed" : "check"}`,
      ].join("\n");
      try {
        await copyText(text);
        showMessage("Status copied.", "ok");
      } catch (err) {
        showMessage(`Could not copy status: ${err.message}`, "bad");
      }
    }

    async function copyLogs() {
      const text = $("logs").textContent.trim();
      if (!text || text === "No logs loaded.") {
        showMessage("Refresh logs first, then tap Copy Logs.", "warn");
        return;
      }
      try {
        await copyText(text);
        showMessage("Logs copied.", "ok");
      } catch (err) {
        showMessage(`Could not copy logs: ${err.message}`, "bad");
      }
    }

    async function copyText(text) {
      if (navigator.clipboard && window.isSecureContext) {
        try {
          await navigator.clipboard.writeText(text);
          return;
        } catch (_) {
          // Fall back below; some mobile in-app browsers expose but block Clipboard API.
        }
      }

      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "");
      textarea.style.position = "fixed";
      textarea.style.top = "0";
      textarea.style.left = "-9999px";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      textarea.setSelectionRange(0, textarea.value.length);
      const copied = document.execCommand("copy");
      textarea.remove();
      if (!copied) throw new Error("Copy was blocked by this browser.");
    }

    function readTokenFromHash() {
      const hash = new URLSearchParams(window.location.hash.replace(/^#/, ""));
      const token = hash.get("token");
      if (!token) return;
      state.token = token.trim();
      $("apiToken").value = state.token;
      localStorage.setItem("deomee_control_token", state.token);
      history.replaceState(null, "", window.location.pathname + window.location.search);
      showMessage("Token paired on this device.", "ok");
    }

    readTokenFromHash();
    $("apiToken").value = state.token;
    setText("authStatus", state.token ? "paired" : "not paired", state.token ? "ok" : "warn");

    for (const button of document.querySelectorAll(".tabs button")) {
      button.addEventListener("click", () => setTab(button.dataset.tab));
    }
    $("refreshBtn").addEventListener("click", refreshAll);
    $("refreshLoginBtn").addEventListener("click", refreshAll);
    $("refreshLogsBtn").addEventListener("click", refreshLogs);
    $("copyLogsBtn").addEventListener("click", copyLogs);
    $("startBtn").addEventListener("click", () => startRun(false));
    $("startDryBtn").addEventListener("click", () => startRun(true));
    $("stopBtn").addEventListener("click", stopRun);
    $("approveBtn").addEventListener("click", () => loginAction("approve"));
    $("denyBtn").addEventListener("click", () => loginAction("deny"));
    $("saveTokenBtn").addEventListener("click", saveToken);
    $("clearTokenBtn").addEventListener("click", clearToken);
    $("copyStatusBtn").addEventListener("click", copyStatus);
    $("openDocsBtn").addEventListener("click", () => window.location.href = api("/docs"));
    $("commandMode").addEventListener("change", () => {
      $("filePathWrap").classList.toggle("hidden", $("commandMode").value !== "manual-file");
    });

    refreshAll();
    setInterval(refreshAll, 15000);
  </script>
</body>
</html>
"""
