import fs from "node:fs";
import path from "node:path";
import WhatsAppWeb from "whatsapp-web.js";
const { Client, LocalAuth } = WhatsAppWeb;
import { connect, StringCodec } from "nats";
import pino from "pino";
import QRCode from "qrcode";
import { config } from "./lib/config.js";
import { HistoryStateStore } from "./lib/state-store.js";
import { PlatformClient } from "./lib/platform-client.js";
import { createBridgeService } from "./lib/bridge-service.js";
import { formatError } from "./lib/errors.js";
import { initializeClientWithRecovery } from "./lib/client-lifecycle.js";
import { ensureClientPage, isTransientPageError } from "./lib/page-session.js";
import { installConsoleNoiseFilter } from "./lib/console-noise-filter.js";

installConsoleNoiseFilter({ enabled: config.filterSessionNoise });
const log = pino({ level: config.logLevel });
const sc = StringCodec();
const runtime = {
  status: "starting",
  authenticated: false,
  error: null,
  webVersion: null,
  browserEndpoint: null,
  visibleProfile: null,
  page: null,
};

function acquireLock() {
  fs.mkdirSync(path.dirname(config.lockPath), { recursive: true });
  try {
    const existing = Number.parseInt(fs.readFileSync(config.lockPath, "utf8"), 10);
    if (existing && existing !== process.pid) {
      try {
        process.kill(existing, 0);
        throw new Error(`WhatsApp Web bridge is already running as PID ${existing}`);
      } catch (error) {
        if (error?.code !== "ESRCH") throw error;
      }
    }
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  fs.writeFileSync(config.lockPath, String(process.pid));
}

function releaseLock() {
  try {
    if (String(fs.readFileSync(config.lockPath, "utf8")).trim() === String(process.pid)) fs.unlinkSync(config.lockPath);
  } catch {}
}

function commonClientOptions() {
  return {
    authTimeoutMs: 120000,
    qrMaxRetries: 0,
    takeoverOnConflict: false,
    deviceName: "Deomee Automation",
    browserName: "Brave",
  };
}

function clientOptions() {
  const common = commonClientOptions();
  if (config.mode === "visible_profile") {
    return {
      ...common,
      puppeteer: {
        headless: config.headless,
        executablePath: config.browserExecutable,
        userDataDir: config.visibleUserDataDir,
        defaultViewport: null,
        args: [
          `--profile-directory=${config.visibleProfileDirectory}`,
          "--no-first-run",
          "--no-default-browser-check",
          "--disable-session-crashed-bubble",
          "--disable-vulkan",
          "--no-sandbox",
          "--disable-setuid-sandbox",
        ],
      },
    };
  }
  if (config.mode === "browser_url") {
    return {
      ...common,
      puppeteer: {
        browserURL: config.browserUrl,
        defaultViewport: null,
      },
    };
  }
  const puppeteer = {
    headless: config.headless,
    defaultViewport: null,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  };
  if (config.browserExecutable) puppeteer.executablePath = config.browserExecutable;
  return {
    ...common,
    authStrategy: new LocalAuth({ clientId: config.clientId, dataPath: config.authPath }),
    puppeteer,
  };
}

async function verifyRuntimePrerequisites() {
  if (config.mode === "browser_url") {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 5000);
    timer.unref?.();
    try {
      const response = await fetch(`${config.browserUrl.replace(/\/$/, "")}/json/version`, {
        signal: controller.signal,
      });
      if (!response.ok) throw new Error(`HTTP ${response.status} ${response.statusText}`);
      const payload = await response.json();
      runtime.browserEndpoint = {
        browser: payload.Browser || null,
        protocolVersion: payload["Protocol-Version"] || null,
        webSocketDebuggerUrl: payload.webSocketDebuggerUrl || null,
      };
      if (!runtime.browserEndpoint.webSocketDebuggerUrl) {
        throw new Error("The debugging endpoint did not return webSocketDebuggerUrl");
      }
      return;
    } catch (error) {
      throw new Error(
        `Cannot attach to Brave at ${config.browserUrl}. browser_url is retained only for diagnostics and is not the normal history mode. `
        + formatError(error, "browser_debug_endpoint"),
      );
    } finally {
      clearTimeout(timer);
    }
  }

  if (config.mode !== "visible_profile") return;
  if (!config.browserExecutable || !fs.existsSync(config.browserExecutable)) {
    throw new Error(`Brave/Chromium executable was not found: ${config.browserExecutable || "not configured"}`);
  }
  const profilePath = path.join(config.visibleUserDataDir, config.visibleProfileDirectory);
  if (!fs.existsSync(profilePath)) {
    throw new Error(
      `Visible-profile snapshot is missing: ${profilePath}. Close normal Brave and run `
      + `whatsapp-web-history-bridge/scripts/launch-brave-debug.sh once, then restart dev.sh.`,
    );
  }
  const singletonFiles = ["SingletonCookie", "SingletonLock", "SingletonSocket"]
    .map((name) => path.join(config.visibleUserDataDir, name))
    .filter((candidate) => fs.existsSync(candidate));
  if (singletonFiles.length) {
    throw new Error(
      `The managed visible-profile snapshot appears to be in use (${singletonFiles.join(", ")}). `
      + `Close the snapshot Brave window or stop the previous bridge before restarting dev.sh.`,
    );
  }
  runtime.visibleProfile = {
    userDataDir: config.visibleUserDataDir,
    profileDirectory: config.visibleProfileDirectory,
    metadataPath: config.visibleProfileMetadataPath,
  };
}

acquireLock();
const store = new HistoryStateStore(config.statePath);
const platform = new PlatformClient(config);
const nc = await connect({ servers: config.natsUrl, maxReconnectAttempts: -1 });
let client = null;
let service = null;
const retiringClients = new WeakSet();

function attachClientEvents(target) {
  target.on("qr", async (qr) => {
    if (retiringClients.has(target)) return;
    runtime.status = "qr";
    runtime.authenticated = false;
    runtime.error = null;
    fs.mkdirSync(path.dirname(config.qrPath), { recursive: true });
    await QRCode.toFile(config.qrPath, qr, { width: 420, margin: 2 });
    const terminalQr = await QRCode.toString(qr, { type: "terminal", small: true });
    process.stdout.write(`\n${terminalQr}\n`);
    log.warn({ qrPath: config.qrPath }, "WhatsApp Web bridge needs pairing; scan the terminal QR or saved PNG");
  });
  target.on("authenticated", () => {
    if (retiringClients.has(target)) return;
    runtime.status = "authenticated";
    runtime.authenticated = true;
    runtime.error = null;
    log.info("WhatsApp Web bridge authenticated");
  });
  target.on("ready", async () => {
    if (retiringClients.has(target)) return;
    try {
      const page = await ensureClientPage(target, {
        timeoutMs: config.pageRecoveryTimeoutMs,
        requireInjected: true,
      });
      if (retiringClients.has(target) || target !== client) return;
      runtime.page = page?.state || null;
      runtime.status = "ready";
      runtime.authenticated = true;
      runtime.error = null;
      runtime.webVersion = await target.getWWebVersion().catch(() => page?.state?.webVersion || null);
      try { fs.unlinkSync(config.qrPath); } catch {}
      log.info({
        workerId: config.workerId,
        subject: config.subject(),
        mode: config.mode,
        webVersion: runtime.webVersion,
        page: runtime.page,
        visibleProfile: runtime.visibleProfile,
      }, "WhatsApp Web history bridge ready");
    } catch (error) {
      if (retiringClients.has(target) || target !== client) return;
      runtime.status = "failed";
      runtime.error = "Managed WhatsApp Web became unavailable while validating its page. Restart the managed bridge.";
      log.error({ error: formatError(error, "ready_page_validation"), stack: error?.stack || null }, "WhatsApp Web page did not stabilize after ready");
    }
  });
  target.on("auth_failure", (message) => {
    if (retiringClients.has(target)) return;
    runtime.status = "auth_failure";
    runtime.authenticated = false;
    runtime.error = "WhatsApp Web authentication failed. Refresh the managed browser snapshot and pair it again.";
    log.error({ error: String(message || "Authentication failed") }, "WhatsApp Web bridge authentication failed");
  });
  target.on("disconnected", (reason) => {
    if (retiringClients.has(target)) return;
    runtime.status = "disconnected";
    runtime.authenticated = false;
    runtime.error = "Managed WhatsApp Web disconnected. Restart the managed bridge.";
    log.warn({ reason: String(reason || "Disconnected") }, "WhatsApp Web bridge disconnected");
  });
}

function createManagedClient() {
  const target = new Client(clientOptions());
  attachClientEvents(target);
  client = target;
  service = createBridgeService({ config, client: target, store, platform, log, sc, runtime });
  return target;
}

async function disposeManagedClient(target) {
  retiringClients.add(target);
  runtime.page = null;
  try {
    if (config.mode === "browser_url") {
      await target.pupPage?.close().catch(() => {});
      target.pupBrowser?.disconnect?.();
    } else {
      await target.destroy();
    }
  } catch {
    await target.pupBrowser?.close?.().catch(() => {});
  }
}

const subscription = nc.subscribe(config.subject(), {
  callback: (_error, message) => {
    if (service) return void service.respond(message);
    message.respond(sc.encode(JSON.stringify({
      accepted: true,
      provider: "wwebjs",
      protocolVersion: config.protocolVersion,
      workerId: config.workerId,
      status: runtime.status,
      ready: false,
      historyReady: false,
      message: "Managed WhatsApp Web is starting.",
    })));
  },
});

async function shutdown(signal) {
  log.info({ signal }, "Stopping WhatsApp Web history bridge");
  subscription.unsubscribe();
  await nc.drain().catch(() => {});
  try {
    if (config.mode === "browser_url") {
      await client?.pupPage?.close().catch(() => {});
      client?.pupBrowser?.disconnect?.();
    } else {
      if (client) await disposeManagedClient(client);
    }
  } catch {}
  releaseLock();
  process.exit(0);
}
process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("exit", releaseLock);

log.info({
  workerId: config.workerId,
  subject: config.subject(),
  mode: config.mode,
  wwebjs: "1.34.7",
  protocolVersion: config.protocolVersion,
}, "Starting WhatsApp Web history bridge");
verifyRuntimePrerequisites()
  .then(() => initializeClientWithRecovery({
    attempts: config.initializationAttempts,
    retryDelayMs: config.initializationRetryDelayMs,
    isTransient: isTransientPageError,
    createClient: () => createManagedClient(),
    disposeClient: (target) => disposeManagedClient(target),
    onAttempt: ({ attempt }) => {
      runtime.status = attempt === 1 ? "initializing" : "recovering";
      runtime.authenticated = false;
      runtime.error = attempt === 1 ? null : "Managed WhatsApp Web is recovering its browser page. Retry shortly.";
    },
    onRetry: ({ attempt, total, error }) => {
      log.warn({
        attempt,
        total,
        error: formatError(error, "bridge_initialize"),
      }, "Transient WhatsApp Web page failure; recreating the managed browser client");
    },
  }))
  .catch((error) => {
    runtime.status = "failed";
    runtime.authenticated = false;
    runtime.error = isTransientPageError(error)
      ? `Managed WhatsApp Web could not stabilize after ${config.initializationAttempts} attempts. Restart the managed bridge.`
      : "Managed WhatsApp Web could not start. Check the bridge server logs and managed browser snapshot.";
    log.error({ error: formatError(error, "bridge_initialize"), stack: error?.stack || null }, "WhatsApp Web bridge initialization failed");
  });
