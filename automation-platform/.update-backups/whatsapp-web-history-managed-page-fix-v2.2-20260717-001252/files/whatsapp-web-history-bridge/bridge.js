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

const log = pino({ level: config.logLevel });
const sc = StringCodec();
const runtime = {
  status: "starting",
  authenticated: false,
  error: null,
  webVersion: null,
  browserEndpoint: null,
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

function clientOptions() {
  const common = {
    authTimeoutMs: 120000,
    qrMaxRetries: 0,
    takeoverOnConflict: false,
    deviceName: "Deomee Automation",
    browserName: "Brave",
  };
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

acquireLock();
const store = new HistoryStateStore(config.statePath);
const platform = new PlatformClient(config);
const client = new Client(clientOptions());
const nc = await connect({ servers: config.natsUrl, maxReconnectAttempts: -1 });
const service = createBridgeService({ config, client, store, platform, log, sc, runtime });
const subscription = nc.subscribe(config.subject(), {
  callback: (_error, message) => void service.respond(message),
});

client.on("qr", async (qr) => {
  runtime.status = "qr";
  runtime.authenticated = false;
  runtime.error = null;
  fs.mkdirSync(path.dirname(config.qrPath), { recursive: true });
  await QRCode.toFile(config.qrPath, qr, { width: 420, margin: 2 });
  const terminalQr = await QRCode.toString(qr, { type: "terminal", small: true });
  process.stdout.write(`\n${terminalQr}\n`);
  log.warn({ qrPath: config.qrPath }, "WhatsApp Web bridge needs pairing; scan the terminal QR or saved PNG");
});
client.on("authenticated", () => {
  runtime.status = "authenticated";
  runtime.authenticated = true;
  runtime.error = null;
  log.info("WhatsApp Web bridge authenticated");
});
client.on("ready", async () => {
  runtime.status = "ready";
  runtime.authenticated = true;
  runtime.error = null;
  runtime.webVersion = await client.getWWebVersion().catch(() => null);
  try { fs.unlinkSync(config.qrPath); } catch {}
  log.info({ workerId: config.workerId, subject: config.subject(), mode: config.mode, webVersion: runtime.webVersion }, "WhatsApp Web history bridge ready");
});
client.on("auth_failure", (message) => {
  runtime.status = "auth_failure";
  runtime.authenticated = false;
  runtime.error = String(message || "Authentication failed");
  log.error({ error: runtime.error }, "WhatsApp Web bridge authentication failed");
});
client.on("disconnected", (reason) => {
  runtime.status = "disconnected";
  runtime.authenticated = false;
  runtime.error = String(reason || "Disconnected");
  log.warn({ reason: runtime.error }, "WhatsApp Web bridge disconnected");
});

async function shutdown(signal) {
  log.info({ signal }, "Stopping WhatsApp Web history bridge");
  subscription.unsubscribe();
  await nc.drain().catch(() => {});
  try {
    if (config.mode === "browser_url") {
      await client.pupPage?.close().catch(() => {});
      client.pupBrowser?.disconnect?.();
    } else {
      await client.destroy();
    }
  } catch {}
  releaseLock();
  process.exit(0);
}
process.once("SIGINT", () => void shutdown("SIGINT"));
process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("exit", releaseLock);

async function verifyBrowserEndpoint() {
  if (config.mode !== "browser_url") return;
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
  } catch (error) {
    throw new Error(
      `Cannot attach to the visible Brave profile at ${config.browserUrl}. Close all Brave windows, run `
      + `whatsapp-web-history-bridge/scripts/launch-brave-debug.sh, verify the required WhatsApp chat/files are visible there, then restart dev.sh. `
      + formatError(error, "browser_debug_endpoint"),
    );
  } finally {
    clearTimeout(timer);
  }
}

log.info({ workerId: config.workerId, subject: config.subject(), mode: config.mode, wwebjs: "1.34.7", protocolVersion: config.protocolVersion }, "Starting WhatsApp Web history bridge");
verifyBrowserEndpoint()
  .then(() => client.initialize())
  .catch((error) => {
    runtime.status = "failed";
    runtime.error = formatError(error, "bridge_initialize");
    log.error({ error: runtime.error, stack: error?.stack || null }, "WhatsApp Web bridge initialization failed");
  });
