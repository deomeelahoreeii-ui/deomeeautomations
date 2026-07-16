import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

const HERE = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(HERE, "..");
dotenv.config({ path: path.join(ROOT, ".env") });

function parseBoolean(name, fallback) {
  const raw = process.env[name];
  if (raw == null || raw === "") return fallback;
  return ["1", "true", "yes", "on"].includes(String(raw).trim().toLowerCase());
}

function parseInteger(name, fallback, { min = 1, max = Number.MAX_SAFE_INTEGER } = {}) {
  const parsed = Number.parseInt(process.env[name] || "", 10);
  const value = Number.isFinite(parsed) ? parsed : fallback;
  return Math.max(min, Math.min(max, value));
}

function resolveFromRoot(value) {
  return path.resolve(ROOT, value);
}

function defaultBrowserExecutable() {
  const candidates = [
    "/usr/bin/brave",
    "/usr/bin/brave-browser",
    "/usr/bin/google-chrome-stable",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium",
    "/usr/bin/chromium-browser",
  ];
  return candidates.find((candidate) => fs.existsSync(candidate)) || "";
}

const mode = String(process.env.WWEBJS_MODE || "browser_url").trim().toLowerCase();
if (!["local_auth", "browser_url"].includes(mode)) {
  throw new Error("WWEBJS_MODE must be local_auth or browser_url");
}

const workerId = String(process.env.WWEBJS_WORKER_ID || process.env.WA_WORKER_ID || "default").trim();
if (!workerId) throw new Error("WWEBJS_WORKER_ID cannot be empty");

export const config = Object.freeze({
  protocolVersion: 2,
  workerId,
  natsUrl: process.env.NATS_URL || "nats://127.0.0.1:4222",
  historySubject: process.env.WWEBJS_HISTORY_SUBJECT || "whatsapp.web.inbound.history",
  mode,
  allowLocalAuthHistory: parseBoolean("WWEBJS_ALLOW_LOCAL_AUTH_HISTORY", false),
  browserUrl: String(process.env.WWEBJS_BROWSER_URL || "http://127.0.0.1:9222").trim(),
  browserExecutable: String(process.env.WWEBJS_BROWSER_EXECUTABLE || defaultBrowserExecutable()).trim(),
  headless: parseBoolean("WWEBJS_HEADLESS", false),
  authPath: resolveFromRoot(process.env.WWEBJS_AUTH_PATH || "data/auth"),
  qrPath: resolveFromRoot(process.env.WWEBJS_QR_PATH || "data/login-qr.png"),
  statePath: resolveFromRoot(process.env.WWEBJS_STATE_PATH || "data/history-state.json"),
  lockPath: resolveFromRoot(process.env.WWEBJS_LOCK_PATH || "data/bridge.lock"),
  clientId: String(process.env.WWEBJS_CLIENT_ID || `deomee-${workerId}`).trim(),
  maxHistoryCount: parseInteger("WWEBJS_MAX_HISTORY_COUNT", 200, { min: 1, max: 1000 }),
  maxMessagesScanned: parseInteger("WWEBJS_MAX_MESSAGES_SCANNED", 5000, { min: 100, max: 50000 }),
  requestTimeoutMs: parseInteger("WWEBJS_REQUEST_TIMEOUT_MS", 600000, { min: 30000, max: 3600000 }),
  fetchAttemptTimeoutMs: parseInteger("WWEBJS_FETCH_ATTEMPT_TIMEOUT_MS", 120000, { min: 5000, max: 600000 }),
  syncHistory: parseBoolean("WWEBJS_SYNC_HISTORY", true),
  syncHistoryTimeoutMs: parseInteger("WWEBJS_SYNC_HISTORY_TIMEOUT_MS", 30000, { min: 1000, max: 180000 }),
  syncSettleMs: parseInteger("WWEBJS_SYNC_SETTLE_MS", 3500, { min: 0, max: 30000 }),
  mediaMaxBytes: parseInteger("WWEBJS_MEDIA_MAX_BYTES", 75 * 1024 * 1024, { min: 1024, max: 1024 * 1024 * 1024 }),
  platformUrl: String(process.env.WWEBJS_PLATFORM_URL || process.env.WA_INBOUND_PLATFORM_URL || "http://127.0.0.1:8020").replace(/\/$/, ""),
  platformToken: String(process.env.WWEBJS_PLATFORM_TOKEN || process.env.WA_INBOUND_PLATFORM_TOKEN || "").trim(),
  platformTimeoutMs: parseInteger("WWEBJS_PLATFORM_TIMEOUT_MS", 180000, { min: 5000, max: 600000 }),
  logLevel: process.env.LOG_LEVEL || "info",
  subject() {
    return `${this.historySubject}.${this.workerId}`;
  },
});
