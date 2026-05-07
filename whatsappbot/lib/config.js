import fs from "fs";
import path from "path";
import "dotenv/config";

function parseInteger(name, fallback) {
  const value = process.env[name];

  if (value == null || value === "") {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseBoolean(name, fallback) {
  const value = process.env[name];

  if (value == null || value === "") {
    return fallback;
  }

  const normalized = String(value).trim().toLowerCase();

  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }

  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }

  return fallback;
}

function resolveFromRoot(value) {
  return path.resolve(process.cwd(), value);
}

export const config = {
  authDir: resolveFromRoot(process.env.WA_AUTH_DIR || "auth_info_baileys"),
  browserName: process.env.WA_BROWSER_NAME || "Chrome",
  browserPlatform: process.env.WA_BROWSER_PLATFORM || "Windows",
  browserVersion: process.env.WA_BROWSER_VERSION || "122.0.0.0",
  consumerName: process.env.NATS_CONSUMER_NAME || "whatsapp_worker_1",
  deadLetterMaxMessages: parseInteger("NATS_DEAD_MAX_MESSAGES", 1000),
  deadLetterStream:
    process.env.NATS_DEAD_LETTER_STREAM || "whatsapp_dead_stream",
  deadLetterSubject:
    process.env.NATS_DEAD_LETTER_SUBJECT || "whatsapp.dead",
  defaultSendDelayMs: parseInteger("WA_SEND_DELAY_MS", 1500),
  enableGroupDiscovery: parseBoolean("WA_ENABLE_GROUP_DISCOVERY", true),
  groupsFile:
    resolveFromRoot(process.env.WA_GROUPS_FILE || "data/discovered-groups.csv"),
  groupMetadataFailureCooldownMs: parseInteger(
    "WA_GROUP_METADATA_FAILURE_COOLDOWN_MS",
    60000,
  ),
  groupMetadataMinIntervalMs: parseInteger(
    "WA_GROUP_METADATA_MIN_INTERVAL_MS",
    1500,
  ),
  groupMetadataRateLimitCooldownMs: parseInteger(
    "WA_GROUP_METADATA_RATELIMIT_COOLDOWN_MS",
    300000,
  ),
  logLevel: process.env.LOG_LEVEL || "info",
  natsAckWaitMs: parseInteger("NATS_ACK_WAIT_MS", 120000),
  natsMaxDeliver: parseInteger("NATS_MAX_DELIVER", 5),
  natsNakDelayMs: parseInteger("NATS_NAK_DELAY_MS", 5000),
  natsStream: process.env.NATS_STREAM || "pending_stream",
  natsSubject: process.env.NATS_SUBJECT || "whatsapp.pending",
  natsUrl: process.env.NATS_URL || "nats://localhost:4222",
  reconnectDelayMs: parseInteger("WA_RECONNECT_DELAY_MS", 5000),
  startupDelayMs: parseInteger("WA_STARTUP_DELAY_MS", 5000),
};

export function ensureRuntimeDirectories() {
  const directories = new Set([
    config.authDir,
    path.dirname(config.groupsFile),
  ]);

  for (const directory of directories) {
    fs.mkdirSync(directory, { recursive: true });
  }
}

export function resolveAgainstFile(filePath, targetPath) {
  if (!targetPath) {
    return null;
  }

  return path.isAbsolute(targetPath)
    ? targetPath
    : path.resolve(path.dirname(filePath), targetPath);
}

export function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
