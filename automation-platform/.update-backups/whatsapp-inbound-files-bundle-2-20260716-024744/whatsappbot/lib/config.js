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

function parseMinimumMessageStatus() {
  const value = String(process.env.WA_DELIVERY_MIN_STATUS || "server_ack")
    .trim()
    .toLowerCase();
  const statuses = {
    pending: 1,
    server_ack: 2,
    sent: 2,
    delivery_ack: 3,
    delivered: 3,
    read: 4,
  };

  if (value in statuses) {
    return statuses[value];
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : statuses.server_ack;
}

function resolveFromRoot(value) {
  return path.resolve(process.cwd(), value);
}

function parseDirectJidPolicy() {
  const value = String(process.env.WA_DIRECT_JID_POLICY || "phone_only")
    .trim()
    .toLowerCase();
  if (["phone_only", "lid_preferred"].includes(value)) {
    return value;
  }
  return "phone_only";
}

const workerId = process.env.WA_WORKER_ID || "default";

export const config = {
  allowDirectGroupAdds: parseBoolean("WA_ALLOW_DIRECT_GROUP_ADDS", false),
  workerId,
  authDir: resolveFromRoot(process.env.WA_AUTH_DIR || "auth_info_baileys"),
  browserName: process.env.WA_BROWSER_NAME || "Chrome",
  browserPlatform: process.env.WA_BROWSER_PLATFORM || "Windows",
  consumerName: process.env.NATS_CONSUMER_NAME || "whatsapp_worker_1",
  deadLetterMaxMessages: parseInteger("NATS_DEAD_MAX_MESSAGES", 1000),
  deadLetterStream:
    process.env.NATS_DEAD_LETTER_STREAM || "whatsapp_dead_stream",
  deadLetterSubject:
    process.env.NATS_DEAD_LETTER_SUBJECT || "whatsapp.dead",
  deliveryAuditPath:
    resolveFromRoot(process.env.WA_DELIVERY_AUDIT_PATH || "data/delivery-audit.jsonl"),
  directJidPolicy: parseDirectJidPolicy(),
  defaultSendDelayMs: parseInteger("WA_SEND_DELAY_MS", 1500),
  enableGroupDiscovery: parseBoolean("WA_ENABLE_GROUP_DISCOVERY", true),
  enableInboundCapture: parseBoolean("WA_ENABLE_INBOUND_CAPTURE", true),
  inboundStorePath: resolveFromRoot(process.env.WA_INBOUND_STORE_PATH || `data/whatsapp-inbound-${workerId}.sqlite`),
  inboundPlatformUrl: process.env.WA_PLATFORM_URL || "http://127.0.0.1:8020",
  inboundPlatformToken: process.env.WA_PLATFORM_INGEST_TOKEN || "",
  inboundPlatformTimeoutMs: parseInteger("WA_PLATFORM_TIMEOUT_MS", 10000),
  inboundOutboxFlushMs: parseInteger("WA_INBOUND_OUTBOX_FLUSH_MS", 5000),
  inboundOutboxBatchSize: parseInteger("WA_INBOUND_OUTBOX_BATCH_SIZE", 100),
  groupsFile:
    resolveFromRoot(process.env.WA_GROUPS_FILE || "data/discovered-groups.csv"),
  qrImagePath:
    resolveFromRoot(process.env.WA_QR_IMAGE_PATH || "data/whatsapp-login-qr.png"),
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
  logFile: resolveFromRoot(process.env.WA_LOG_FILE || "data/worker-runtime.log"),
  lockFile: resolveFromRoot(process.env.WA_WORKER_LOCK_FILE || "data/worker.lock"),
  natsAckWaitMs: parseInteger("NATS_ACK_WAIT_MS", 120000),
  natsMaxDeliver: parseInteger("NATS_MAX_DELIVER", 5),
  natsNakDelayMs: parseInteger("NATS_NAK_DELAY_MS", 5000),
  messageAckTimeoutMs: parseInteger("WA_MESSAGE_ACK_TIMEOUT_MS", 30000),
  minimumMessageStatus: parseMinimumMessageStatus(),
  natsStream: process.env.NATS_STREAM || "pending_stream",
  natsSubject: process.env.NATS_SUBJECT || "whatsapp.pending",
  healthSubject:
    process.env.NATS_HEALTH_SUBJECT || "whatsapp.worker.health",
  groupMembersSubject:
    process.env.NATS_GROUP_MEMBERS_SUBJECT || "whatsapp.worker.group-members",
  groupDirectorySubject:
    process.env.NATS_GROUP_DIRECTORY_SUBJECT || "whatsapp.worker.directory.groups",
  identityDirectorySubject:
    process.env.NATS_IDENTITY_DIRECTORY_SUBJECT || "whatsapp.worker.directory.identities",
  identityStorePath:
    resolveFromRoot(
      process.env.WA_IDENTITY_STORE_PATH ||
        `data/whatsapp-identities-${workerId}.json`,
    ),
  natsUrl: process.env.NATS_URL || "nats://localhost:4222",
  whatsappConnectTimeoutMs: parseInteger("WA_CONNECT_TIMEOUT_MS", 60000),
  reconnectDelayMs: parseInteger("WA_RECONNECT_DELAY_MS", 5000),
  startupDelayMs: parseInteger("WA_STARTUP_DELAY_MS", 5000),
};

export function ensureRuntimeDirectories() {
  const directories = new Set([
    config.authDir,
    path.dirname(config.groupsFile),
    path.dirname(config.deliveryAuditPath),
    path.dirname(config.identityStorePath),
    path.dirname(config.inboundStorePath),
    path.dirname(config.logFile),
    path.dirname(config.qrImagePath),
    path.dirname(config.lockFile),
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
