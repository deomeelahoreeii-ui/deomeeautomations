import fs from "fs";
import path from "path";
import {
  connect,
  StringCodec,
  AckPolicy,
  nanos,
} from "nats";
import makeWASocket, {
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
} from "@whiskeysockets/baileys";
import {
  buildMergedTcTokenIndexWrite,
  isTcTokenExpired,
  storeTcTokensFromIqResult,
} from "@whiskeysockets/baileys/lib/Utils/tc-token-utils.js";
import pino from "pino";
import QRCode from "qrcode";

import { config, ensureRuntimeDirectories, sleep } from "./lib/config.js";
import { createGroupDiscovery } from "./lib/group-discovery.js";
import {
  canonicalLidJid,
  canonicalPhoneJid,
  createWhatsAppIdentityStore,
  isLidJid,
  isPhoneJid,
} from "./lib/identity-store.js";
import { resolveJobPayload, ValidationError } from "./lib/jobs.js";
import { ensureJetStreamWorkerResources } from "./lib/nats-setup.js";
import { createLogger } from "./lib/logger.js";

const log = createLogger("worker");
const sc = StringCodec();
const identityStore = createWhatsAppIdentityStore({
  authDir: config.authDir,
  filePath: config.identityStorePath,
  log,
});
const groupDiscovery = config.enableGroupDiscovery
  ? createGroupDiscovery({ config, log })
  : null;
const MAX_WHATSAPP_CAPTION_CHARS = 900;
const MAX_WHATSAPP_TEXT_CHARS = 3500;
const MESSAGE_STATUS = {
  ERROR: 0,
  PENDING: 1,
  SERVER_ACK: 2,
  DELIVERY_ACK: 3,
  READ: 4,
  PLAYED: 5,
};

class ParticipantUpdateError extends ValidationError {
  constructor(message, statusCodes, operationResult) {
    super(message);
    this.name = "ParticipantUpdateError";
    this.statusCodes = statusCodes;
    this.operationResult = operationResult;
  }
}

class MessageAcknowledgementTimeoutError extends ValidationError {
  constructor(message, operationResult) {
    super(message);
    this.name = "MessageAcknowledgementTimeoutError";
    this.operationResult = operationResult;
  }
}

class WhatsAppMessageStatusError extends ValidationError {
  constructor(message, operationResult, statusCodes = []) {
    super(message);
    this.name = "WhatsAppMessageStatusError";
    this.operationResult = operationResult;
    this.statusCodes = statusCodes;
  }
}

let activeSocket = null;
let activeNatsConnection = null;
let activeHealthNatsConnection = null;
let activeJetStream = null;
let activeConsumerMessages = null;
let isWhatsAppConnected = false;
let natsStartInProgress = false;
let healthResponderStartInProgress = false;
let healthResponderRetryTimer = null;
let workerStartInProgress = false;
let reconnectTimer = null;
let connectionOpenTimer = null;
let shuttingDown = false;
let workerLockOwned = false;
let identityPolicyInitialized = false;
let lastConnectionStatus = "starting";
let lastDisconnectInfo = null;
let credentialSaveQueue = Promise.resolve();
let credentialSaveError = null;
const pendingMessageStatusWaiters = new Map();
const recentMessageStatuses = new Map();
let activeAuthState = null;

ensureRuntimeDirectories();

function processIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function processLooksLikeThisWorker(pid) {
  if (!processIsAlive(pid)) {
    return false;
  }

  if (process.platform !== "linux") {
    return true;
  }

  try {
    const currentCwd = fs.realpathSync(process.cwd());
    const lockedCwd = fs.realpathSync(`/proc/${pid}/cwd`);
    const args = fs
      .readFileSync(`/proc/${pid}/cmdline`, "utf8")
      .split("\0")
      .filter(Boolean);
    const executable = path.basename(args[0] || "");
    const workerScript = path.resolve(currentCwd, "worker.js");
    const hasWorkerScript = args.some((arg) => {
      if (path.basename(arg) === "worker.js") {
        return true;
      }
      return path.resolve(currentCwd, arg) === workerScript;
    });

    return lockedCwd === currentCwd && executable.includes("node") && hasWorkerScript;
  } catch {
    return false;
  }
}

function acquireWorkerLock() {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const descriptor = fs.openSync(config.lockFile, "wx");
      fs.writeFileSync(descriptor, `${process.pid}\n`, "utf8");
      fs.closeSync(descriptor);
      workerLockOwned = true;
      return;
    } catch (error) {
      if (error.code !== "EEXIST") {
        throw error;
      }
      const existingPid = Number.parseInt(
          fs.readFileSync(config.lockFile, "utf8").trim(),
        10,
      );
      if (processLooksLikeThisWorker(existingPid)) {
        throw new Error(
          `Another WhatsApp worker is already running with PID ${existingPid}`,
        );
      }
      log.warn("Removing stale WhatsApp worker lock", {
        lockFile: config.lockFile,
        pid: Number.isFinite(existingPid) ? existingPid : null,
      });
      fs.rmSync(config.lockFile, { force: true });
    }
  }
  throw new Error(`Could not acquire worker lock: ${config.lockFile}`);
}

function releaseWorkerLock() {
  if (!workerLockOwned) {
    return;
  }
  try {
    const ownerPid = Number.parseInt(
      fs.readFileSync(config.lockFile, "utf8").trim(),
      10,
    );
    if (ownerPid === process.pid) {
      fs.rmSync(config.lockFile, { force: true });
    }
  } catch {
    // Lock cleanup is best effort during process exit.
  }
  workerLockOwned = false;
}

function queueCredentialSave(saveCreds) {
  credentialSaveQueue = credentialSaveQueue
    .then(() => saveCreds())
    .catch((error) => {
      credentialSaveError = error;
      log.error("Failed to persist WhatsApp credentials", {
        error: error.message,
        authDir: config.authDir,
      });
    });
  return credentialSaveQueue;
}

async function flushCredentialSaves() {
  await credentialSaveQueue;
  if (credentialSaveError) {
    const error = credentialSaveError;
    credentialSaveError = null;
    throw error;
  }
}

try {
  acquireWorkerLock();
} catch (error) {
  log.error("WhatsApp worker refused to start", {
    error: error.message,
    lockFile: config.lockFile,
  });
  process.exit(1);
}

function splitTextIntoChunks(text, maxChars = MAX_WHATSAPP_TEXT_CHARS) {
  const normalized = String(text || "").trim();

  if (!normalized) {
    return [];
  }

  if (normalized.length <= maxChars) {
    return [normalized];
  }

  const lines = normalized.split("\n");
  const chunks = [];
  let currentChunk = "";

  for (const line of lines) {
    const candidate = currentChunk
      ? `${currentChunk}\n${line}`
      : line;

    if (candidate.length <= maxChars) {
      currentChunk = candidate;
      continue;
    }

    if (currentChunk) {
      chunks.push(currentChunk);
      currentChunk = "";
    }

    if (line.length <= maxChars) {
      currentChunk = line;
      continue;
    }

    let remaining = line;
    while (remaining.length > maxChars) {
      let splitAt = remaining.lastIndexOf(" ", maxChars);
      if (splitAt < Math.floor(maxChars * 0.6)) {
        splitAt = maxChars;
      }
      chunks.push(remaining.slice(0, splitAt).trim());
      remaining = remaining.slice(splitAt).trim();
    }

    currentChunk = remaining;
  }

  if (currentChunk) {
    chunks.push(currentChunk);
  }

  return chunks;
}

async function sendTextMessages(sock, target, text) {
  const results = [];
  for (const chunk of splitTextIntoChunks(text)) {
    results.push(await sock.sendMessage(target, { text: chunk }));
  }
  return results;
}

function resolveKnownLidTarget(target) {
  if (!isPhoneJid(target)) {
    return target;
  }

  const lidTarget = identityStore.findLidForPhoneJid(target);
  return lidTarget || target;
}

function getKnownPhoneTargetByLid(lidTarget) {
  return identityStore.findPhoneForLidJid(lidTarget);
}

async function getLidForKnownPhoneTarget(target) {
  const resolved = resolveKnownLidTarget(target);
  return isLidJid(resolved) ? canonicalLidJid(resolved) : null;
}

function resolveIdentityTarget(job) {
  const requestedTarget = String(job.target || "");
  const knownLidTarget = isPhoneJid(requestedTarget)
    ? identityStore.findLidForPhoneJid(requestedTarget)
    : canonicalLidJid(requestedTarget);
  const knownPhoneTarget =
    job.type === "contact"
      ? isLidJid(requestedTarget)
        ? identityStore.findPhoneForLidJid(requestedTarget)
        : canonicalPhoneJid(requestedTarget)
      : requestedTarget;
  let actualSendTarget = requestedTarget;
  let reason = "target used as requested";

  if (job.type === "contact" && isPhoneJid(requestedTarget)) {
    if (config.directJidPolicy === "lid_preferred" && knownLidTarget) {
      actualSendTarget = knownLidTarget;
      reason = "configured to prefer known LID alias";
    } else if (knownLidTarget) {
      reason = "known LID alias retained, but proactive policy uses phone JID";
    } else {
      reason = "no known LID alias; phone JID used";
    }
  } else if (job.type === "contact" && isLidJid(requestedTarget)) {
    actualSendTarget = canonicalLidJid(requestedTarget) || requestedTarget;
    reason = "job explicitly requested LID target";
  }

  return {
    actualSendTarget,
    canonicalTarget: knownPhoneTarget || requestedTarget,
    knownLidTarget: knownLidTarget || null,
    policy: config.directJidPolicy,
    reason,
    requestedTarget,
    usedLidAlias: actualSendTarget !== requestedTarget && isLidJid(actualSendTarget),
  };
}

function uniqueValues(values) {
  return [
    ...new Set(
      values
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  ];
}

function observeIdentityFromOperationResult(job, identityResolution, operationResult, source) {
  const returnedRemoteJids = uniqueValues(
    (operationResult || []).map((item) => item?.remoteJid),
  );
  const phoneCandidates = uniqueValues([
    identityResolution.canonicalTarget,
    identityResolution.requestedTarget,
    identityResolution.actualSendTarget,
    ...returnedRemoteJids,
  ]).filter(isPhoneJid);
  const lidCandidates = uniqueValues([
    identityResolution.knownLidTarget,
    identityResolution.requestedTarget,
    identityResolution.actualSendTarget,
    ...returnedRemoteJids,
  ])
    .filter(isLidJid)
    .map(canonicalLidJid)
    .filter(Boolean);

  const phoneJid = phoneCandidates[0] || getKnownPhoneTargetByLid(lidCandidates[0]);
  const observedLidJids = [];
  if (phoneJid) {
    for (const lidJid of lidCandidates) {
      identityStore.observe({
        phoneJid,
        lidJid,
        source,
        contactName: job.recipientName || "",
        confidence: source === "delivery_remote_jid" ? 0.95 : 0.8,
        evidence: {
          actualSendTarget: identityResolution.actualSendTarget,
          jobId: job.jobId,
          requestedTarget: identityResolution.requestedTarget,
          returnedRemoteJids,
        },
      });
      observedLidJids.push(lidJid);
    }
  }

  identityResolution.identityPhoneJid = phoneJid || null;
  identityResolution.observedLidJids = uniqueValues(observedLidJids);
  identityResolution.returnedRemoteJids = returnedRemoteJids;
  return identityResolution;
}

async function getPrivacyTokenEntry(target) {
  if (!activeAuthState || !String(target || "").endsWith("@lid")) {
    return null;
  }

  const data = await activeAuthState.keys.get("tctoken", [target]);
  return data?.[target] || null;
}

function summarizePrivacyTokenEntry(entry) {
  const tokenLength = entry?.token?.length || 0;
  const timestamp = entry?.timestamp ? Number(entry.timestamp) : null;
  const expired = tokenLength > 0 ? isTcTokenExpired(timestamp) : null;

  return {
    hasToken: tokenLength > 0,
    tokenLength,
    timestamp,
    senderTimestamp: entry?.senderTimestamp ? Number(entry.senderTimestamp) : null,
    expired,
    usable: tokenLength > 0 && expired === false,
  };
}

async function choosePrivacyTokenIssueTarget(sock, lidTarget) {
  if (sock.serverProps?.lidTrustedTokenIssueToLid) {
    return lidTarget;
  }

  return getKnownPhoneTargetByLid(lidTarget) || lidTarget;
}

async function issueAndStorePrivacyToken(sock, lidTarget) {
  if (!activeAuthState || typeof sock.issuePrivacyTokens !== "function") {
    return { issued: false, reason: "Baileys privacy token issuer unavailable" };
  }

  const issueTarget = await choosePrivacyTokenIssueTarget(sock, lidTarget);
  const issueTimestamp = Math.floor(Date.now() / 1000);
  const result = await sock.issuePrivacyTokens([issueTarget], issueTimestamp);

  await storeTcTokensFromIqResult({
    result,
    fallbackJid: lidTarget,
    keys: activeAuthState.keys,
    getLIDForPN: getLidForKnownPhoneTarget,
  });

  const currentData = await activeAuthState.keys.get("tctoken", [lidTarget]);
  const currentEntry = currentData?.[lidTarget];
  const indexWrite = await buildMergedTcTokenIndexWrite(activeAuthState.keys, [
    lidTarget,
  ]);

  await activeAuthState.keys.set({
    tctoken: {
      [lidTarget]: {
        token: Buffer.alloc(0),
        ...currentEntry,
        senderTimestamp: issueTimestamp,
      },
      ...indexWrite,
    },
  });

  return {
    issued: true,
    issueTarget,
    issueTimestamp,
  };
}

async function ensurePrivacyTokenForDirectSend(sock, target) {
  if (!String(target || "").endsWith("@lid")) {
    return null;
  }

  const before = summarizePrivacyTokenEntry(await getPrivacyTokenEntry(target));
  if (before.usable) {
    return { checked: true, recovered: false, before, after: before };
  }

  let issuance;
  try {
    issuance = await issueAndStorePrivacyToken(sock, target);
  } catch (error) {
    throw new ValidationError(
      "Could not issue WhatsApp privacy token (tctoken) for direct LID send " +
        `${target}: ${error.message}. Direct send was blocked so fallback ` +
        "escalation can deliver the report.",
    );
  }
  const after = summarizePrivacyTokenEntry(await getPrivacyTokenEntry(target));
  const result = {
    checked: true,
    recovered: after.usable,
    before,
    after,
    issuance,
  };

  if (!after.usable) {
    throw new ValidationError(
      "Missing usable WhatsApp privacy token (tctoken) for direct LID send " +
        `${target}. Baileys issued a fresh token request but no valid token was ` +
        "stored; direct send was blocked so fallback escalation can deliver the report.",
    );
  }

  log.info("Prepared WhatsApp privacy token for direct send", {
    target,
    recovered: result.recovered,
    issueTarget: issuance.issueTarget,
    tokenTimestamp: after.timestamp,
  });

  return result;
}

function isFatalDisconnect(statusCode) {
  return [
    DisconnectReason.loggedOut,
    DisconnectReason.badSession,
    DisconnectReason.multideviceMismatch,
    DisconnectReason.connectionReplaced,
    DisconnectReason.forbidden,
  ].includes(statusCode);
}

function configuredBrowserIdentity() {
  const platform = String(config.browserPlatform || "Windows").toLowerCase();
  if (platform === "windows") {
    return Browsers.windows(config.browserName);
  }
  if (platform === "mac" || platform === "macos" || platform === "mac os") {
    return Browsers.macOS(config.browserName);
  }
  if (platform === "ubuntu" || platform === "linux") {
    return Browsers.ubuntu(config.browserName);
  }
  return Browsers.appropriate(config.browserName);
}

function scheduleReconnect(reason) {
  if (shuttingDown || reconnectTimer) {
    return;
  }

  log.warn(`Scheduling WhatsApp reconnect in ${config.reconnectDelayMs}ms`, {
    reason,
  });

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    startWorker().catch((error) => {
      log.error("Worker restart failed", { error: error.message });
      scheduleReconnect("worker restart failed");
    });
  }, config.reconnectDelayMs);
}

function clearConnectionOpenTimer() {
  if (connectionOpenTimer) {
    clearTimeout(connectionOpenTimer);
    connectionOpenTimer = null;
  }
}

function scheduleConnectionOpenTimeout(sock) {
  clearConnectionOpenTimer();
  connectionOpenTimer = setTimeout(() => {
    connectionOpenTimer = null;
    if (
      shuttingDown ||
      activeSocket !== sock ||
      isWhatsAppConnected ||
      workerStartInProgress
    ) {
      return;
    }

    lastConnectionStatus = "connect_timeout";
    log.warn("WhatsApp connection did not open before timeout; reconnecting", {
      timeoutMs: config.whatsappConnectTimeoutMs,
    });

    stopSocket()
      .catch((error) => {
        log.warn("Failed to stop timed-out WhatsApp socket", {
          error: error.message,
        });
      })
      .finally(() => {
        if (!shuttingDown && activeSocket === null && !isWhatsAppConnected) {
          scheduleReconnect("WhatsApp connection open timeout");
        }
      });
  }, config.whatsappConnectTimeoutMs);
}

async function writeLoginQrImage(qr) {
  await QRCode.toFile(config.qrImagePath, qr, {
    errorCorrectionLevel: "M",
    margin: 4,
    width: 480,
  });
  log.info("WhatsApp login QR code is ready", {
    qrImagePath: config.qrImagePath,
  });
}

function removeLoginQrImage() {
  try {
    fs.rmSync(config.qrImagePath, { force: true });
  } catch (error) {
    log.warn("Could not remove expired WhatsApp QR image", {
      error: error.message,
      qrImagePath: config.qrImagePath,
    });
  }
}

async function stopNatsConsumer() {
  const iter = activeConsumerMessages;
  const nc = activeNatsConnection;

  activeConsumerMessages = null;
  activeNatsConnection = null;
  activeJetStream = null;
  natsStartInProgress = false;

  if (iter) {
    try {
      await iter.close();
    } catch (error) {
      log.warn("Failed to close NATS iterator cleanly", {
        error: error.message,
      });
    }
  }

  if (nc) {
    try {
      await nc.close();
    } catch (error) {
      log.warn("Failed to close NATS connection cleanly", {
        error: error.message,
      });
    }
  }
}

function isQueueReady() {
  return Boolean(
    isWhatsAppConnected &&
      activeSocket &&
      activeNatsConnection &&
      activeConsumerMessages,
  );
}

function workerHealthPayload() {
  const ready = isQueueReady();
  let status = "waiting_for_whatsapp";

  if (shuttingDown) {
    status = "stopping";
  } else if (ready) {
    status = "ready";
  } else if (reconnectTimer) {
    status = "reconnect_scheduled";
  } else if (workerStartInProgress) {
    status = "starting_whatsapp";
  } else if (!activeSocket) {
    status = "waiting_for_reconnect";
  } else if (isWhatsAppConnected && natsStartInProgress) {
    status = "starting_nats_consumer";
  } else if (isWhatsAppConnected) {
    status = "waiting_for_nats_consumer";
  }

  return {
    ready,
    status,
    checkedAt: new Date().toISOString(),
    workerId: config.workerId,
    whatsappConnected: isWhatsAppConnected,
    natsConsumerReady: ready,
    hasActiveSocket: Boolean(activeSocket),
    reconnectScheduled: Boolean(reconnectTimer),
    workerStartInProgress,
    lastConnectionStatus,
    lastDisconnect: lastDisconnectInfo,
    identityStore: identityStore.stats(),
    targetPolicy: config.directJidPolicy,
  };
}

function initializeIdentityPolicy() {
  if (identityPolicyInitialized) {
    return;
  }

  identityStore.seedFromAuthMappings();
  identityPolicyInitialized = true;
  log.info("WhatsApp identity policy initialized", {
    identityStore: identityStore.stats(),
    targetPolicy: config.directJidPolicy,
  });
}

function scheduleHealthResponderRetry(reason) {
  if (shuttingDown || healthResponderRetryTimer) {
    return;
  }

  log.warn("Scheduling NATS health responder reconnect", {
    reason,
    delayMs: config.reconnectDelayMs,
  });

  healthResponderRetryTimer = setTimeout(() => {
    healthResponderRetryTimer = null;
    startHealthResponder().catch((error) => {
      log.error("NATS health responder restart failed", {
        error: error.message,
      });
      scheduleHealthResponderRetry("health responder restart failed");
    });
  }, config.reconnectDelayMs);
}

async function startHealthResponder() {
  if (
    shuttingDown ||
    healthResponderStartInProgress ||
    activeHealthNatsConnection
  ) {
    return;
  }

  healthResponderStartInProgress = true;
  let nc;

  try {
    nc = await connect({ servers: config.natsUrl });
    if (shuttingDown) {
      await nc.close();
      return;
    }

    nc.subscribe(config.healthSubject, {
      callback: (error, message) => {
        if (error) {
          log.warn("NATS health request failed", { error: error.message });
          return;
        }

        const responded = message.respond(
          sc.encode(JSON.stringify(workerHealthPayload())),
        );
        if (!responded) {
          log.warn("NATS health request had no reply subject");
        }
      },
    });

    activeHealthNatsConnection = nc;
    log.info("NATS health responder is listening", {
      healthSubject: config.healthSubject,
      natsUrl: config.natsUrl,
    });

    nc.closed().then((error) => {
      if (activeHealthNatsConnection === nc) {
        activeHealthNatsConnection = null;
      }
      if (shuttingDown) {
        return;
      }
      log.warn("NATS health responder closed", {
        error: error?.message || null,
      });
      scheduleHealthResponderRetry("health responder closed");
    });
  } catch (error) {
    if (nc) {
      try {
        await nc.close();
      } catch {
        // Ignore close failures while handling startup failure.
      }
    }
    if (!shuttingDown) {
      log.warn("NATS health responder is unavailable", {
        error: error.message,
        natsUrl: config.natsUrl,
      });
      scheduleHealthResponderRetry("health responder startup failed");
    }
  } finally {
    healthResponderStartInProgress = false;
  }
}

async function stopHealthResponder() {
  if (healthResponderRetryTimer) {
    clearTimeout(healthResponderRetryTimer);
    healthResponderRetryTimer = null;
  }

  const nc = activeHealthNatsConnection;
  activeHealthNatsConnection = null;
  healthResponderStartInProgress = false;

  if (nc) {
    try {
      await nc.close();
    } catch (error) {
      log.warn("Failed to close NATS health responder cleanly", {
        error: error.message,
      });
    }
  }
}

async function stopSocket() {
  if (!activeSocket) {
    return;
  }

  clearConnectionOpenTimer();
  const socket = activeSocket;
  activeSocket = null;
  isWhatsAppConnected = false;
  activeAuthState = null;
  for (const [messageId, waiter] of pendingMessageStatusWaiters.entries()) {
    waiter.reject(
      new Error(`WhatsApp socket stopped before message ${messageId} was acknowledged`),
    );
  }
  pendingMessageStatusWaiters.clear();

  try {
    socket.end(new Error("Worker shutdown"));
  } catch (error) {
    log.warn("Failed to close WhatsApp socket cleanly", {
      error: error.message,
    });
  }
}

async function publishDeadLetter(reason, payload, details = {}) {
  if (!activeJetStream) {
    log.warn("Dead-letter publish skipped because JetStream is unavailable", {
      reason,
      target: payload?.target,
    });
    return;
  }

  const body = {
    failed_at: new Date().toISOString(),
    reason,
    payload,
    details,
  };

  try {
    await activeJetStream.publish(
      config.deadLetterSubject,
      sc.encode(JSON.stringify(body)),
    );
  } catch (error) {
    log.error("Failed to publish dead-letter event", {
      reason,
      error: error.message,
    });
  }
}

async function publishDeliveryStatus(payload, status, details = {}) {
  if (!payload?.statusSubject) {
    return;
  }

  if (!activeNatsConnection) {
    log.warn("Delivery status publish skipped because NATS is unavailable", {
      jobId: payload.jobId,
      status,
      target: payload.target,
    });
    return;
  }

  const body = {
    actualSendTarget: details.identityResolution?.actualSendTarget || null,
    batchId: payload.batchId,
    canonicalTarget: details.identityResolution?.canonicalTarget || payload.target,
    complaintCode: payload.complaintCode,
    deliveryCount: details.deliveryCount,
    dispatchRoute: payload.dispatchRoute,
    error: details.error,
    errorCode: details.errorCode,
    operation: payload.operation,
    operationResult: details.operationResult,
    identity: details.identityResolution || null,
    jobId: payload.jobId,
    knownLidTarget: details.identityResolution?.knownLidTarget || null,
    requestedTarget: details.identityResolution?.requestedTarget || payload.target,
    status,
    statusAt: new Date().toISOString(),
    target: payload.target,
    targetPolicy: details.identityResolution?.policy || config.directJidPolicy,
    type: payload.type,
  };

  try {
    activeNatsConnection.publish(
      payload.statusSubject,
      sc.encode(JSON.stringify(body)),
    );
    await activeNatsConnection.flush();
  } catch (error) {
    log.error("Failed to publish delivery status", {
      error: error.message,
      jobId: payload.jobId,
      status,
      target: payload.target,
    });
  }
}

function summarizeSendResult(result) {
  const key = result?.key || {};
  const status = result?.status ?? null;
  return {
    id: key.id || null,
    participant: key.participant || null,
    remoteJid: key.remoteJid || null,
    fromMe: key.fromMe ?? null,
    messageTimestamp: result?.messageTimestamp || null,
    status,
    statusName: status == null ? null : messageStatusName(status),
    statusAt: null,
  };
}

function messageStatusName(status) {
  return (
    Object.entries(MESSAGE_STATUS).find(([, value]) => value === status)?.[0] ||
    `UNKNOWN_${status}`
  );
}

function messageStatusMeetsMinimum(status) {
  return Number(status) >= config.minimumMessageStatus;
}

function updatePendingMessageStatus(messageId, status, updates = {}) {
  if (!messageId) {
    return;
  }

  recentMessageStatuses.set(messageId, {
    status,
    statusName: messageStatusName(status),
    statusAt: new Date().toISOString(),
    remoteJid: updates.remoteJid || null,
    participant: updates.participant || null,
  });

  if (!pendingMessageStatusWaiters.has(messageId)) {
    return;
  }

  const waiter = pendingMessageStatusWaiters.get(messageId);
  waiter.summary.status = status;
  waiter.summary.statusName = messageStatusName(status);
  waiter.summary.statusAt = new Date().toISOString();
  waiter.summary.remoteJid = updates.remoteJid || waiter.summary.remoteJid;
  waiter.summary.participant = updates.participant || waiter.summary.participant;

  if (status === MESSAGE_STATUS.ERROR) {
    const statusCodes = Array.isArray(updates.rawUpdate?.messageStubParameters)
      ? updates.rawUpdate.messageStubParameters.map((value) => String(value))
      : [];
    waiter.summary.errorCode = statusCodes[0] || null;
    waiter.summary.errorReason = statusCodes.slice(1).join(" ") || null;
    log.warn("WhatsApp reported a terminal message error", {
      messageId,
      remoteJid: waiter.summary.remoteJid,
      participant: waiter.summary.participant,
      status,
      statusName: waiter.summary.statusName,
      statusCodes,
      update: updates.rawUpdate,
    });
    waiter.reject(
      new WhatsAppMessageStatusError(
        `WhatsApp reported ERROR${statusCodes.length ? ` ${statusCodes.join(" ")}` : ""} for message ${messageId} to ${waiter.summary.remoteJid}`,
        waiter.operationResult || [waiter.summary],
        statusCodes,
      ),
    );
    pendingMessageStatusWaiters.delete(messageId);
    return;
  }

  if (messageStatusMeetsMinimum(status)) {
    waiter.resolve(waiter.summary);
    pendingMessageStatusWaiters.delete(messageId);
  }
}

async function waitForMessageAcknowledgements(sendResults) {
  const pendingResults = sendResults.filter(
    (result) => {
      if (!result.id) {
        return false;
      }

      const cachedStatus = recentMessageStatuses.get(result.id);
      if (cachedStatus) {
        result.status = cachedStatus.status;
        result.statusName = cachedStatus.statusName;
        result.statusAt = cachedStatus.statusAt;
        result.remoteJid = cachedStatus.remoteJid || result.remoteJid;
        result.participant = cachedStatus.participant || result.participant;
      }

      return !messageStatusMeetsMinimum(result.status);
    },
  );

  if (pendingResults.length === 0) {
    return sendResults;
  }

  await Promise.all(
    pendingResults.map(
      (summary) =>
        new Promise((resolve, reject) => {
          const timeout = setTimeout(() => {
            pendingMessageStatusWaiters.delete(summary.id);
            summary.statusName = messageStatusName(summary.status);
            summary.statusAt = new Date().toISOString();
            reject(
              new MessageAcknowledgementTimeoutError(
                `Timed out waiting for WhatsApp ${messageStatusName(config.minimumMessageStatus)} acknowledgement for message ${summary.id} to ${summary.remoteJid}; last status was ${messageStatusName(summary.status)}`,
                sendResults,
              ),
            );
          }, config.messageAckTimeoutMs);

          pendingMessageStatusWaiters.set(summary.id, {
            summary,
            operationResult: sendResults,
            resolve: (value) => {
              clearTimeout(timeout);
              resolve(value);
            },
            reject: (error) => {
              clearTimeout(timeout);
              reject(error);
            },
          });
        }),
    ),
  );

  return sendResults;
}

function operationResultHasMessageId(operationResult) {
  return Array.isArray(operationResult) && operationResult.some(
    (item) => item && typeof item === "object" && item.id,
  );
}

function operationResultHasTerminalError(operationResult) {
  return Array.isArray(operationResult) && operationResult.some(
    (item) => Number(item?.status) === MESSAGE_STATUS.ERROR,
  );
}

function shouldAcceptPendingGroupAck(payload, error) {
  return (
    payload?.type === "group" &&
    error instanceof MessageAcknowledgementTimeoutError &&
    operationResultHasMessageId(error.operationResult) &&
    !operationResultHasTerminalError(error.operationResult)
  );
}

function handleMessageUpdates(updates) {
  for (const update of updates || []) {
    const messageId = update?.key?.id;
    const status = update?.update?.status;
    if (messageId && status != null) {
      updatePendingMessageStatus(messageId, status, {
        remoteJid: update.key.remoteJid,
        participant: update.key.participant,
        rawUpdate: update.update,
      });
    }
  }
}

async function appendDeliveryAudit(payload, status, details = {}) {
  const auditRecord = {
    actualSendTarget: details.identityResolution?.actualSendTarget || null,
    auditedAt: new Date().toISOString(),
    canonicalTarget: details.identityResolution?.canonicalTarget || payload?.target || null,
    jobId: payload?.jobId || null,
    target: payload?.target || null,
    requestedTarget: details.identityResolution?.requestedTarget || payload?.target || null,
    type: payload?.type || null,
    operation: payload?.operation || null,
    recipientName: payload?.recipientName || null,
    dispatchRoute: payload?.dispatchRoute || null,
    identity: details.identityResolution || null,
    knownLidTarget: details.identityResolution?.knownLidTarget || null,
    targetPolicy: details.identityResolution?.policy || config.directJidPolicy,
    status,
    error: details.error || null,
    errorCode: details.errorCode || null,
    deliveryCount: details.deliveryCount || null,
    operationResult: details.operationResult || null,
  };

  try {
    await fs.promises.appendFile(
      config.deliveryAuditPath,
      `${JSON.stringify(auditRecord)}\n`,
      "utf8",
    );
  } catch (error) {
    log.warn("Failed to append delivery audit", {
      error: error.message,
      auditPath: config.deliveryAuditPath,
      jobId: payload?.jobId,
      target: payload?.target,
    });
  }
}

async function respondWithGroupMembers(sock, message) {
  let request = {};
  try {
    request = JSON.parse(sc.decode(message.data) || "{}");
    const groupJid = String(request.groupJid || request.group_jid || "").trim();
    if (!groupJid.endsWith("@g.us")) {
      throw new ValidationError("group_jid must end with @g.us");
    }
    const metadata = await sock.groupMetadata(groupJid);
    const members = (metadata.participants || []).map((participant) => ({
      admin: participant.admin || null,
      id: participant.id || null,
      lid: participant.lid || null,
      name: participant.name || participant.notify || null,
      phoneNumber: participant.phoneNumber ||
        (String(participant.id || "").endsWith("@s.whatsapp.net")
          ? participant.id
          : null),
    }));
    for (const member of members) {
      const phoneJid = member.phoneNumber || (isPhoneJid(member.id) ? member.id : null);
      const lidJid = member.lid || (isLidJid(member.id) ? member.id : null);
      if (phoneJid && lidJid) {
        identityStore.observe({
          phoneJid,
          lidJid,
          source: "group_metadata",
          contactName: member.name || "",
          confidence: 0.9,
          evidence: { groupJid, groupName: metadata.subject || "" },
        });
      }
    }
    message.respond(
      sc.encode(
        JSON.stringify({
          checkedAt: new Date().toISOString(),
          groupJid,
          groupName: metadata.subject || "",
          members,
          size: metadata.size ?? members.length,
          workerId: config.workerId,
        }),
      ),
    );
  } catch (error) {
    message.respond(
      sc.encode(
        JSON.stringify({
          error: error.message,
          groupJid: request.groupJid || request.group_jid || null,
          workerId: config.workerId,
        }),
      ),
    );
  }
}

function tokenDirectoryStatus(entry, hasLid) {
  if (!hasLid) {
    return { status: "not_applicable", checkedAt: new Date().toISOString() };
  }
  const summary = summarizePrivacyTokenEntry(entry);
  let status = "missing";
  if (summary.usable) status = "usable";
  else if (summary.hasToken && summary.expired) status = "expired";
  return {
    status,
    checkedAt: new Date().toISOString(),
    issuedAt: summary.timestamp
      ? new Date(summary.timestamp * 1000).toISOString()
      : null,
  };
}

async function identityDirectoryPayload() {
  const contacts = identityStore.directory();
  const lids = uniqueValues(contacts.map((contact) => contact.primaryLidJid));
  const tokenEntries = activeAuthState && lids.length
    ? await activeAuthState.keys.get("tctoken", lids)
    : {};
  return contacts.map((contact) => ({
    ...contact,
    token: tokenDirectoryStatus(
      contact.primaryLidJid ? tokenEntries?.[contact.primaryLidJid] : null,
      Boolean(contact.primaryLidJid),
    ),
  }));
}

async function respondWithGroupDirectory(sock, message) {
  try {
    const request = JSON.parse(sc.decode(message.data) || "{}");
    const groups = groupDiscovery
      ? await groupDiscovery.listParticipatingGroups(sock, {
          refresh: request.refresh !== false,
        })
      : Object.values(await sock.groupFetchAllParticipating()).map((group) => ({
          jid: group.id,
          name: group.subject || "",
          description: group.desc || "",
          ownerJid: group.owner || null,
          participantCount: Number(group.size ?? group.participants?.length ?? 0),
          createdAt: group.creation
            ? new Date(Number(group.creation) * 1000).toISOString()
            : null,
        }));
    message.respond(sc.encode(JSON.stringify({
      checkedAt: new Date().toISOString(),
      groups,
      total: groups.length,
      workerId: config.workerId,
    })));
  } catch (error) {
    message.respond(sc.encode(JSON.stringify({
      error: error.message,
      workerId: config.workerId,
    })));
  }
}

async function respondWithIdentityDirectory(sock, message) {
  let request = {};
  try {
    request = JSON.parse(sc.decode(message.data) || "{}");
    if (request.action === "refresh_token") {
      const lidJid = canonicalLidJid(request.lidJid || request.lid_jid);
      if (!lidJid) {
        throw new ValidationError("A valid LID JID is required");
      }
      const before = tokenDirectoryStatus(await getPrivacyTokenEntry(lidJid), true);
      const issuance = await issueAndStorePrivacyToken(sock, lidJid);
      const after = tokenDirectoryStatus(await getPrivacyTokenEntry(lidJid), true);
      message.respond(sc.encode(JSON.stringify({
        action: "refresh_token",
        lidJid,
        before,
        token: after,
        issued: Boolean(issuance.issued),
        checkedAt: new Date().toISOString(),
        workerId: config.workerId,
      })));
      return;
    }
    const contacts = await identityDirectoryPayload();
    message.respond(sc.encode(JSON.stringify({
      checkedAt: new Date().toISOString(),
      contacts,
      total: contacts.length,
      workerId: config.workerId,
    })));
  } catch (error) {
    message.respond(sc.encode(JSON.stringify({
      action: request.action || "list",
      error: error.message,
      workerId: config.workerId,
    })));
  }
}

async function sendJob(sock, job) {
  if (job.operation === "add_group_participants") {
    if (!config.allowDirectGroupAdds) {
      throw new ValidationError(
        "Direct group additions are disabled because WhatsApp invalidated linked " +
          "sessions after participant-add operations. Use a membership audit and invite link.",
      );
    }
    const results = await sock.groupParticipantsUpdate(
      job.target,
      job.participants,
      "add",
    );
    const failures = results.filter((result) => !["200", "409"].includes(String(result.status)));

    if (failures.length > 0) {
      const statuses = failures.map((result) => String(result.status));
      const permanent = statuses.every((status) => ["400", "401", "403", "404", "405", "406"].includes(status));
      if (permanent) {
        throw new ParticipantUpdateError(
          `WhatsApp rejected participant update with status: ${statuses.join(", ")}`,
          statuses,
          results,
        );
      }
      const error = new Error(
        `WhatsApp participant update temporarily failed with status: ${statuses.join(", ")}`,
      );
      error.statusCodes = statuses;
      error.operationResult = results;
      throw error;
    }

    return {
      identityResolution: {
        actualSendTarget: job.target,
        canonicalTarget: job.target,
        knownLidTarget: null,
        policy: config.directJidPolicy,
        reason: "group participant update",
        requestedTarget: job.target,
        returnedRemoteJids: [],
        usedLidAlias: false,
      },
      operationResult: results,
    };
  }

  const missingFiles = [];
  const imageExists = job.imagePath ? fs.existsSync(job.imagePath) : false;
  const excelExists = job.excelPath ? fs.existsSync(job.excelPath) : false;
  const existingDocuments = (job.documents || []).filter((document) =>
    fs.existsSync(document.path),
  );
  const hasAttachments = imageExists || excelExists || existingDocuments.length > 0;
  const prefersSeparateText = job.attachmentTextMode === "separate";
  const captionWouldOverflow =
    Boolean(job.text) && job.text.length > MAX_WHATSAPP_CAPTION_CHARS;
  const sendTextSeparately =
    Boolean(job.text) &&
    (!hasAttachments || prefersSeparateText || captionWouldOverflow);
  const identityResolution = resolveIdentityTarget(job);
  const sendTarget = identityResolution.actualSendTarget;
  let privacyTokenPreflight = null;
  try {
    privacyTokenPreflight = await ensurePrivacyTokenForDirectSend(
      sock,
      sendTarget,
    );
  } catch (error) {
    error.identityResolution = identityResolution;
    throw error;
  }
  const sendResults = [];

  if (sendTarget !== job.target) {
    log.info("Resolved WhatsApp send target through identity policy", {
      target: job.target,
      sendTarget,
      policy: identityResolution.policy,
      reason: identityResolution.reason,
      recipientName: job.recipientName,
    });
  }

  if (job.imagePath && !imageExists) {
    missingFiles.push(job.imagePath);
  }

  if (job.excelPath && !excelExists) {
    missingFiles.push(job.excelPath);
  }

  for (const document of job.documents || []) {
    if (!fs.existsSync(document.path)) {
      missingFiles.push(document.path);
    }
  }

  if (!job.text && !imageExists && !excelExists && existingDocuments.length === 0) {
    throw new ValidationError(
      `No deliverable content found for ${job.target}. Missing files: ${missingFiles.join(", ") || "n/a"}`,
    );
  }

  if (missingFiles.length > 0) {
    log.warn("Some referenced files were missing at send time", {
      target: job.target,
      missingFiles,
    });
  }

  if (captionWouldOverflow && !prefersSeparateText && hasAttachments) {
    log.warn("Falling back to separate text because caption would be truncated", {
      target: job.target,
      textLength: job.text.length,
      maxCaptionChars: MAX_WHATSAPP_CAPTION_CHARS,
    });
  }

  if (sendTextSeparately) {
    sendResults.push(...(await sendTextMessages(sock, sendTarget, job.text)));
  }

  if (imageExists) {
    const imageMessage = {
      image: { url: job.imagePath },
    };

    if (!sendTextSeparately && !excelExists && job.text) {
      imageMessage.caption = job.text;
    }

    sendResults.push(await sock.sendMessage(sendTarget, imageMessage));
  }

  if (excelExists) {
    sendResults.push(
      await sock.sendMessage(sendTarget, {
        document: { url: job.excelPath },
        mimetype:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        fileName: job.excelFilename || path.basename(job.excelPath),
        caption:
          !sendTextSeparately && existingDocuments.length === 0
            ? job.text || undefined
            : undefined,
      }),
    );
  }

  for (const [index, document] of existingDocuments.entries()) {
    sendResults.push(
      await sock.sendMessage(sendTarget, {
        document: { url: document.path },
        mimetype: document.mimetype,
        fileName: document.filename || path.basename(document.path),
        caption:
          document.caption ||
          (!sendTextSeparately &&
          !imageExists &&
          !excelExists &&
          index === 0
            ? job.text || undefined
            : undefined),
      }),
    );
  }

  if (!imageExists && !excelExists && existingDocuments.length === 0 && job.text && !sendTextSeparately) {
    sendResults.push(...(await sendTextMessages(sock, sendTarget, job.text)));
  }

  const summarizedResults = sendResults.map((result) => ({
    ...summarizeSendResult(result),
    actualSendTarget: sendTarget,
    knownLidTarget: identityResolution.knownLidTarget,
    requestedTarget: job.target,
    targetPolicy: identityResolution.policy,
  }));
  for (const result of summarizedResults) {
    if (privacyTokenPreflight) {
      result.privacyTokenPreflight = privacyTokenPreflight;
    }
  }
  try {
    const operationResult = await waitForMessageAcknowledgements(summarizedResults);
    observeIdentityFromOperationResult(
      job,
      identityResolution,
      operationResult,
      "delivery_remote_jid",
    );
    return { identityResolution, operationResult };
  } catch (error) {
    observeIdentityFromOperationResult(
      job,
      identityResolution,
      error.operationResult || summarizedResults,
      "delivery_remote_jid",
    );
    error.identityResolution = identityResolution;
    throw error;
  }
}

async function handleQueueMessage(msg, sock) {
  let payload;

  try {
    payload = resolveJobPayload(JSON.parse(sc.decode(msg.data)));
  } catch (error) {
    const reason =
      error instanceof ValidationError ? error.message : "Invalid JSON payload";

    await publishDeadLetter(reason, null, {
      raw_payload: sc.decode(msg.data),
      error: error.message,
      delivery_count: msg.info.deliveryCount,
    });

    msg.term(reason);
    log.error("Discarded invalid queue item", {
      reason,
      deliveryCount: msg.info.deliveryCount,
    });
    return;
  }

  const heartbeat = setInterval(() => {
    try {
      msg.working();
    } catch {
      // Ignore heartbeat failures while the message is already settling.
    }
  }, Math.max(1000, Math.floor(config.natsAckWaitMs / 2)));

  try {
    log.info("Processing queue item", {
      jobId: payload.jobId,
      target: payload.target,
      deliveryCount: msg.info.deliveryCount,
    });

    const sendOutcome = await sendJob(sock, payload);
    const operationResult = sendOutcome.operationResult;
    const identityResolution = sendOutcome.identityResolution;
    msg.ack();
    await appendDeliveryAudit(payload, "delivered", {
      deliveryCount: msg.info.deliveryCount,
      identityResolution,
      operationResult,
    });
    await publishDeliveryStatus(payload, "delivered", {
      deliveryCount: msg.info.deliveryCount,
      identityResolution,
      operationResult,
    });

    log.info("Message delivered", {
      actualSendTarget: identityResolution?.actualSendTarget,
      jobId: payload.jobId,
      target: payload.target,
      operation: payload.operation,
      targetPolicy: identityResolution?.policy,
    });

    if (payload.delayMs > 0) {
      await sleep(payload.delayMs);
    }
  } catch (error) {
    if (shouldAcceptPendingGroupAck(payload, error)) {
      const reason = `${error.message}. WhatsApp returned a message id, so the group message is marked sent pending final acknowledgement.`;
      msg.ack();
      await appendDeliveryAudit(payload, "sent_pending_confirmation", {
        deliveryCount: msg.info.deliveryCount,
        error: reason,
        identityResolution: error.identityResolution,
        operationResult: error.operationResult,
      });
      await publishDeliveryStatus(payload, "sent_pending_confirmation", {
        deliveryCount: msg.info.deliveryCount,
        error: reason,
        identityResolution: error.identityResolution,
        operationResult: error.operationResult,
      });
      log.warn("Group message sent but final acknowledgement is still pending", {
        jobId: payload.jobId,
        target: payload.target,
        reason,
      });
      if (payload.delayMs > 0) {
        await sleep(payload.delayMs);
      }
      return;
    }

    const isPermanent = error instanceof ValidationError;
    const exceededRetries = msg.info.deliveryCount >= config.natsMaxDeliver;

    if (isPermanent || exceededRetries) {
      const reason = isPermanent
        ? error.message
        : `Exceeded max delivery attempts (${config.natsMaxDeliver})`;

      await publishDeadLetter(reason, payload, {
        error: error.message,
        error_code: error.statusCodes?.join(","),
        identity_resolution: error.identityResolution,
        operation_result: error.operationResult,
        delivery_count: msg.info.deliveryCount,
      });

      msg.term(reason);
      await appendDeliveryAudit(payload, "failed", {
        deliveryCount: msg.info.deliveryCount,
        error: reason,
        errorCode: error.statusCodes?.join(","),
        identityResolution: error.identityResolution,
        operationResult: error.operationResult,
      });
      await publishDeliveryStatus(payload, "failed", {
        deliveryCount: msg.info.deliveryCount,
        error: reason,
        errorCode: error.statusCodes?.join(","),
        identityResolution: error.identityResolution,
        operationResult: error.operationResult,
      });
      log.error("Queue item moved to dead-letter flow", {
        jobId: payload.jobId,
        target: payload.target,
        reason,
      });
    } else {
      msg.nak(config.natsNakDelayMs);
      log.warn("Queue item will be retried", {
        jobId: payload.jobId,
        target: payload.target,
        deliveryCount: msg.info.deliveryCount,
        error: error.message,
      });
    }
  } finally {
    clearInterval(heartbeat);
  }
}

async function startNatsConsumer(sock) {
  if (
    shuttingDown ||
    natsStartInProgress ||
    activeConsumerMessages ||
    activeSocket !== sock ||
    !isWhatsAppConnected
  ) {
    return;
  }

  natsStartInProgress = true;
  let nc;
  let iter;

  try {
    nc = await connect({ servers: config.natsUrl });
    nc.subscribe(config.groupMembersSubject, {
      callback: (_error, message) => {
        void respondWithGroupMembers(sock, message);
      },
    });
    nc.subscribe(config.groupDirectorySubject, {
      callback: (_error, message) => {
        void respondWithGroupDirectory(sock, message);
      },
    });
    nc.subscribe(config.identityDirectorySubject, {
      callback: (_error, message) => {
        void respondWithIdentityDirectory(sock, message);
      },
    });
    const jsm = await nc.jetstreamManager();

    const jetStreamResources = await ensureJetStreamWorkerResources(jsm, {
      ackPolicy: AckPolicy.Explicit,
      ackWaitNanos: nanos(config.natsAckWaitMs),
      consumerName: config.consumerName,
      deadLetterMaxMessages: config.deadLetterMaxMessages,
      deadLetterStream: config.deadLetterStream,
      deadLetterSubject: config.deadLetterSubject,
      logger: log,
      maxDeliver: config.natsMaxDeliver,
      streamName: config.natsStream,
      subject: config.natsSubject,
    });
    const queueStreamName = jetStreamResources.streamName;

    if (shuttingDown || activeSocket !== sock || !isWhatsAppConnected) {
      await nc.close();
      return;
    }

    const js = nc.jetstream();
    const consumer = await js.consumers.get(
      queueStreamName,
      config.consumerName,
    );

    iter = await consumer.consume();
    activeNatsConnection = nc;
    activeJetStream = js;
    activeConsumerMessages = iter;

    log.info("NATS consumer is listening", {
      healthSubject: config.healthSubject,
      groupDirectorySubject: config.groupDirectorySubject,
      groupMembersSubject: config.groupMembersSubject,
      identityDirectorySubject: config.identityDirectorySubject,
      subject: config.natsSubject,
      stream: queueStreamName,
      consumer: config.consumerName,
    });

    for await (const msg of iter) {
      if (shuttingDown || activeSocket !== sock || !isWhatsAppConnected) {
        break;
      }

      await handleQueueMessage(msg, sock);
    }
  } catch (error) {
    log.error("NATS consumer failed", {
      error: error.message,
      natsUrl: config.natsUrl,
    });
  } finally {
    natsStartInProgress = false;

    if (activeConsumerMessages === iter) {
      activeConsumerMessages = null;
    }

    if (activeNatsConnection === nc) {
      activeNatsConnection = null;
      activeJetStream = null;
    }

    if (nc) {
      try {
        await nc.close();
      } catch {
        // Ignore close failures during shutdown/retry.
      }
    }

    if (!shuttingDown && activeSocket === sock && isWhatsAppConnected) {
      log.warn("NATS consumer stopped unexpectedly; retrying", {
        delayMs: config.reconnectDelayMs,
      });
      setTimeout(() => {
        startNatsConsumer(sock).catch((retryError) => {
          log.error("NATS retry failed", { error: retryError.message });
        });
      }, config.reconnectDelayMs);
    }
  }
}

async function startWorker() {
  if (workerStartInProgress || shuttingDown) {
    return;
  }

  workerStartInProgress = true;

  try {
    await flushCredentialSaves();
    await stopNatsConsumer();
    await stopSocket();
    await startHealthResponder();
    initializeIdentityPolicy();

    log.info("Starting WhatsApp worker", {
      authDir: config.authDir,
      natsUrl: config.natsUrl,
    });

    const { state, saveCreds } = await useMultiFileAuthState(config.authDir);
    activeAuthState = state;
    const latestVersion = await fetchLatestBaileysVersion().catch(() => null);

    const sock = makeWASocket({
      auth: state,
      logger: pino({ level: config.logLevel === "debug" ? "debug" : "silent" }),
      version: latestVersion?.version,
      browser: configuredBrowserIdentity(),
    });

    activeSocket = sock;
    lastConnectionStatus = "connecting";
    lastDisconnectInfo = null;
    scheduleConnectionOpenTimeout(sock);

    groupDiscovery?.reset();

    sock.ev.on("creds.update", () => {
      void queueCredentialSave(saveCreds);
    });

    sock.ev.on("messages.upsert", ({ messages }) => {
      if (groupDiscovery) {
        groupDiscovery.handleMessagesUpsert(sock, { messages });
      }
    });

    sock.ev.on("messages.update", handleMessageUpdates);

    sock.ev.on("groups.upsert", (groups) => {
      groupDiscovery?.handleGroupsUpsert(groups);
    });

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (activeSocket !== sock) {
        log.debug("Ignoring event from a stale WhatsApp socket", { connection });
        return;
      }

      if (qr) {
        try {
          await writeLoginQrImage(qr);
        } catch (error) {
          log.error("Failed to create WhatsApp login QR image", {
            error: error.message,
            qrImagePath: config.qrImagePath,
          });
        }
      }

      if (connection) {
        lastConnectionStatus = connection;
      }

      if (connection === "open") {
        clearConnectionOpenTimer();
        isWhatsAppConnected = true;
        lastDisconnectInfo = null;
        await flushCredentialSaves();
        removeLoginQrImage();
        log.info("WhatsApp connected; authentication state persisted");

        await sleep(config.startupDelayMs);
        await groupDiscovery?.syncParticipatingGroups(sock);
        await startNatsConsumer(sock);
        return;
      }

      if (connection !== "close") {
        return;
      }

      clearConnectionOpenTimer();
      isWhatsAppConnected = false;
      await stopNatsConsumer();

      const statusCode =
        lastDisconnect?.error?.output?.statusCode ||
        lastDisconnect?.error?.statusCode;
      const disconnectMessage = String(lastDisconnect?.error?.message || "");
      const isConnectionConflict = /conflict|connection replaced/i.test(
        disconnectMessage,
      );

      log.warn("WhatsApp connection closed", {
        statusCode,
        error: lastDisconnect?.error?.message,
      });
      lastDisconnectInfo = {
        statusCode: statusCode || null,
        error: lastDisconnect?.error?.message || null,
        closedAt: new Date().toISOString(),
      };

      if (activeSocket === sock) {
        activeSocket = null;
      }

      if (isConnectionConflict) {
        log.error(
          "WhatsApp connection conflict detected; credentials were preserved. " +
            "Automatic reconnect is disabled for conflicts. Stop any competing " +
            "worker and restart this service once.",
          { statusCode },
        );
        await shutdown("WhatsApp connection conflict", 1);
        return;
      }

      if (isFatalDisconnect(statusCode)) {
        log.error(
          "WhatsApp rejected the preserved session. Authentication was not modified. " +
            "Use the GUI's explicit 'Reset Login & Create QR' action only if re-linking is required.",
          {
          statusCode,
          authDir: config.authDir,
          credentialsPreserved: true,
          },
        );
        await shutdown("WhatsApp authentication rejected", 1);
        return;
      }

      scheduleReconnect(`connection closed (${statusCode || "unknown"})`);
    });
  } finally {
    workerStartInProgress = false;
  }
}

async function shutdown(signal, exitCode = 0) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  isWhatsAppConnected = false;

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  log.info("Shutting down worker", { signal });

  await stopNatsConsumer();
  await stopHealthResponder();
  await stopSocket();
  await flushCredentialSaves();
  releaseWorkerLock();

  process.exit(exitCode);
}

process.on("SIGINT", () => {
  shutdown("SIGINT").catch((error) => {
    log.error("Shutdown failed", { error: error.message });
    process.exit(1);
  });
});

process.on("SIGTERM", () => {
  shutdown("SIGTERM").catch((error) => {
    log.error("Shutdown failed", { error: error.message });
    process.exit(1);
  });
});

process.on("exit", releaseWorkerLock);

startWorker().catch((error) => {
  log.error("Worker boot failed", { error: error.message });
  scheduleReconnect("initial boot failure");
});
