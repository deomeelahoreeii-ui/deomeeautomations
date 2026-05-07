import fs from "fs";
import path from "path";
import {
  connect,
  StringCodec,
  AckPolicy,
  nanos,
} from "nats";
import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  useMultiFileAuthState,
} from "@whiskeysockets/baileys";
import pino from "pino";
import qrcode from "qrcode-terminal";

import { config, ensureRuntimeDirectories, sleep } from "./lib/config.js";
import { createGroupDiscovery } from "./lib/group-discovery.js";
import { resolveJobPayload, ValidationError } from "./lib/jobs.js";
import { ensureJetStreamWorkerResources } from "./lib/nats-setup.js";
import { createLogger } from "./lib/logger.js";

const log = createLogger("worker");
const sc = StringCodec();
const groupDiscovery = config.enableGroupDiscovery
  ? createGroupDiscovery({ config, log })
  : null;
const MAX_WHATSAPP_CAPTION_CHARS = 900;
const MAX_WHATSAPP_TEXT_CHARS = 3500;

let activeSocket = null;
let activeNatsConnection = null;
let activeJetStream = null;
let activeConsumerMessages = null;
let isWhatsAppConnected = false;
let natsStartInProgress = false;
let workerStartInProgress = false;
let reconnectTimer = null;
let shuttingDown = false;

ensureRuntimeDirectories();

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
  for (const chunk of splitTextIntoChunks(text)) {
    await sock.sendMessage(target, { text: chunk });
  }
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

async function stopSocket() {
  if (!activeSocket) {
    return;
  }

  const socket = activeSocket;
  activeSocket = null;

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

async function sendJob(sock, job) {
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
    await sendTextMessages(sock, job.target, job.text);
  }

  if (imageExists) {
    const imageMessage = {
      image: { url: job.imagePath },
    };

    if (!sendTextSeparately && !excelExists && job.text) {
      imageMessage.caption = job.text;
    }

    await sock.sendMessage(job.target, imageMessage);
  }

  if (excelExists) {
    await sock.sendMessage(job.target, {
      document: { url: job.excelPath },
      mimetype:
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      fileName: job.excelFilename || path.basename(job.excelPath),
      caption:
        !sendTextSeparately && existingDocuments.length === 0
          ? job.text || undefined
          : undefined,
    });
  }

  for (const [index, document] of existingDocuments.entries()) {
    await sock.sendMessage(job.target, {
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
    });
  }

  if (!imageExists && !excelExists && existingDocuments.length === 0 && job.text && !sendTextSeparately) {
    await sendTextMessages(sock, job.target, job.text);
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

    await sendJob(sock, payload);
    msg.ack();

    log.info("Message delivered", {
      jobId: payload.jobId,
      target: payload.target,
    });

    if (payload.delayMs > 0) {
      await sleep(payload.delayMs);
    }
  } catch (error) {
    const isPermanent = error instanceof ValidationError;
    const exceededRetries = msg.info.deliveryCount >= config.natsMaxDeliver;

    if (isPermanent || exceededRetries) {
      const reason = isPermanent
        ? error.message
        : `Exceeded max delivery attempts (${config.natsMaxDeliver})`;

      await publishDeadLetter(reason, payload, {
        error: error.message,
        delivery_count: msg.info.deliveryCount,
      });

      msg.term(reason);
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
    await stopNatsConsumer();

    log.info("Starting WhatsApp worker", {
      authDir: config.authDir,
      natsUrl: config.natsUrl,
    });

    const { state, saveCreds } = await useMultiFileAuthState(config.authDir);
    const latestVersion = await fetchLatestBaileysVersion().catch(() => null);

    const sock = makeWASocket({
      auth: state,
      logger: pino({ level: config.logLevel === "debug" ? "debug" : "silent" }),
      version: latestVersion?.version,
      browser: [
        config.browserPlatform,
        config.browserName,
        config.browserVersion,
      ],
    });

    activeSocket = sock;

    groupDiscovery?.reset();

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("messages.upsert", ({ messages }) => {
      if (groupDiscovery) {
        groupDiscovery.handleMessagesUpsert(sock, { messages });
      }
    });

    sock.ev.on("connection.update", async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        log.info("Scan the QR code with WhatsApp to authenticate");
        qrcode.generate(qr, { small: true });
      }

      if (connection === "open") {
        isWhatsAppConnected = true;
        log.info("WhatsApp connected");

        await sleep(config.startupDelayMs);
        await startNatsConsumer(sock);
        return;
      }

      if (connection !== "close") {
        return;
      }

      isWhatsAppConnected = false;
      await stopNatsConsumer();

      const statusCode =
        lastDisconnect?.error?.output?.statusCode ||
        lastDisconnect?.error?.statusCode;

      log.warn("WhatsApp connection closed", {
        statusCode,
        error: lastDisconnect?.error?.message,
      });

      if (activeSocket === sock) {
        activeSocket = null;
      }

      if (isFatalDisconnect(statusCode)) {
        log.error("WhatsApp session needs attention before reconnecting", {
          statusCode,
          authDir: config.authDir,
        });
        return;
      }

      scheduleReconnect(`connection closed (${statusCode || "unknown"})`);
    });
  } finally {
    workerStartInProgress = false;
  }
}

async function shutdown(signal) {
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
  await stopSocket();

  process.exit(0);
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

startWorker().catch((error) => {
  log.error("Worker boot failed", { error: error.message });
  scheduleReconnect("initial boot failure");
});
