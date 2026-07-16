import { resolveDirectChat } from "./jid.js";
import {
  fetchOlderMessages,
  isSupportedMedia,
  mediaToBuffer,
  normalizeMessageEvent,
  rawMediaMetadata,
} from "./history-loader.js";

function parseRequest(sc, message) {
  try {
    return JSON.parse(sc.decode(message.data) || "{}");
  } catch {
    throw new Error("WhatsApp Web bridge request is not valid JSON");
  }
}

function responseItem(item) {
  return {
    accepted: item.status !== "failed",
    ...item,
  };
}

export function createBridgeService({ config, client, store, platform, log, sc, runtime }) {
  async function prepareMedia(message) {
    const metadata = rawMediaMetadata(message);
    if (!message?.hasMedia || !isSupportedMedia(metadata)) return { metadata: null, buffer: null };
    const media = await message.downloadMedia();
    if (!media) return { metadata, buffer: null };
    const normalized = {
      mimetype: media.mimetype || metadata.mimetype,
      filename: media.filename || metadata.filename,
      filesize: Number(media.filesize || metadata.filesize || 0) || null,
      type: metadata.type,
    };
    if (!isSupportedMedia(normalized)) return { metadata: null, buffer: null };
    return { metadata: normalized, buffer: mediaToBuffer(media, config.mediaMaxBytes) };
  }

  async function processRequest(request, item) {
    const timeout = setTimeout(() => {
      try {
        store.update(item.requestId, { status: "timed_out", error: "WhatsApp Web history request exceeded its time limit" });
      } catch {}
    }, config.requestTimeoutMs);
    timeout.unref?.();

    try {
      store.update(item.requestId, { status: "syncing" });
      const chat = await resolveDirectChat(client, request.remoteJids || []);
      const messages = await fetchOlderMessages(chat, {
        beforeTimestamp: request.beforeTimestamp || null,
        anchorMessageId: request.anchorMessageId || null,
        count: item.requestedCount,
        maxScan: config.maxMessagesScanned,
      });
      let messagesReceived = 0;
      let attachmentsDiscovered = 0;
      let attachmentsArchived = 0;

      for (const message of messages) {
        if (store.status(item.requestId).status === "timed_out") break;
        let prepared = { metadata: null, buffer: null };
        let mediaError = null;
        try {
          prepared = await prepareMedia(message);
        } catch (error) {
          mediaError = error.message;
          const metadata = rawMediaMetadata(message);
          if (isSupportedMedia(metadata)) prepared = { metadata, buffer: null };
        }
        const event = normalizeMessageEvent(message, {
          workerId: config.workerId,
          platformRemoteJid: request.platformRemoteJid || request.remoteJids?.[0],
          media: prepared.metadata,
        });
        const ingested = await platform.ingest(event);
        if (store.status(item.requestId).status === "timed_out") break;
        messagesReceived += 1;
        if (event.attachment) attachmentsDiscovered += 1;
        if (event.attachment && prepared.buffer && ingested.attachment_id) {
          try {
            await platform.uploadAttachment(ingested.attachment_id, prepared.buffer, prepared.metadata?.mimetype);
            if (store.status(item.requestId).status === "timed_out") break;
            attachmentsArchived += 1;
          } catch (error) {
            mediaError = error.message;
            log.warn({ requestId: item.requestId, messageId: event.messageId, error: mediaError }, "Historical media metadata was ingested but immediate archive failed");
          }
        }
        store.update(item.requestId, {
          status: "syncing",
          messagesReceived,
          attachmentsDiscovered,
          attachmentsArchived,
          error: mediaError,
        });
      }
      if (store.status(item.requestId).status !== "timed_out") {
        store.update(item.requestId, {
          status: messages.length ? "succeeded" : "no_results",
          messagesReceived,
          attachmentsDiscovered,
          attachmentsArchived,
          error: null,
        });
      }
      log.info({ requestId: item.requestId, messages: messages.length, attachmentsDiscovered, attachmentsArchived }, "WhatsApp Web history request completed");
    } catch (error) {
      if (store.status(item.requestId).status !== "timed_out") {
        store.update(item.requestId, { status: "failed", error: error.message });
      }
      log.error({ requestId: item.requestId, error: error.message }, "WhatsApp Web history request failed");
    } finally {
      clearTimeout(timeout);
    }
  }

  async function requestHistory(request) {
    if (request.workerId && request.workerId !== config.workerId) {
      throw new Error(`Request targets worker ${request.workerId}, not ${config.workerId}`);
    }
    if (runtime.status !== "ready") {
      const detail = runtime.status === "qr"
        ? `WhatsApp Web bridge needs pairing; scan ${config.qrPath}`
        : `WhatsApp Web bridge is not ready (${runtime.status})${runtime.error ? `: ${runtime.error}` : ""}`;
      throw new Error(detail);
    }
    const count = Math.max(1, Math.min(Number.parseInt(request.count, 10) || 50, config.maxHistoryCount));
    const requestId = String(request.requestId || "").trim();
    if (!requestId) throw new Error("requestId is required");
    const remoteJids = [...new Set((request.remoteJids || []).map((value) => String(value || "").trim()).filter(Boolean))];
    if (!remoteJids.length) throw new Error("At least one contact JID is required");
    const item = store.create({
      requestId,
      workerId: config.workerId,
      requestedCount: count,
      remoteJid: request.platformRemoteJid || remoteJids[0],
      anchorMessageId: request.anchorMessageId || null,
      anchorTimestamp: request.beforeTimestamp || null,
    });
    setImmediate(() => void processRequest({ ...request, remoteJids }, item));
    return responseItem({ ...item, operationId: `wwebjs:${requestId}` });
  }

  async function downloadAttachment(request) {
    if (runtime.status !== "ready") throw new Error(`WhatsApp Web bridge is not ready (${runtime.status})`);
    const attachmentId = String(request.attachmentId || "").trim();
    const messageId = String(request.messageId || "").trim();
    if (!attachmentId || !messageId) throw new Error("attachmentId and messageId are required");
    const message = await client.getMessageById(messageId);
    if (!message) throw new Error("WhatsApp Web could not reload the requested historical message");
    const media = await message.downloadMedia();
    const buffer = mediaToBuffer(media, config.mediaMaxBytes);
    const uploaded = await platform.uploadAttachment(attachmentId, buffer, media?.mimetype || request.declaredMimeType || null);
    return { uploaded: true, workerId: config.workerId, provider: "wwebjs", ...uploaded };
  }

  function health() {
    return {
      accepted: true,
      provider: "wwebjs",
      protocolVersion: config.protocolVersion,
      workerId: config.workerId,
      mode: config.mode,
      status: runtime.status,
      ready: runtime.status === "ready",
      authenticated: Boolean(runtime.authenticated),
      error: runtime.error || null,
      qrPath: runtime.status === "qr" ? config.qrPath : null,
      webVersion: runtime.webVersion || null,
      activeRequest: store.active()?.requestId || null,
    };
  }

  async function respond(message) {
    let request = {};
    try {
      request = parseRequest(sc, message);
      let result;
      if (request.action === "request_history") result = await requestHistory(request);
      else if (request.action === "history_status") result = responseItem(store.status(String(request.requestId || "")));
      else if (request.action === "bridge_health") result = health();
      else if (request.action === "download_attachment") result = await downloadAttachment(request);
      else throw new Error(`Unsupported WhatsApp Web bridge action: ${request.action || "missing"}`);
      message.respond(sc.encode(JSON.stringify(result)));
    } catch (error) {
      message.respond(sc.encode(JSON.stringify({
        accepted: false,
        provider: "wwebjs",
        workerId: config.workerId,
        requestId: request.requestId || null,
        status: "failed",
        active: false,
        error: error.message,
      })));
    }
  }

  return { respond, requestHistory, downloadAttachment, health, processRequest };
}
