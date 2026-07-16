import { formatError } from "./errors.js";
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
  } catch (error) {
    throw new Error(formatError(error, "request_json"));
  }
}

function responseItem(item) {
  return {
    accepted: item.status !== "failed",
    ...item,
  };
}

function localAuthHistoryBlocked(config) {
  return config.mode === "local_auth" && config.allowLocalAuthHistory !== true;
}

function localAuthInstruction(config) {
  return `Historical retrieval is blocked in WWEBJS_MODE=local_auth because that is a separate linked-device browser session. `
    + `Use the Brave profile where the files are visibly available: close Brave, run whatsapp-web-history-bridge/scripts/launch-brave-debug.sh, `
    + `set WWEBJS_MODE=browser_url and WWEBJS_BROWSER_URL=${config.browserUrl || "http://127.0.0.1:9222"}, then restart dev.sh. `
    + `The LocalAuth data and Baileys implementation remain preserved.`;
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
        store.update(item.requestId, { status: "timed_out", error: "request_timeout: WhatsApp Web history request exceeded its time limit" });
      } catch {}
    }, config.requestTimeoutMs);
    timeout.unref?.();

    try {
      store.update(item.requestId, { status: "syncing", error: null });
      const resolved = await resolveDirectChat(client, request.remoteJids || []);
      store.update(item.requestId, { diagnostics: { resolution: resolved.diagnostics } });
      const loaded = await fetchOlderMessages(client, resolved.chat, {
        beforeTimestamp: request.beforeTimestamp || null,
        anchorMessageId: request.anchorMessageId || null,
        count: item.requestedCount,
        maxScan: config.maxMessagesScanned,
        attemptTimeoutMs: config.fetchAttemptTimeoutMs || 120000,
        syncHistory: config.syncHistory !== false,
        syncHistoryTimeoutMs: config.syncHistoryTimeoutMs || 30000,
        syncSettleMs: config.syncSettleMs ?? 3500,
      });
      const messages = loaded.messages;
      store.update(item.requestId, {
        diagnostics: { resolution: resolved.diagnostics, loading: loaded.diagnostics },
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
          mediaError = formatError(error, "media_download");
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
            mediaError = formatError(error, "media_archive");
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
      log.info({
        requestId: item.requestId,
        messages: messages.length,
        attachmentsDiscovered,
        attachmentsArchived,
        diagnostics: loaded.diagnostics,
      }, "WhatsApp Web history request completed");
    } catch (error) {
      const detail = formatError(error, error?.phase || "history_request");
      if (store.status(item.requestId).status !== "timed_out") {
        store.update(item.requestId, { status: "failed", error: detail });
      }
      log.error({ requestId: item.requestId, error: detail, stack: error?.stack || null }, "WhatsApp Web history request failed");
    } finally {
      clearTimeout(timeout);
    }
  }

  async function requestHistory(request) {
    if (request.workerId && request.workerId !== config.workerId) {
      throw new Error(`Request targets worker ${request.workerId}, not ${config.workerId}`);
    }
    if (localAuthHistoryBlocked(config)) throw new Error(localAuthInstruction(config));
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
    const blocked = localAuthHistoryBlocked(config);
    return {
      accepted: true,
      provider: "wwebjs",
      protocolVersion: config.protocolVersion,
      workerId: config.workerId,
      mode: config.mode,
      browserUrl: config.mode === "browser_url" ? config.browserUrl : null,
      status: runtime.status,
      ready: runtime.status === "ready",
      historyReady: runtime.status === "ready" && !blocked,
      authenticated: Boolean(runtime.authenticated),
      error: runtime.error || (blocked ? localAuthInstruction(config) : null),
      warning: blocked ? localAuthInstruction(config) : null,
      qrPath: runtime.status === "qr" ? config.qrPath : null,
      webVersion: runtime.webVersion || null,
      browserEndpoint: runtime.browserEndpoint || null,
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
        error: formatError(error, "bridge_response"),
      })));
    }
  }

  return { respond, requestHistory, downloadAttachment, health, processRequest };
}
