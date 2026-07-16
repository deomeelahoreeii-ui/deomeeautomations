import { randomUUID } from "node:crypto";

function parseRequest(sc, message) {
  try {
    return JSON.parse(sc.decode(message.data) || "{}");
  } catch {
    throw new Error("Inbound history request is not valid JSON");
  }
}

export function createInboundHistoryResponder({ config, store, log, sc }) {
  async function requestHistory(sock, request) {
    if (request.workerId && request.workerId !== config.workerId) {
      throw new Error(`Request targets worker ${request.workerId}, not ${config.workerId}`);
    }
    const count = Math.max(1, Math.min(Number.parseInt(request.count, 10) || 50, config.inboundHistoryMaxCount));
    const remoteJids = Array.isArray(request.remoteJids) ? request.remoteJids : [];
    const anchor = store.oldestBoundary(remoteJids);
    if (!anchor) {
      throw new Error("No captured message is available to anchor an older-history request for this contact");
    }
    const anchorTimestampMs = Date.parse(anchor.message_timestamp);
    if (!Number.isFinite(anchorTimestampMs)) {
      throw new Error("The oldest captured message has an invalid timestamp");
    }
    const requestId = String(request.requestId || randomUUID());
    let operationId;
    store.recordHistoryRequest({
      requestId,
      remoteJid: anchor.remote_jid,
      count,
      anchorMessageId: anchor.message_id,
      anchorTimestamp: anchor.message_timestamp,
      operationId: null,
    });
    try {
      operationId = await sock.fetchMessageHistory(
        count,
        {
          remoteJid: anchor.remote_jid,
          id: anchor.message_id,
          fromMe: Boolean(anchor.from_me),
        },
        anchorTimestampMs,
      );
      store.recordHistoryRequest({
        requestId,
        remoteJid: anchor.remote_jid,
        count,
        anchorMessageId: anchor.message_id,
        anchorTimestamp: anchor.message_timestamp,
        operationId,
      });
    } catch (error) {
      store.markHistoryRequestFailed(requestId, error.message);
      throw error;
    }
    log.info("Requested older WhatsApp history", {
      requestId,
      operationId,
      remoteJid: anchor.remote_jid,
      count,
      anchorMessageId: anchor.message_id,
      anchorTimestamp: anchor.message_timestamp,
    });
    return {
      accepted: true,
      requestId,
      operationId,
      workerId: config.workerId,
      remoteJid: anchor.remote_jid,
      requestedCount: count,
      anchorMessageId: anchor.message_id,
      anchorTimestamp: anchor.message_timestamp,
      note: "WhatsApp will deliver available older messages asynchronously. Rescan after a few seconds.",
    };
  }

  async function respond(sock, message) {
    let request = {};
    try {
      request = parseRequest(sc, message);
      if (request.action !== "request_history") {
        throw new Error(`Unsupported inbound history action: ${request.action || "missing"}`);
      }
      const result = await requestHistory(sock, request);
      message.respond(sc.encode(JSON.stringify(result)));
    } catch (error) {
      log.warn("Inbound history request failed", { error: error.message });
      message.respond(sc.encode(JSON.stringify({
        accepted: false,
        workerId: config.workerId,
        error: error.message,
      })));
    }
  }

  return { respond, requestHistory };
}
