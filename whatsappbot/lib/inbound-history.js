import { randomUUID } from "node:crypto";

const ACTIVE_HISTORY_STATUSES = new Set(["requested", "accepted", "syncing"]);

function parseRequest(sc, message) {
  try {
    return JSON.parse(sc.decode(message.data) || "{}");
  } catch {
    throw new Error("Inbound history request is not valid JSON");
  }
}

function uniqueJids(values) {
  return [...new Set((values || []).map((value) => String(value || "").trim()).filter(Boolean))];
}

function messageJids(messages) {
  return uniqueJids(
    (messages || []).flatMap((item) => [
      item?.key?.remoteJid,
      item?.key?.remoteJidAlt,
    ]),
  );
}

function messageTimestampMs(message) {
  const value = message?.messageTimestamp;
  if (value == null) return null;
  if (typeof value === "bigint") return Number(value) * 1000;
  if (typeof value?.toNumber === "function") return value.toNumber() * 1000;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed * 1000 : null;
}

function matchingHistoricalMessages(messages, active, { allowMissingTimestamp = false } = {}) {
  if (!active) return [];
  const remoteJid = String(active.remote_jid || "").trim();
  const anchorMs = Date.parse(active.anchor_timestamp || "");
  return (messages || []).filter((message) => {
    const jids = messageJids([message]);
    if (!remoteJid || !jids.includes(remoteJid)) return false;
    const timestampMs = messageTimestampMs(message);
    if (timestampMs == null) return allowMissingTimestamp;
    if (!Number.isFinite(anchorMs)) return true;
    return timestampMs <= anchorMs + 1000;
  });
}

export function inboundHistorySocketOptions(config) {
  return {
    syncFullHistory: Boolean(config.inboundSyncFullHistory),
    shouldSyncHistoryMessage: () => Boolean(config.enableInboundCapture),
  };
}

export function createInboundHistoryResponder({ config, store, log, sc }) {
  const lifecycle = {
    quietMs: config.inboundHistoryQuietMs || 8000,
    noResultMs: config.inboundHistoryNoResultMs || 45000,
    hardTimeoutMs: config.inboundHistoryHardTimeoutMs || 180000,
  };

  function workerStatus(requestId) {
    const item = store.reconcileHistoryRequest?.(requestId, lifecycle) || store.getHistoryRequest?.(requestId);
    if (!item) {
      throw new Error(`History request ${requestId} was not found on worker ${config.workerId}`);
    }
    return {
      accepted: item.status !== "failed",
      workerId: config.workerId,
      requestId: item.request_id,
      status: item.status,
      active: ACTIVE_HISTORY_STATUSES.has(item.status),
      error: item.last_error || null,
      updatedAt: item.updated_at || null,
    };
  }

  async function requestHistory(sock, request) {
    if (request.workerId && request.workerId !== config.workerId) {
      throw new Error(`Request targets worker ${request.workerId}, not ${config.workerId}`);
    }
    store.reconcileHistoryRequests?.(lifecycle);
    const active = store.activeHistoryRequest?.(lifecycle);
    if (active) {
      throw new Error(`Another history request is still active (${active.request_id})`);
    }

    const requestedCount = Number.parseInt(request.count, 10) || 50;
    if (requestedCount < 1 || requestedCount > config.inboundHistoryMaxCount) {
      throw new Error(`History count must be between 1 and ${config.inboundHistoryMaxCount}`);
    }
    const count = request.allHistory ? config.inboundHistoryMaxCount : requestedCount;
    const remoteJids = uniqueJids(request.remoteJids);
    const anchor = store.oldestBoundary(remoteJids);
    if (!anchor) {
      throw new Error("No captured message is available to anchor an older-history request for this contact");
    }
    const anchorTimestampMs = Date.parse(anchor.message_timestamp);
    if (!Number.isFinite(anchorTimestampMs)) {
      throw new Error("The oldest captured message has an invalid timestamp");
    }

    const requestId = String(request.requestId || randomUUID());
    store.recordHistoryRequest({
      requestId,
      remoteJid: anchor.remote_jid,
      count,
      anchorMessageId: anchor.message_id,
      anchorTimestamp: anchor.message_timestamp,
      operationId: null,
    });
    let operationId;
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
      fullHistoryEnabled: Boolean(config.inboundSyncFullHistory),
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
      status: "accepted",
      note: "WhatsApp history delivery is asynchronous and may be unavailable for an existing linked session.",
    };
  }

  function observeMessages(messages, { allowMissingTimestamp = false } = {}) {
    store.reconcileHistoryRequests?.(lifecycle);
    const active = store.activeHistoryRequest?.(lifecycle);
    const matched = matchingHistoricalMessages(messages, active, { allowMissingTimestamp });
    if (!matched.length) return false;
    const jids = messageJids(matched);
    const updated = store.markActiveHistoryForJids?.(jids, { complete: false });
    if (updated) {
      log.info("Observed WhatsApp history messages", {
        requestId: updated.request_id,
        remoteJid: updated.remote_jid,
        messages: matched.length,
      });
    }
    return Boolean(updated);
  }

  function handleMessagesUpsert(event) {
    if (!["append", "notify"].includes(event?.type)) return false;
    return observeMessages(event.messages || [], {
      allowMissingTimestamp: event.type === "append",
    });
  }

  function handleMessagingHistorySet(event) {
    const messages = event?.messages || [];
    if (!messages.length) return false;
    return observeMessages(messages, { allowMissingTimestamp: true });
  }

  async function respond(sock, message) {
    let request = {};
    try {
      request = parseRequest(sc, message);
      let result;
      if (request.action === "request_history") {
        result = await requestHistory(sock, request);
      } else if (request.action === "history_status") {
        if (request.workerId && request.workerId !== config.workerId) {
          throw new Error(`Request targets worker ${request.workerId}, not ${config.workerId}`);
        }
        result = workerStatus(String(request.requestId || ""));
      } else {
        throw new Error(`Unsupported inbound history action: ${request.action || "missing"}`);
      }
      message.respond(sc.encode(JSON.stringify(result)));
    } catch (error) {
      log.warn("Inbound history request failed", { action: request.action, error: error.message });
      message.respond(sc.encode(JSON.stringify({
        accepted: false,
        workerId: config.workerId,
        requestId: request.requestId || null,
        status: "failed",
        active: false,
        error: error.message,
      })));
    }
  }

  return {
    respond,
    requestHistory,
    workerStatus,
    handleMessagesUpsert,
    handleMessagingHistorySet,
  };
}
