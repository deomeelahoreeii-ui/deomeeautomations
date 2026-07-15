import { normalizeInboundBatch } from "./inbound-normalizer.js";

export function createInboundCapture({ config, store, log }) {
  let flushing = false;
  let timer = null;

  function capture(messages, source) {
    let stored = 0;
    for (const item of normalizeInboundBatch(messages, { source })) {
      try { store.upsert(item); stored += 1; }
      catch (error) { log.error("Failed to store inbound WhatsApp message", { error: error.message, messageId: item.messageId, remoteJid: item.remoteJid }); }
    }
    if (stored) log.info("Captured inbound WhatsApp messages", { source, stored });
    return stored;
  }

  async function flushOutbox() {
    if (flushing || !config.inboundPlatformUrl || !config.inboundPlatformToken) return;
    flushing = true;
    try {
      for (const row of store.pendingOutbox(config.inboundOutboxBatchSize)) {
        try {
          const response = await fetch(`${config.inboundPlatformUrl.replace(/\/$/, "")}/api/v1/whatsapp/inbound/events`, {
            method: "POST",
            headers: { "content-type": "application/json", "x-whatsapp-worker-token": config.inboundPlatformToken },
            body: row.payload_json,
            signal: AbortSignal.timeout(config.inboundPlatformTimeoutMs),
          });
          if (!response.ok) throw new Error(`platform returned HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
          store.markDelivered(row.id);
        } catch (error) { store.markFailed(row.id, error.message); break; }
      }
    } finally { flushing = false; }
  }

  function start() {
    if (!config.enableInboundCapture || timer) return;
    timer = setInterval(() => void flushOutbox(), config.inboundOutboxFlushMs);
    timer.unref?.();
    void flushOutbox();
  }

  function stop() { if (timer) clearInterval(timer); timer = null; }

  return {
    handleMessagesUpsert(event) { return capture(event?.messages || [], event?.type === "append" ? "offline_sync" : "live"); },
    handleMessagingHistorySet(event) { return capture(event?.messages || [], "history_sync"); },
    getMessage(key) { return store.getMessage(String(key?.remoteJid || ""), String(key?.id || "")); },
    stats() { return { enabled: config.enableInboundCapture, ...store.stats() }; },
    flushOutbox, start, stop,
  };
}
