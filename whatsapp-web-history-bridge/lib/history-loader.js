import { createHash } from "node:crypto";
import { formatError, phaseError } from "./errors.js";
import { normalizePlatformJid } from "./jid.js";
import { withPageRecovery } from "./page-session.js";

const IMAGE_EXTENSIONS = new Set([".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tif", ".tiff"]);
const SHEET_EXTENSIONS = new Set([".xls", ".xlsx", ".xlsm", ".csv", ".ods"]);
const SHEET_MIMES = new Set([
  "text/csv",
  "application/csv",
  "application/vnd.ms-excel",
  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  "application/vnd.oasis.opendocument.spreadsheet",
]);

function extension(filename) {
  const match = String(filename || "").toLowerCase().match(/(\.[a-z0-9]+)$/);
  return match ? match[1] : "";
}

export function isSupportedMedia({ mimetype, filename, type } = {}) {
  const mime = String(mimetype || "").toLowerCase().split(";")[0].trim();
  const ext = extension(filename);
  return mime.startsWith("image/") || mime === "application/pdf" || SHEET_MIMES.has(mime)
    || IMAGE_EXTENSIONS.has(ext) || ext === ".pdf" || SHEET_EXTENSIONS.has(ext)
    || String(type || "").toLowerCase() === "image";
}

function messageTimestampSeconds(message) {
  const parsed = Number(message?.timestamp || message?._data?.t || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function messageKey(message) {
  return String(message?.id?._serialized || message?.id?.id || message?._data?.id?._serialized || "");
}

function dedupeMessages(messages) {
  const unique = new Map();
  for (const message of messages || []) {
    const key = messageKey(message);
    if (key) unique.set(key, message);
  }
  return [...unique.values()];
}

function eligibleMessages(messages, { afterSeconds, beforeSeconds, anchorMessageId, wanted }) {
  const eligible = dedupeMessages(messages)
    .filter((message) => !message.fromMe)
    .filter((message) => {
      const timestamp = messageTimestampSeconds(message);
      if (afterSeconds != null && timestamp < afterSeconds) return false;
      if (beforeSeconds == null) return true;
      if (timestamp < beforeSeconds) return true;
      return Boolean(anchorMessageId) && timestamp === beforeSeconds && messageKey(message) !== String(anchorMessageId);
    })
    .sort((a, b) => messageTimestampSeconds(b) - messageTimestampSeconds(a));
  const selected = eligible.slice(0, wanted);
  return selected.sort((a, b) => messageTimestampSeconds(a) - messageTimestampSeconds(b));
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout(promise, timeoutMs, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} exceeded ${timeoutMs}ms`)), timeoutMs);
        timer.unref?.();
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function hydrateMessageIds(client, ids, pageRecoveryTimeoutMs) {
  const output = new Array(ids.length);
  const concurrency = Math.min(12, Math.max(1, ids.length));
  let cursor = 0;
  async function worker() {
    while (cursor < ids.length) {
      const index = cursor;
      cursor += 1;
      output[index] = await withPageRecovery(
        client,
        `message_hydration:${ids[index]}`,
        () => client.getMessageById(ids[index]),
        { pageTimeoutMs: pageRecoveryTimeoutMs },
      );
    }
  }
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  return output.filter(Boolean);
}

async function fetchWithCompatibilityLoader(client, chat, limit, timeoutMs, pageRecoveryTimeoutMs) {
  const chatId = String(chat?.id?._serialized || "");
  if (!chatId) throw new Error("Resolved WhatsApp Web chat has no serialized id");
  if (!client?.pupPage?.evaluate || typeof client.getMessageById !== "function") {
    throw new Error("WhatsApp Web compatibility loader is unavailable because the browser page is not attached");
  }

  const result = await withTimeout(withPageRecovery(
    client,
    `compatibility_page:${chatId}`,
    () => client.pupPage.evaluate(
    async (targetChatId, targetLimit) => {
      const errorText = (error) => {
        if (error == null) return "Unknown browser error";
        if (typeof error === "string") return error;
        const name = error.name ? `${error.name}: ` : "";
        const message = error.message || (() => {
          try { return JSON.stringify(error, Object.getOwnPropertyNames(error)); } catch { return String(error); }
        })();
        const stack = String(error.stack || "").split("\n").slice(1, 5).map((line) => line.trim()).filter(Boolean).join(" | ");
        return `${name}${message}${stack ? ` | ${stack}` : ""}`;
      };
      const idOf = (message) => String(message?.id?._serialized || message?.id?.toString?.() || "");
      const incoming = (message) => !message?.isNotification && message?.id?.fromMe !== true;
      const merge = (items) => {
        const byId = new Map();
        for (const item of items || []) {
          const id = idOf(item);
          if (id && incoming(item)) byId.set(id, item);
        }
        return [...byId.values()];
      };
      const diagnostics = {
        chatId: targetChatId,
        requestedLimit: targetLimit,
        initialCount: 0,
        finalCount: 0,
        rounds: 0,
        variants: [],
        loaderKeys: [],
        loaderSource: null,
        failures: [],
      };

      try {
        const targetChat = await window.WWebJS.getChat(targetChatId, { getAsModel: false });
        if (!targetChat) return { ok: false, error: `window.WWebJS.getChat returned no chat for ${targetChatId}`, diagnostics };
        let messages = merge(targetChat.msgs?.getModelsArray?.() || []);
        diagnostics.initialCount = messages.length;
        let stagnantRounds = 0;
        const maxRounds = Math.max(4, Math.min(250, Math.ceil(targetLimit / 20) + 10));

        while (messages.length < targetLimit && diagnostics.rounds < maxRounds) {
          diagnostics.rounds += 1;
          const beforeCount = messages.length;
          let loaderModule;
          try {
            loaderModule = window.require("WAWebChatLoadMessages");
          } catch (error) {
            return { ok: false, error: `window.require(WAWebChatLoadMessages) failed: ${errorText(error)}`, diagnostics };
          }
          diagnostics.loaderKeys = Object.keys(loaderModule || {});
          const loader = loaderModule?.loadEarlierMsgs;
          if (typeof loader !== "function") {
            return { ok: false, error: `WAWebChatLoadMessages.loadEarlierMsgs is unavailable; exports=${diagnostics.loaderKeys.join(",")}`, diagnostics };
          }
          if (!diagnostics.loaderSource) {
            try { diagnostics.loaderSource = String(loader).slice(0, 700); } catch {}
          }

          let loaded = null;
          let loadedSuccessfully = false;
          const variants = [
            ["object_chat", () => loader({ chat: targetChat })],
            ["direct_chat", () => loader(targetChat)],
          ];
          const roundFailures = [];
          for (const [variant, invoke] of variants) {
            try {
              loaded = await Promise.race([
                invoke(),
                new Promise((_, reject) => setTimeout(() => reject(new Error("browser loader round timed out")), 30000)),
              ]);
              diagnostics.variants.push(variant);
              loadedSuccessfully = true;
              break;
            } catch (error) {
              roundFailures.push(`${variant}: ${errorText(error)}`);
            }
          }
          if (!loadedSuccessfully) {
            diagnostics.failures.push(...roundFailures);
            return {
              ok: false,
              error: `All earlier-message loader signatures failed: ${roundFailures.join(" || ")}`,
              diagnostics,
            };
          }

          await new Promise((resolve) => setTimeout(resolve, 300));
          const storeMessages = targetChat.msgs?.getModelsArray?.() || [];
          messages = merge([
            ...(Array.isArray(loaded) ? loaded : []),
            ...storeMessages,
            ...messages,
          ]);
          if (messages.length <= beforeCount) stagnantRounds += 1;
          else stagnantRounds = 0;
          if (stagnantRounds >= 2) break;
        }

        messages.sort((a, b) => Number(a?.t || 0) - Number(b?.t || 0));
        if (messages.length > targetLimit) messages = messages.slice(messages.length - targetLimit);
        diagnostics.finalCount = messages.length;
        return { ok: true, ids: messages.map(idOf).filter(Boolean), diagnostics };
      } catch (error) {
        return { ok: false, error: errorText(error), diagnostics };
      }
    },
    chatId,
    limit,
  ),
    { pageTimeoutMs: pageRecoveryTimeoutMs },
  ), timeoutMs, "WhatsApp Web compatibility history loader");

  if (!result?.ok) {
    const diagnostics = result?.diagnostics ? ` diagnostics=${JSON.stringify(result.diagnostics)}` : "";
    throw new Error(`${result?.error || "WhatsApp Web browser loader failed"}${diagnostics}`);
  }
  const messages = await withTimeout(
    hydrateMessageIds(client, [...new Set(result.ids || [])], pageRecoveryTimeoutMs),
    timeoutMs,
    "WhatsApp Web message hydration",
  );
  return { messages, diagnostics: result.diagnostics };
}

async function fetchHistoryPage(client, chat, limit, timeoutMs, pageRecoveryTimeoutMs) {
  let officialMessages = [];
  let officialError = null;
  try {
    officialMessages = await withTimeout(
      withPageRecovery(
        client,
        `official_fetch_page:${limit}`,
        () => chat.fetchMessages({ limit, fromMe: false }),
        { pageTimeoutMs: pageRecoveryTimeoutMs },
      ),
      timeoutMs,
      `whatsapp-web.js fetchMessages(limit=${limit})`,
    );
  } catch (error) {
    officialError = formatError(error, `official_fetch:${limit}`);
  }

  // A native Store chat already loads and serializes messages directly from
  // WAWebCollections. Falling through to the compatibility loader would call
  // window.WWebJS.getChat/getMessageModel again—the exact serializer path this
  // adapter exists to bypass.
  if (chat?.__nativeStore) {
    if (officialError) throw new Error(officialError);
    return {
      messages: officialMessages,
      diagnostic: { limit, strategy: "native_store", count: officialMessages.length, officialError: null },
    };
  }

  if (officialMessages.length >= limit || !client?.pupPage?.evaluate) {
    if (officialError) throw new Error(officialError);
    return {
      messages: officialMessages,
      diagnostic: { limit, strategy: "official", count: officialMessages.length, officialError: null },
    };
  }

  try {
    const compatible = await fetchWithCompatibilityLoader(client, chat, limit, timeoutMs, pageRecoveryTimeoutMs);
    const messages = compatible.messages.length >= officialMessages.length ? compatible.messages : officialMessages;
    return {
      messages,
      diagnostic: {
        limit,
        strategy: compatible.messages.length >= officialMessages.length ? "compatibility" : "official",
        count: messages.length,
        officialCount: officialMessages.length,
        officialError,
        compatibility: compatible.diagnostics,
      },
    };
  } catch (compatibilityError) {
    if (officialMessages.length) {
      return {
        messages: officialMessages,
        diagnostic: {
          limit,
          strategy: "official_partial",
          count: officialMessages.length,
          officialError,
          compatibilityError: formatError(compatibilityError, `compatibility_fetch:${limit}`),
        },
      };
    }
    const details = [officialError, formatError(compatibilityError, `compatibility_fetch:${limit}`)].filter(Boolean).join(" || ");
    throw new Error(details || `WhatsApp Web returned no messages for limit ${limit}`);
  }
}

export async function fetchOlderMessages(client, chat, {
  afterTimestamp = null,
  beforeTimestamp = null,
  anchorMessageId = null,
  count = 50,
  maxScan = 5000,
  attemptTimeoutMs = 120000,
  syncHistory = true,
  syncHistoryTimeoutMs = 30000,
  syncSettleMs = 3500,
  pageRecoveryTimeoutMs = 30000,
} = {}) {
  const wanted = Math.max(1, Number(count) || 50);
  const afterMs = afterTimestamp ? Date.parse(afterTimestamp) : Number.NaN;
  const beforeMs = beforeTimestamp ? Date.parse(beforeTimestamp) : Number.NaN;
  const afterSeconds = Number.isFinite(afterMs) ? Math.floor(afterMs / 1000) : null;
  const beforeSeconds = Number.isFinite(beforeMs) ? Math.floor(beforeMs / 1000) : null;
  const diagnostics = { sync: null, pages: [] };

  if (syncHistory && typeof chat?.syncHistory === "function") {
    try {
      diagnostics.sync = await withTimeout(
        withPageRecovery(
          client,
          "sync_history_page",
          () => chat.syncHistory(),
          { pageTimeoutMs: pageRecoveryTimeoutMs },
        ),
        syncHistoryTimeoutMs,
        "whatsapp-web.js syncHistory",
      );
      if (diagnostics.sync && syncSettleMs > 0) await delay(syncSettleMs);
    } catch (error) {
      diagnostics.sync = formatError(error, "sync_history_nonfatal");
    }
  }

  let limit = Math.min(maxScan, Math.max(100, wanted * 2));
  let lastSize = -1;
  let messages = [];

  while (true) {
    let page;
    try {
      page = await fetchHistoryPage(client, chat, limit, attemptTimeoutMs, pageRecoveryTimeoutMs);
    } catch (error) {
      throw phaseError("history_fetch", error);
    }
    diagnostics.pages.push(page.diagnostic);
    messages = dedupeMessages(page.messages);
    const eligible = eligibleMessages(messages, { afterSeconds, beforeSeconds, anchorMessageId, wanted });
    if (eligible.length >= wanted) return { messages: eligible, diagnostics };
    if (messages.length < limit || messages.length === lastSize || limit >= maxScan) {
      return { messages: eligible, diagnostics };
    }
    lastSize = messages.length;
    limit = Math.min(maxScan, Math.max(limit + wanted, limit * 2));
  }
}

export function rawMediaMetadata(message) {
  const data = message?._data || {};
  return {
    mimetype: data.mimetype || data.mime || null,
    filename: data.filename || data.fileName || null,
    filesize: Number(data.size || data.fileSize || 0) || null,
    type: message?.type || data.type || null,
  };
}

export function mediaToBuffer(media, maxBytes) {
  if (!media?.data) throw new Error("WhatsApp Web returned no media data");
  const buffer = Buffer.from(media.data, "base64");
  if (!buffer.length) throw new Error("WhatsApp Web returned an empty media payload");
  if (buffer.length > maxBytes) {
    throw new Error(`WhatsApp Web media is ${buffer.length} bytes, above the configured limit of ${maxBytes}`);
  }
  return buffer;
}

export function normalizeMessageEvent(message, {
  workerId,
  platformRemoteJid,
  batchId = null,
  media = null,
} = {}) {
  const timestamp = messageTimestampSeconds(message) || Math.floor(Date.now() / 1000);
  const id = messageKey(message);
  if (!id) throw new Error("WhatsApp Web message has no stable id");
  const remoteJid = normalizePlatformJid(platformRemoteJid || message?.from || message?.id?.remote || "");
  const raw = {
    provider: "wwebjs",
    wwebjsId: message?.id || null,
    from: message?.from || null,
    to: message?.to || null,
    author: message?.author || null,
    timestamp,
    type: message?.type || null,
    hasMedia: Boolean(message?.hasMedia),
    body: message?.body || "",
  };
  const payloadJson = JSON.stringify(raw);
  const descriptor = media && isSupportedMedia(media)
    ? {
        mediaKind: String(message?.type || media.type || "document"),
        messageKey: String(message?.type || media.type || "document"),
        originalFilename: media.filename || null,
        mimeType: media.mimetype || null,
        declaredSize: Number(media.filesize || 0) || null,
        mediaSha256: null,
        caption: String(message?.body || "").trim() || null,
      }
    : null;
  return {
    workerId,
    batchId,
    messageId: id,
    remoteJid,
    participantJid: null,
    senderJid: remoteJid,
    fromMe: Boolean(message?.fromMe),
    chatScope: "direct",
    messageTimestamp: new Date(timestamp * 1000).toISOString(),
    pushName: String(message?._data?.notifyName || "").trim() || null,
    text: String(message?.body || "").trim() || null,
    messageType: String(message?.type || "unknown"),
    ingestionSource: "web_history",
    payloadSha256: createHash("sha256").update(payloadJson).digest("hex"),
    rawPayload: raw,
    attachment: descriptor,
  };
}
