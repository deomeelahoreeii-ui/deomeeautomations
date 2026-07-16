import { createHash } from "node:crypto";
import { normalizePlatformJid } from "./jid.js";

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

export async function fetchOlderMessages(chat, {
  beforeTimestamp = null,
  anchorMessageId = null,
  count = 50,
  maxScan = 5000,
} = {}) {
  const wanted = Math.max(1, Number(count) || 50);
  const beforeMs = beforeTimestamp ? Date.parse(beforeTimestamp) : Number.NaN;
  const beforeSeconds = Number.isFinite(beforeMs) ? Math.floor(beforeMs / 1000) : null;
  let limit = Math.min(maxScan, Math.max(100, wanted * 2));
  let lastSize = -1;
  let messages = [];

  while (true) {
    messages = await chat.fetchMessages({ limit, fromMe: false });
    const unique = new Map();
    for (const message of messages || []) {
      const key = messageKey(message);
      if (key) unique.set(key, message);
    }
    messages = [...unique.values()];
    const eligible = messages
      .filter((message) => !message.fromMe)
      .filter((message) => {
        if (beforeSeconds == null) return true;
        const timestamp = messageTimestampSeconds(message);
        if (timestamp < beforeSeconds) return true;
        return Boolean(anchorMessageId) && timestamp === beforeSeconds && messageKey(message) !== String(anchorMessageId);
      })
      .sort((a, b) => messageTimestampSeconds(b) - messageTimestampSeconds(a));
    if (eligible.length >= wanted) {
      return eligible.slice(0, wanted).sort((a, b) => messageTimestampSeconds(a) - messageTimestampSeconds(b));
    }
    if (messages.length < limit || messages.length === lastSize || limit >= maxScan) {
      return eligible.sort((a, b) => messageTimestampSeconds(a) - messageTimestampSeconds(b));
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
