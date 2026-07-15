import { createHash } from "node:crypto";

const MEDIA_KEYS = [
  ["imageMessage", "image"],
  ["documentMessage", "document"],
  ["videoMessage", "video"],
  ["audioMessage", "audio"],
  ["stickerMessage", "sticker"],
];

function unwrapMessageContent(message) {
  let current = message || {};
  for (let depth = 0; depth < 8; depth += 1) {
    if (current?.ephemeralMessage?.message) current = current.ephemeralMessage.message;
    else if (current?.viewOnceMessage?.message) current = current.viewOnceMessage.message;
    else if (current?.viewOnceMessageV2?.message) current = current.viewOnceMessageV2.message;
    else if (current?.viewOnceMessageV2Extension?.message) current = current.viewOnceMessageV2Extension.message;
    else if (current?.documentWithCaptionMessage?.message) current = current.documentWithCaptionMessage.message;
    else break;
  }
  return current || {};
}

function unixSeconds(value) {
  if (value == null) return null;
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value?.toNumber === "function") return value.toNumber();
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeText(content) {
  return String(
    content?.conversation ||
      content?.extendedTextMessage?.text ||
      content?.imageMessage?.caption ||
      content?.videoMessage?.caption ||
      content?.documentMessage?.caption ||
      "",
  ).trim();
}

function mediaDescriptor(content) {
  for (const [key, kind] of MEDIA_KEYS) {
    const media = content?.[key];
    if (!media) continue;
    return {
      mediaKind: kind,
      messageKey: key,
      mimeType: String(media.mimetype || "").trim() || null,
      originalFilename: String(media.fileName || "").trim() || null,
      declaredSize: unixSeconds(media.fileLength),
      mediaSha256: media.fileSha256
        ? Buffer.from(media.fileSha256).toString("hex")
        : null,
      caption: String(media.caption || "").trim() || null,
    };
  }
  return null;
}

export function normalizeInboundMessage(rawMessage, { source = "live" } = {}) {
  const key = rawMessage?.key || {};
  const remoteJid = String(key.remoteJid || "").trim();
  const participantJid = String(key.participant || "").trim() || null;
  const messageId = String(key.id || "").trim();
  if (!remoteJid || !messageId) return null;

  const content = unwrapMessageContent(rawMessage?.message);
  const media = mediaDescriptor(content);
  const timestamp = unixSeconds(rawMessage?.messageTimestamp);
  const serialized = JSON.stringify(rawMessage);
  const payloadSha256 = createHash("sha256").update(serialized).digest("hex");

  return {
    accountKey: null,
    messageId,
    remoteJid,
    participantJid,
    senderJid: participantJid || remoteJid,
    fromMe: Boolean(key.fromMe),
    chatScope: remoteJid.endsWith("@g.us") ? "group" : "direct",
    messageTimestamp: timestamp ? new Date(timestamp * 1000).toISOString() : new Date().toISOString(),
    pushName: String(rawMessage?.pushName || "").trim() || null,
    text: safeText(content) || null,
    messageType: media?.messageKey || Object.keys(content || {})[0] || "unknown",
    ingestionSource: source,
    payloadSha256,
    rawPayload: rawMessage,
    attachment: media,
  };
}

export function normalizeInboundBatch(messages, options = {}) {
  return (messages || [])
    .map((message) => normalizeInboundMessage(message, options))
    .filter(Boolean);
}
