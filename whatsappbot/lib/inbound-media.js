import { createHash } from "node:crypto";
import { downloadMediaMessage, proto } from "@whiskeysockets/baileys";

const activeDownloads = new Map();

function parseRequest(sc, message) {
  try {
    return JSON.parse(sc.decode(message.data) || "{}");
  } catch {
    throw new Error("Inbound media request is not valid JSON");
  }
}

async function uploadToPlatform({ config, attachmentId, buffer, sha256, declaredMimeType, fetchImpl }) {
  const baseUrl = String(config.inboundPlatformUrl || "").replace(/\/$/, "");
  if (!baseUrl || !config.inboundPlatformToken) {
    throw new Error("WhatsApp inbound platform URL/token is not configured");
  }
  const headers = {
    "content-type": "application/octet-stream",
    "x-whatsapp-worker-token": config.inboundPlatformToken,
    "x-whatsapp-worker-id": config.workerId,
    "x-content-sha256": sha256,
  };
  if (declaredMimeType) {
    headers["x-declared-mime-type"] = String(declaredMimeType).slice(0, 200);
  }
  const response = await fetchImpl(
    `${baseUrl}/api/v1/whatsapp/inbound/attachments/${encodeURIComponent(attachmentId)}/content`,
    {
      method: "POST",
      headers,
      body: buffer,
      signal: AbortSignal.timeout(config.inboundMediaUploadTimeoutMs),
    },
  );
  const responseText = await response.text();
  let body = {};
  try {
    body = responseText ? JSON.parse(responseText) : {};
  } catch {
    body = { detail: responseText.slice(0, 1000) };
  }
  if (!response.ok) {
    const detail = typeof body?.detail === "string" ? body.detail : JSON.stringify(body);
    throw new Error(`Platform rejected inbound media upload (HTTP ${response.status}): ${detail}`);
  }
  return body;
}

export function createInboundMediaResponder({ config, store, log, sc, downloadMedia = downloadMediaMessage, fetchImpl = fetch }) {
  async function downloadAndUpload(sock, request) {
    if (request.workerId && request.workerId !== config.workerId) {
      throw new Error(`Request targets worker ${request.workerId}, not ${config.workerId}`);
    }
    const attachmentId = String(request.attachmentId || "").trim();
    const remoteJid = String(request.remoteJid || "").trim();
    const messageId = String(request.messageId || "").trim();
    if (!attachmentId || !remoteJid || !messageId) {
      throw new Error("attachmentId, remoteJid and messageId are required");
    }
    const lockKey = `${remoteJid}:${messageId}`;
    if (activeDownloads.has(lockKey)) {
      return activeDownloads.get(lockKey);
    }
    const operation = (async () => {
      const stored = store.getMessage(remoteJid, messageId);
      if (!stored?.message) {
        throw new Error("The requested WhatsApp message is not available in the durable inbound store");
      }
      const waMessage = proto.WebMessageInfo.fromObject(stored);
      const buffer = await downloadMedia(
        waMessage,
        "buffer",
        {},
        { reuploadRequest: sock.updateMediaMessage },
      );
      if (!Buffer.isBuffer(buffer) || buffer.length === 0) {
        throw new Error("WhatsApp returned an empty media payload");
      }
      if (buffer.length > config.inboundMediaMaxBytes) {
        throw new Error(
          `Inbound media is ${buffer.length} bytes, above the configured limit of ${config.inboundMediaMaxBytes}`,
        );
      }
      const sha256 = createHash("sha256").update(buffer).digest("hex");
      const uploaded = await uploadToPlatform({
        config,
        attachmentId,
        buffer,
        sha256,
        declaredMimeType: request.declaredMimeType,
        fetchImpl,
      });
      log.info("Archived inbound WhatsApp media", {
        attachmentId,
        messageId,
        remoteJid,
        sizeBytes: buffer.length,
        sha256,
        category: uploaded.media_category,
      });
      return { uploaded: true, workerId: config.workerId, ...uploaded };
    })();
    activeDownloads.set(lockKey, operation);
    try {
      return await operation;
    } finally {
      activeDownloads.delete(lockKey);
    }
  }

  async function respond(sock, message) {
    let request = {};
    try {
      request = parseRequest(sc, message);
      if (request.action !== "download_attachment") {
        throw new Error(`Unsupported inbound media action: ${request.action || "missing"}`);
      }
      const result = await downloadAndUpload(sock, request);
      message.respond(sc.encode(JSON.stringify(result)));
    } catch (error) {
      log.warn("Inbound media request failed", {
        attachmentId: request.attachmentId || null,
        messageId: request.messageId || null,
        error: error.message,
      });
      message.respond(sc.encode(JSON.stringify({
        uploaded: false,
        workerId: config.workerId,
        error: error.message,
      })));
    }
  }

  return { respond, downloadAndUpload };
}
