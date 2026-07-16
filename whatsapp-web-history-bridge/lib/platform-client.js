import { createHash } from "node:crypto";

async function readResponse(response) {
  const text = await response.text();
  let body = {};
  try {
    body = text ? JSON.parse(text) : {};
  } catch {
    body = { detail: text.slice(0, 1000) };
  }
  if (!response.ok) {
    const detail = typeof body?.detail === "string" ? body.detail : JSON.stringify(body);
    throw new Error(`Automation Platform returned HTTP ${response.status}: ${detail}`);
  }
  return body;
}

export class PlatformClient {
  constructor(config, fetchImpl = fetch) {
    this.config = config;
    this.fetchImpl = fetchImpl;
  }

  headers(extra = {}) {
    if (!this.config.platformToken) throw new Error("WWEBJS_PLATFORM_TOKEN is not configured");
    return {
      "x-whatsapp-worker-token": this.config.platformToken,
      ...extra,
    };
  }

  async ingest(event) {
    const response = await this.fetchImpl(`${this.config.platformUrl}/api/v1/whatsapp/inbound/events`, {
      method: "POST",
      headers: this.headers({ "content-type": "application/json" }),
      body: JSON.stringify(event),
      signal: AbortSignal.timeout(this.config.platformTimeoutMs),
    });
    return readResponse(response);
  }

  async uploadAttachment(attachmentId, buffer, mimetype = null) {
    const sha256 = createHash("sha256").update(buffer).digest("hex");
    const headers = this.headers({
      "content-type": "application/octet-stream",
      "x-whatsapp-worker-id": this.config.workerId,
      "x-content-sha256": sha256,
    });
    if (mimetype) headers["x-declared-mime-type"] = String(mimetype).slice(0, 200);
    const response = await this.fetchImpl(
      `${this.config.platformUrl}/api/v1/whatsapp/inbound/attachments/${encodeURIComponent(attachmentId)}/content`,
      {
        method: "POST",
        headers,
        body: buffer,
        signal: AbortSignal.timeout(this.config.platformTimeoutMs),
      },
    );
    return readResponse(response);
  }
}
