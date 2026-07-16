import fs from "node:fs";
import path from "node:path";

const ACTIVE = new Set(["requested", "accepted", "syncing"]);
const TERMINAL = new Set(["succeeded", "no_results", "failed", "timed_out"]);

function nowIso() {
  return new Date().toISOString();
}

export class HistoryStateStore {
  constructor(filePath) {
    this.filePath = filePath;
    this.items = new Map();
    this.load();
  }

  load() {
    try {
      const payload = JSON.parse(fs.readFileSync(this.filePath, "utf8"));
      for (const item of Array.isArray(payload?.items) ? payload.items : []) {
        if (!item?.requestId) continue;
        if (ACTIVE.has(item.status)) {
          item.status = "failed";
          item.active = false;
          item.error = "WhatsApp Web bridge restarted before this request completed";
          item.finishedAt = nowIso();
          item.updatedAt = item.finishedAt;
        }
        this.items.set(item.requestId, item);
      }
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }

  persist() {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    const temporary = `${this.filePath}.${process.pid}.tmp`;
    const items = [...this.items.values()]
      .sort((a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt)))
      .slice(0, 200);
    fs.writeFileSync(temporary, JSON.stringify({ version: 1, items }, null, 2));
    fs.renameSync(temporary, this.filePath);
  }

  active() {
    return [...this.items.values()].find((item) => ACTIVE.has(item.status)) || null;
  }

  create(input) {
    const existing = this.active();
    if (existing) throw new Error(`Another WhatsApp Web history request is active (${existing.requestId})`);
    const timestamp = nowIso();
    const item = {
      requestId: input.requestId,
      workerId: input.workerId,
      provider: "wwebjs",
      status: "accepted",
      active: true,
      requestedCount: input.requestedCount,
      remoteJid: input.remoteJid || null,
      anchorMessageId: input.anchorMessageId || null,
      anchorTimestamp: input.anchorTimestamp || null,
      messagesReceived: 0,
      attachmentsDiscovered: 0,
      attachmentsArchived: 0,
      error: null,
      requestedAt: timestamp,
      updatedAt: timestamp,
      finishedAt: null,
    };
    this.items.set(item.requestId, item);
    this.persist();
    return item;
  }

  get(requestId) {
    return this.items.get(requestId) || null;
  }

  update(requestId, patch) {
    const item = this.get(requestId);
    if (!item) throw new Error(`History request ${requestId} was not found`);
    Object.assign(item, patch, { updatedAt: nowIso() });
    item.active = ACTIVE.has(item.status);
    if (TERMINAL.has(item.status) && !item.finishedAt) item.finishedAt = item.updatedAt;
    this.persist();
    return item;
  }

  status(requestId) {
    const item = this.get(requestId);
    if (!item) throw new Error(`History request ${requestId} was not found`);
    return { ...item };
  }
}
