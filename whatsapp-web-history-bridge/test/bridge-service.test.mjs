import test from "node:test";
import assert from "node:assert/strict";
import { createBridgeService } from "../lib/bridge-service.js";

function stateStore() {
  const items = new Map();
  return {
    active: () => [...items.values()].find((item) => ["accepted", "syncing"].includes(item.status)) || null,
    create(input) {
      const item = { ...input, status: "accepted", active: true, messagesReceived: 0, attachmentsDiscovered: 0, attachmentsArchived: 0 };
      items.set(item.requestId, item);
      return item;
    },
    get: (id) => items.get(id),
    status: (id) => ({ ...items.get(id) }),
    update(id, patch) {
      Object.assign(items.get(id), patch, { active: ["accepted", "syncing"].includes(patch.status || items.get(id).status) });
      return items.get(id);
    },
  };
}

const sc = { encode: (value) => Buffer.from(value), decode: (value) => Buffer.from(value).toString() };

test("history request is rejected until WhatsApp Web is ready", async () => {
  const service = createBridgeService({
    config: { workerId: "default", maxHistoryCount: 200, maxMessagesScanned: 500, mediaMaxBytes: 1000, requestTimeoutMs: 1000, qrPath: "/tmp/qr.png", mode: "local_auth", protocolVersion: 1 },
    client: {}, store: stateStore(), platform: {}, log: { info() {}, error() {} }, sc,
    runtime: { status: "qr", authenticated: false, error: null },
  });
  await assert.rejects(() => service.requestHistory({ workerId: "default", requestId: "r1", remoteJids: ["1@s.whatsapp.net"], count: 10 }), /needs pairing/);
});

test("health exposes protocol and QR state", () => {
  const service = createBridgeService({
    config: { workerId: "default", maxHistoryCount: 200, maxMessagesScanned: 500, mediaMaxBytes: 1000, requestTimeoutMs: 1000, qrPath: "/tmp/qr.png", mode: "local_auth", protocolVersion: 1 },
    client: {}, store: stateStore(), platform: {}, log: { info() {}, error() {} }, sc,
    runtime: { status: "qr", authenticated: false, error: null, webVersion: null },
  });
  const health = service.health();
  assert.equal(health.protocolVersion, 1);
  assert.equal(health.ready, false);
  assert.equal(health.qrPath, "/tmp/qr.png");
});


test("a timed-out request cannot be resurrected by late WhatsApp Web results", async () => {
  const store = stateStore();
  const chat = {
    isGroup: false,
    async fetchMessages() {
      await new Promise((resolve) => setTimeout(resolve, 25));
      return [];
    },
  };
  const client = {
    async getNumberId() { return { _serialized: "923360249999@c.us" }; },
    async getChatById() { return chat; },
  };
  const service = createBridgeService({
    config: { workerId: "default", maxHistoryCount: 200, maxMessagesScanned: 500, mediaMaxBytes: 1000, requestTimeoutMs: 5, qrPath: "/tmp/qr.png", mode: "local_auth", protocolVersion: 1 },
    client,
    store,
    platform: { async ingest() { throw new Error("should not ingest"); } },
    log: { info() {}, error() {}, warn() {} },
    sc,
    runtime: { status: "ready", authenticated: true, error: null },
  });
  const item = store.create({ requestId: "late", workerId: "default", requestedCount: 10, remoteJid: "923360249999@s.whatsapp.net" });
  await service.processRequest({ remoteJids: ["923360249999@s.whatsapp.net"] }, item);
  assert.equal(store.status("late").status, "timed_out");
});

test("historical PDF metadata and bytes are ingested for only the selected direct chat", async () => {
  const store = stateStore();
  const pdf = Buffer.from("%PDF-1.4\nbridge-test\n");
  const message = {
    id: { _serialized: "false_923360249999@c.us_pdf-1" },
    timestamp: 1_700_000_000,
    fromMe: false,
    from: "923360249999@c.us",
    to: "923001112222@c.us",
    type: "document",
    body: "CRM complaint",
    hasMedia: true,
    _data: {
      mimetype: "application/pdf",
      filename: "complaint.pdf",
      size: pdf.length,
      notifyName: "Faheem",
    },
    async downloadMedia() {
      return {
        mimetype: "application/pdf",
        filename: "complaint.pdf",
        filesize: pdf.length,
        data: pdf.toString("base64"),
      };
    },
  };
  const chat = { isGroup: false, async fetchMessages() { return [message]; } };
  const client = {
    async getNumberId() { return { _serialized: "923360249999@c.us" }; },
    async getChatById(id) {
      assert.equal(id, "923360249999@c.us");
      return chat;
    },
  };
  const ingested = [];
  const uploaded = [];
  const platform = {
    async ingest(event) {
      ingested.push(event);
      return { accepted: true, created: true, attachment_id: "attachment-1" };
    },
    async uploadAttachment(id, bytes, mimetype) {
      uploaded.push({ id, bytes, mimetype });
      return { uploaded: true };
    },
  };
  const service = createBridgeService({
    config: { workerId: "default", maxHistoryCount: 200, maxMessagesScanned: 500, mediaMaxBytes: 1000, requestTimeoutMs: 1000, qrPath: "/tmp/qr.png", mode: "local_auth", protocolVersion: 1 },
    client,
    store,
    platform,
    log: { info() {}, error() {}, warn() {} },
    sc,
    runtime: { status: "ready", authenticated: true, error: null },
  });
  const item = store.create({ requestId: "pdf", workerId: "default", requestedCount: 10, remoteJid: "923360249999@s.whatsapp.net" });
  await service.processRequest({
    remoteJids: ["923360249999@s.whatsapp.net"],
    platformRemoteJid: "923360249999@s.whatsapp.net",
  }, item);

  assert.equal(store.status("pdf").status, "succeeded");
  assert.equal(store.status("pdf").messagesReceived, 1);
  assert.equal(store.status("pdf").attachmentsDiscovered, 1);
  assert.equal(store.status("pdf").attachmentsArchived, 1);
  assert.equal(ingested.length, 1);
  assert.equal(ingested[0].senderJid, "923360249999@s.whatsapp.net");
  assert.equal(ingested[0].chatScope, "direct");
  assert.equal(ingested[0].attachment.originalFilename, "complaint.pdf");
  assert.equal(uploaded.length, 1);
  assert.equal(uploaded[0].id, "attachment-1");
  assert.equal(uploaded[0].mimetype, "application/pdf");
  assert.deepEqual(uploaded[0].bytes, pdf);
});
