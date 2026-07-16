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

function config(overrides = {}) {
  return {
    workerId: "default",
    maxHistoryCount: 200,
    maxMessagesScanned: 500,
    mediaMaxBytes: 1000,
    requestTimeoutMs: 1000,
    fetchAttemptTimeoutMs: 1000,
    syncHistory: false,
    syncHistoryTimeoutMs: 1000,
    syncSettleMs: 0,
    qrPath: "/tmp/qr.png",
    browserUrl: "http://127.0.0.1:9222",
    mode: "browser_url",
    allowLocalAuthHistory: false,
    protocolVersion: 2,
    ...overrides,
  };
}

function service(overrides = {}) {
  return createBridgeService({
    config: config(overrides.config),
    client: overrides.client || {},
    store: overrides.store || stateStore(),
    platform: overrides.platform || {},
    log: overrides.log || { info() {}, error() {}, warn() {} },
    sc,
    runtime: overrides.runtime || { status: "ready", authenticated: true, error: null, browserEndpoint: { browser: "Brave" } },
  });
}

test("history request is rejected until the attached WhatsApp Web page is ready", async () => {
  const instance = service({ runtime: { status: "qr", authenticated: false, error: null } });
  await assert.rejects(
    () => instance.requestHistory({ workerId: "default", requestId: "r1", remoteJids: ["1@s.whatsapp.net"], count: 10 }),
    /needs pairing/,
  );
});

test("local_auth is explicitly blocked because it is not the visible browser profile", async () => {
  const instance = service({ config: { mode: "local_auth", allowLocalAuthHistory: false } });
  await assert.rejects(
    () => instance.requestHistory({ workerId: "default", requestId: "r1", remoteJids: ["923360249999@s.whatsapp.net"], count: 10 }),
    /separate linked-device browser session.*launch-brave-debug\.sh/s,
  );
});

test("health exposes protocol 2 and whether historical retrieval is actually ready", () => {
  const instance = service();
  const health = instance.health();
  assert.equal(health.protocolVersion, 2);
  assert.equal(health.ready, true);
  assert.equal(health.historyReady, true);
  assert.equal(health.mode, "browser_url");
  assert.equal(health.browserUrl, "http://127.0.0.1:9222");
});

test("health keeps LocalAuth alive but marks it unusable for history", () => {
  const instance = service({ config: { mode: "local_auth", allowLocalAuthHistory: false } });
  const health = instance.health();
  assert.equal(health.ready, true);
  assert.equal(health.historyReady, false);
  assert.match(health.warning, /separate linked-device/);
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
  const instance = service({
    config: { requestTimeoutMs: 5 },
    client,
    store,
    platform: { async ingest() { throw new Error("should not ingest"); } },
  });
  const item = store.create({ requestId: "late", workerId: "default", requestedCount: 10, remoteJid: "923360249999@s.whatsapp.net" });
  await instance.processRequest({ remoteJids: ["923360249999@s.whatsapp.net"] }, item);
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
  const instance = service({ client, store, platform });
  const item = store.create({ requestId: "pdf", workerId: "default", requestedCount: 10, remoteJid: "923360249999@s.whatsapp.net" });
  await instance.processRequest({
    remoteJids: ["923360249999@s.whatsapp.net"],
    platformRemoteJid: "923360249999@s.whatsapp.net",
  }, item);

  assert.equal(store.status("pdf").status, "succeeded");
  assert.equal(store.status("pdf").messagesReceived, 1);
  assert.equal(store.status("pdf").attachmentsDiscovered, 1);
  assert.equal(store.status("pdf").attachmentsArchived, 1);
  assert.equal(store.status("pdf").diagnostics.resolution.matchedBy, "getChatById");
  assert.equal(ingested.length, 1);
  assert.equal(ingested[0].senderJid, "923360249999@s.whatsapp.net");
  assert.equal(ingested[0].chatScope, "direct");
  assert.equal(ingested[0].attachment.originalFilename, "complaint.pdf");
  assert.equal(uploaded.length, 1);
  assert.equal(uploaded[0].id, "attachment-1");
  assert.equal(uploaded[0].mimetype, "application/pdf");
  assert.deepEqual(uploaded[0].bytes, pdf);
});

test("opaque thrown strings are persisted with a useful phase", async () => {
  const store = stateStore();
  const client = {
    async getNumberId() { throw "r"; },
    async getContactLidAndPhone() { throw "mapping unavailable"; },
    async getChatById() { throw "chat lookup unavailable"; },
    async getChats() { throw "r"; },
  };
  const instance = service({ client, store });
  const item = store.create({ requestId: "opaque", workerId: "default", requestedCount: 10, remoteJid: "923360249999@s.whatsapp.net" });
  await instance.processRequest({ remoteJids: ["923360249999@s.whatsapp.net"] }, item);
  assert.equal(store.status("opaque").status, "failed");
  assert.match(store.status("opaque").error, /history_request:.*get_chats: r/s);
  assert.notEqual(store.status("opaque").error, "r");
});
