import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { normalizeInboundMessage } from "../lib/inbound-normalizer.js";
import { InboundMessageStore } from "../lib/inbound-message-store.js";

const sample = {
  key: { id: "ABC", remoteJid: "923360249999@s.whatsapp.net", fromMe: false },
  messageTimestamp: 1720000000,
  pushName: "Faheem",
  message: { documentMessage: { fileName: "complaints.xlsx", mimetype: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", fileLength: 1234, caption: "CRM" } },
};

test("normalizes document metadata", () => {
  const value = normalizeInboundMessage(sample);
  assert.equal(value.messageId, "ABC");
  assert.equal(value.chatScope, "direct");
  assert.equal(value.attachment.mediaKind, "document");
  assert.equal(value.attachment.originalFilename, "complaints.xlsx");
});

test("does not attribute the connected account push name to an outgoing contact", () => {
  const value = normalizeInboundMessage({
    ...sample,
    key: { ...sample.key, fromMe: true },
    pushName: "Operator Name",
  });
  assert.equal(value.fromMe, true);
  assert.equal(value.pushName, null);
});

test("stores messages idempotently and queues one outbox event", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "wa-inbound-"));
  const store = new InboundMessageStore({ filePath: path.join(dir, "store.sqlite"), workerId: "default", log: console });
  const normalized = normalizeInboundMessage(sample);
  store.upsert(normalized);
  store.upsert(normalized);
  const stats = store.stats();
  assert.equal(stats.messages, 1);
  assert.equal(stats.attachments, 1);
  assert.equal(stats.outboxPending, 1);
  assert.equal(store.getMessage(sample.key.remoteJid, sample.key.id).key.id, "ABC");
  store.close();
});

import { createInboundMediaResponder } from "../lib/inbound-media.js";

test("downloads inbound media and uploads bytes to the platform", async () => {
  const uploads = [];
  const responder = createInboundMediaResponder({
    config: {
      workerId: "default",
      inboundPlatformUrl: "http://127.0.0.1:8020",
      inboundPlatformToken: "test-secret",
      inboundMediaMaxBytes: 1024,
      inboundMediaUploadTimeoutMs: 1000,
    },
    store: {
      getMessage(remoteJid, messageId) {
        assert.equal(remoteJid, sample.key.remoteJid);
        assert.equal(messageId, sample.key.id);
        return sample;
      },
    },
    log: { info() {}, warn() {} },
    sc: { encode: (value) => Buffer.from(value), decode: (value) => Buffer.from(value).toString() },
    downloadMedia: async () => Buffer.from("sample-pdf"),
    fetchImpl: async (url, init) => {
      uploads.push({ url, init });
      return new Response(JSON.stringify({
        uploaded: true,
        attachment_id: "00000000-0000-0000-0000-000000000001",
        media_category: "pdf",
      }), { status: 200, headers: { "content-type": "application/json" } });
    },
  });

  const result = await responder.downloadAndUpload({}, {
    action: "download_attachment",
    workerId: "default",
    attachmentId: "00000000-0000-0000-0000-000000000001",
    remoteJid: sample.key.remoteJid,
    messageId: sample.key.id,
    declaredMimeType: "application/pdf",
  });

  assert.equal(result.uploaded, true);
  assert.equal(uploads.length, 1);
  assert.match(uploads[0].url, /attachments\/00000000-0000-0000-0000-000000000001\/content$/);
  assert.equal(Buffer.from(uploads[0].init.body).toString(), "sample-pdf");
  assert.equal(uploads[0].init.headers["x-whatsapp-worker-token"], "test-secret");
  assert.equal(uploads[0].init.headers["x-whatsapp-worker-id"], "default");
});

import { createInboundHistoryResponder } from "../lib/inbound-history.js";

test("selects oldest captured message as history anchor", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "wa-history-anchor-"));
  const store = new InboundMessageStore({ filePath: path.join(dir, "store.sqlite"), workerId: "default", log: console });
  const newer = normalizeInboundMessage({ ...sample, key: { ...sample.key, id: "NEW" }, messageTimestamp: 1720000100 });
  const older = normalizeInboundMessage({ ...sample, key: { ...sample.key, id: "OLD" }, messageTimestamp: 1719990000 });
  store.upsert(newer);
  store.upsert(older);
  const anchor = store.oldestBoundary([sample.key.remoteJid]);
  assert.equal(anchor.message_id, "OLD");
  assert.equal(anchor.remote_jid, sample.key.remoteJid);
  store.close();
});

test("requests on-demand history from the oldest anchor", async () => {
  const calls = [];
  const responder = createInboundHistoryResponder({
    config: { workerId: "default", inboundHistoryMaxCount: 200 },
    store: {
      oldestBoundary() {
        return {
          remote_jid: sample.key.remoteJid,
          message_id: "OLD",
          from_me: 0,
          message_timestamp: "2024-07-03T00:00:00.000Z",
        };
      },
      recordHistoryRequest(value) { calls.push({ stored: value }); },
      markHistoryRequestFailed() {},
    },
    log: { info() {}, warn() {} },
    sc: { encode: (value) => Buffer.from(value), decode: (value) => Buffer.from(value).toString() },
  });
  const result = await responder.requestHistory({
    async fetchMessageHistory(count, key, timestamp) {
      calls.push({ count, key, timestamp });
      return "operation-1";
    },
  }, {
    action: "request_history",
    workerId: "default",
    requestId: "request-1",
    remoteJids: [sample.key.remoteJid],
    count: 50,
  });
  assert.equal(result.accepted, true);
  assert.equal(result.operationId, "operation-1");
  assert.equal(calls[0].stored.requestId, "request-1");
  assert.equal(calls[1].count, 50);
  assert.equal(calls[1].key.id, "OLD");
  assert.equal(calls[2].stored.operationId, "operation-1");
});
