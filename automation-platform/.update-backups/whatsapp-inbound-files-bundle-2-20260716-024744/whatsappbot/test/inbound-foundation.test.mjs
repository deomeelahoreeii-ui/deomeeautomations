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
