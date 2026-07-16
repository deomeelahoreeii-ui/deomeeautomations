import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  createInboundHistoryResponder,
  inboundHistorySocketOptions,
} from "../lib/inbound-history.js";
import { createInboundCapture } from "../lib/inbound-capture.js";
import { InboundMessageStore } from "../lib/inbound-message-store.js";

const remoteJid = "923360249999@s.whatsapp.net";
const sample = {
  key: { id: "ABC", remoteJid, fromMe: false },
  messageTimestamp: 1720000000,
  message: { conversation: "history" },
};

function createStore(prefix) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  return new InboundMessageStore({
    filePath: path.join(dir, "store.sqlite"),
    workerId: "default",
    log: console,
  });
}

test("enables full history sync when inbound capture is enabled", () => {
  const options = inboundHistorySocketOptions({
    enableInboundCapture: true,
    inboundSyncFullHistory: true,
  });
  assert.equal(options.syncFullHistory, true);
  assert.equal(options.shouldSyncHistoryMessage({}), true);
});

test("accepted history requests terminate with no results", () => {
  const store = createStore("wa-history-lifecycle-");
  store.recordHistoryRequest({
    requestId: "request-no-results",
    remoteJid,
    count: 50,
    anchorMessageId: sample.key.id,
    anchorTimestamp: "2024-07-03T00:00:00.000Z",
    operationId: "operation-no-results",
  });
  assert.equal(store.getHistoryRequest("request-no-results").status, "accepted");
  const finished = store.reconcileHistoryRequest("request-no-results", {
    quietMs: 1,
    noResultMs: 1,
    hardTimeoutMs: 60000,
    nowMs: Date.now() + 5000,
  });
  assert.equal(finished.status, "no_results");
  store.close();
});


test("accept acknowledgement cannot overwrite early sync activity", () => {
  const store = createStore("wa-history-early-sync-");
  store.recordHistoryRequest({
    requestId: "request-early-sync",
    remoteJid,
    count: 50,
    anchorMessageId: sample.key.id,
    anchorTimestamp: "2024-07-04T00:00:00.000Z",
    operationId: null,
  });
  store.markActiveHistoryForJids([remoteJid]);
  assert.equal(store.getHistoryRequest("request-early-sync").status, "syncing");
  store.recordHistoryRequest({
    requestId: "request-early-sync",
    remoteJid,
    count: 50,
    anchorMessageId: sample.key.id,
    anchorTimestamp: "2024-07-04T00:00:00.000Z",
    operationId: "operation-early-sync",
  });
  const item = store.getHistoryRequest("request-early-sync");
  assert.equal(item.status, "syncing");
  assert.equal(item.operation_id, "operation-early-sync");
  store.close();
});

test("append activity is correlated and completes after quiet", () => {
  const store = createStore("wa-history-progress-");
  store.recordHistoryRequest({
    requestId: "request-progress",
    remoteJid,
    count: 50,
    anchorMessageId: sample.key.id,
    anchorTimestamp: "2024-07-04T00:00:00.000Z",
    operationId: "operation-progress",
  });
  const responder = createInboundHistoryResponder({
    config: {
      workerId: "default",
      inboundHistoryMaxCount: 200,
      inboundHistoryQuietMs: 1,
      inboundHistoryNoResultMs: 45000,
      inboundHistoryHardTimeoutMs: 180000,
    },
    store,
    log: { info() {}, warn() {} },
    sc: {
      encode: (value) => Buffer.from(value),
      decode: (value) => Buffer.from(value).toString(),
    },
  });
  assert.equal(
    responder.handleMessagesUpsert({ type: "append", messages: [sample] }),
    true,
  );
  assert.equal(store.getHistoryRequest("request-progress").status, "syncing");
  const finished = store.reconcileHistoryRequest("request-progress", {
    quietMs: 1,
    noResultMs: 45000,
    hardTimeoutMs: 180000,
    nowMs: Date.now() + 5000,
  });
  assert.equal(finished.status, "succeeded");
  assert.equal(responder.workerStatus("request-progress").active, false);
  store.close();
});


test("correlates older notify events but not new live messages", () => {
  const store = createStore("wa-history-notify-");
  store.recordHistoryRequest({
    requestId: "request-notify",
    remoteJid,
    count: 50,
    anchorMessageId: "anchor",
    anchorTimestamp: "2024-07-04T00:00:00.000Z",
    operationId: "operation-notify",
  });
  const responder = createInboundHistoryResponder({
    config: {
      workerId: "default",
      inboundHistoryMaxCount: 200,
      inboundHistoryQuietMs: 8000,
      inboundHistoryNoResultMs: 45000,
      inboundHistoryHardTimeoutMs: 180000,
    },
    store,
    log: { info() {}, warn() {} },
    sc: {
      encode: (value) => Buffer.from(value),
      decode: (value) => Buffer.from(value).toString(),
    },
  });
  assert.equal(
    responder.handleMessagesUpsert({ type: "notify", messages: [sample] }),
    true,
  );
  assert.equal(store.getHistoryRequest("request-notify").status, "syncing");

  store.recordHistoryRequest({
    requestId: "request-notify-new",
    remoteJid,
    count: 50,
    anchorMessageId: "anchor-new",
    anchorTimestamp: "2024-07-03T00:00:00.000Z",
    operationId: "operation-notify-new",
  });
  store.markHistoryRequestFailed("request-notify", "test cleanup");
  const liveMessage = {
    ...sample,
    key: { ...sample.key, id: "LIVE" },
    messageTimestamp: 1720100000,
  };
  assert.equal(
    responder.handleMessagesUpsert({ type: "notify", messages: [liveMessage] }),
    false,
  );
  assert.equal(store.getHistoryRequest("request-notify-new").status, "accepted");
  store.close();
});

test("capture can label a matched notify batch as history sync", () => {
  const store = createStore("wa-history-capture-");
  const capture = createInboundCapture({
    config: {
      enableInboundCapture: true,
      inboundPlatformUrl: "",
      inboundPlatformToken: "",
      inboundOutboxBatchSize: 100,
      inboundOutboxFlushMs: 5000,
      inboundPlatformTimeoutMs: 1000,
    },
    store,
    log: { info() {}, error() {} },
  });
  capture.handleMessagesUpsert(
    { type: "notify", messages: [{ ...sample, key: { ...sample.key, id: "HISTORY-CAPTURE" } }] },
    { source: "history_sync" },
  );
  const [outbox] = store.pendingOutbox(10);
  assert.ok(outbox);
  assert.equal(JSON.parse(outbox.payload_json).ingestionSource, "history_sync");
  store.close();
});

test("serializes history requests per linked worker", async () => {
  const responder = createInboundHistoryResponder({
    config: { workerId: "default", inboundHistoryMaxCount: 200 },
    store: {
      reconcileHistoryRequests() {},
      activeHistoryRequest() {
        return { request_id: "already-active" };
      },
    },
    log: { info() {}, warn() {} },
    sc: {
      encode: (value) => Buffer.from(value),
      decode: (value) => Buffer.from(value).toString(),
    },
  });
  await assert.rejects(
    responder.requestHistory({}, {
      action: "request_history",
      workerId: "default",
      requestId: "new-request",
      remoteJids: [remoteJid],
      count: 50,
    }),
    /Another history request is still active/,
  );
});
