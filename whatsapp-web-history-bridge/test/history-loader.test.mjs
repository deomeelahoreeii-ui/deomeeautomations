import test from "node:test";
import assert from "node:assert/strict";
import { fetchOlderMessages, isSupportedMedia, normalizeMessageEvent } from "../lib/history-loader.js";

function message(id, timestamp, extra = {}) {
  return {
    id: { _serialized: id },
    timestamp,
    fromMe: false,
    from: "923360249999@c.us",
    type: "chat",
    body: "",
    hasMedia: false,
    _data: {},
    ...extra,
  };
}

const noBrowserClient = {};

test("loader expands until it crosses the platform anchor", async () => {
  const all = Array.from({ length: 240 }, (_, index) => message(`m${index}`, 1000 + index));
  const calls = [];
  const chat = {
    async fetchMessages({ limit }) {
      calls.push(limit);
      return all.slice(-limit);
    },
  };
  const loaded = await fetchOlderMessages(noBrowserClient, chat, {
    beforeTimestamp: new Date(1150 * 1000).toISOString(),
    count: 20,
    maxScan: 500,
    syncHistory: false,
  });
  const result = loaded.messages;
  assert.equal(result.length, 20);
  assert.ok(calls.length > 1);
  assert.ok(result.every((item) => item.timestamp < 1150));
  assert.deepEqual(result.map((item) => item.timestamp), [...result.map((item) => item.timestamp)].sort((a, b) => a - b));
});

test("supported media classification covers requested file families", () => {
  assert.equal(isSupportedMedia({ mimetype: "application/pdf" }), true);
  assert.equal(isSupportedMedia({ filename: "report.xlsx" }), true);
  assert.equal(isSupportedMedia({ mimetype: "image/jpeg" }), true);
  assert.equal(isSupportedMedia({ mimetype: "video/mp4" }), false);
});

test("normalized event keeps the platform contact jid", () => {
  const event = normalizeMessageEvent(message("abc", 1000, { body: "hello" }), {
    workerId: "default",
    platformRemoteJid: "923360249999@s.whatsapp.net",
  });
  assert.equal(event.senderJid, "923360249999@s.whatsapp.net");
  assert.equal(event.ingestionSource, "web_history");
  assert.equal(event.messageId, "abc");
});

test("loader keeps same-second peers while excluding the anchor", async () => {
  const timestamp = 1_700_000_000;
  const messages = [
    message("older", timestamp - 1),
    message("same-second-peer", timestamp),
    message("anchor", timestamp),
  ];
  const chat = { fetchMessages: async () => messages };
  const loaded = await fetchOlderMessages(noBrowserClient, chat, {
    beforeTimestamp: new Date(timestamp * 1000).toISOString(),
    anchorMessageId: "anchor",
    count: 10,
    syncHistory: false,
  });
  assert.deepEqual(loaded.messages.map((item) => item.id._serialized), ["older", "same-second-peer"]);
});

test("opaque official fetch failure falls back to the attached-page compatibility loader", async () => {
  const messages = new Map([
    ["older-1", message("older-1", 1000)],
    ["older-2", message("older-2", 1001)],
  ]);
  const client = {
    pupPage: {
      async evaluate() {
        return {
          ok: true,
          ids: ["older-1", "older-2"],
          diagnostics: { variants: ["object_chat"], finalCount: 2 },
        };
      },
    },
    async getMessageById(id) { return messages.get(id) || null; },
  };
  const chat = {
    id: { _serialized: "923360249999@c.us" },
    async fetchMessages() { throw "r"; },
  };
  const loaded = await fetchOlderMessages(client, chat, {
    count: 2,
    maxScan: 100,
    syncHistory: false,
  });
  assert.deepEqual(loaded.messages.map((item) => item.id._serialized), ["older-1", "older-2"]);
  assert.equal(loaded.diagnostics.pages[0].strategy, "compatibility");
  assert.match(loaded.diagnostics.pages[0].officialError, /official_fetch:100: r/);
});

test("syncHistory is attempted but a sync failure does not hide locally cached history", async () => {
  const chat = {
    async syncHistory() { throw new Error("peer sync unavailable"); },
    async fetchMessages() { return [message("cached", 1000)]; },
  };
  const loaded = await fetchOlderMessages(noBrowserClient, chat, {
    count: 1,
    syncHistory: true,
    syncSettleMs: 0,
  });
  assert.equal(loaded.messages[0].id._serialized, "cached");
  assert.match(String(loaded.diagnostics.sync), /sync_history_nonfatal/);
});

test("both loader failures preserve phase and browser diagnostics", async () => {
  const client = {
    pupPage: {
      async evaluate() {
        return {
          ok: false,
          error: "All earlier-message loader signatures failed: object_chat: r || direct_chat: TypeError: bad argument",
          diagnostics: { loaderKeys: ["loadEarlierMsgs"], variants: [], loaderSource: "function r(e){}" },
        };
      },
    },
    async getMessageById() { return null; },
  };
  const chat = {
    id: { _serialized: "923360249999@c.us" },
    async fetchMessages() { throw "r"; },
  };
  await assert.rejects(
    () => fetchOlderMessages(client, chat, { count: 2, syncHistory: false }),
    /history_fetch:.*official_fetch:100: r.*loader signatures failed.*loaderKeys/s,
  );
});
