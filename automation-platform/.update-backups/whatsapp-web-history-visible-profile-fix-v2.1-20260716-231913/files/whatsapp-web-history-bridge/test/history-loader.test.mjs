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

test("loader expands until it crosses the platform anchor", async () => {
  const all = Array.from({ length: 240 }, (_, index) => message(`m${index}`, 1000 + index));
  const calls = [];
  const chat = {
    async fetchMessages({ limit }) {
      calls.push(limit);
      return all.slice(-limit);
    },
  };
  const result = await fetchOlderMessages(chat, {
    beforeTimestamp: new Date(1150 * 1000).toISOString(),
    count: 20,
    maxScan: 500,
  });
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
  const result = await fetchOlderMessages(chat, {
    beforeTimestamp: new Date(timestamp * 1000).toISOString(),
    anchorMessageId: "anchor",
    count: 10,
  });
  assert.deepEqual(result.map((message) => message.id._serialized), ["older", "same-second-peer"]);
});
