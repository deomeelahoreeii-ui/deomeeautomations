import test from "node:test";
import assert from "node:assert/strict";
import { inspectNativeDirectChats, resolveNativeDirectChat } from "../lib/native-store.js";
import { resolveDirectChat } from "../lib/jid.js";

function createHarness() {
  const pdf = Buffer.from("%PDF-1.4\nnative-store-test\n");
  const chatId = "251371852939507@lid";
  const messageId = `false_${chatId}_PDF1`;
  const message = {
    id: { _serialized: messageId, id: "PDF1", remote: { _serialized: chatId }, fromMe: false },
    t: 1_700_000_000,
    type: "document",
    caption: "CRM complaint",
    mimetype: "application/pdf",
    filename: "complaint.pdf",
    size: pdf.length,
    directPath: "/media/path",
    mediaData: { mediaStage: "RESOLVED" },
    mediaKey: "key",
    mediaKeyTimestamp: 1,
    encFilehash: "enc",
    filehash: "plain",
    notifyName: "Faheem",
  };
  const chat = {
    id: { _serialized: chatId, user: "251371852939507", server: "lid" },
    formattedTitle: "Faheem Bukhari CEO Office",
    isGroup: false,
    contact: {
      id: { _serialized: chatId },
      phoneNumber: { _serialized: "923360249999@c.us" },
    },
    msgs: { getModelsArray: () => [message] },
  };
  const chatCollection = {
    getModelsArray: () => [chat],
    get(value) {
      const id = String(value?._serialized || value || "");
      return id === chatId ? chat : null;
    },
  };
  const messageCollection = {
    get: (value) => value === messageId ? message : null,
    getModelsArray: () => [message],
    async getMessagesById(values) {
      return { messages: values.includes(messageId) ? [message] : [] };
    },
  };
  const modules = {
    WAWebCollections: { Chat: chatCollection, Msg: messageCollection },
    WAWebWidFactory: { createWid: (value) => ({ _serialized: String(value) }) },
    WAWebApiContact: { getPhoneNumber: () => ({ _serialized: "923360249999@c.us" }) },
    WAWebChatLoadMessages: { loadEarlierMsgs: async () => [] },
    WAWebDownloadManager: {
      downloadManager: {
        async downloadAndMaybeDecrypt() { return new Uint8Array(pdf); },
      },
    },
  };
  const browserGlobals = {
    location: { href: "https://web.whatsapp.com/" },
    Debug: { VERSION: "2.3000.test" },
    document: {
      readyState: "complete",
      visibilityState: "visible",
      querySelectorAll: () => [{ getAttribute: () => messageId }],
    },
    WWebJS: {
      async enforceLidAndPnRetrieval(value) {
        const text = String(value || "");
        if (text.includes("923360249999")) {
          return {
            lid: { _serialized: chatId },
            phone: { _serialized: "923360249999@c.us" },
          };
        }
        throw new Error("unknown identity");
      },
      async arrayBufferToBase64Async(value) {
        return Buffer.from(value).toString("base64");
      },
    },
    require(name) {
      if (!(name in modules)) throw new Error(`unknown module ${name}`);
      return modules[name];
    },
  };
  const page = {
    isClosed: () => false,
    url: () => "https://web.whatsapp.com/",
    async evaluate(fn, ...args) {
      const previous = new Map();
      for (const [key, value] of Object.entries(browserGlobals)) {
        previous.set(key, globalThis[key]);
        globalThis[key] = value;
      }
      try {
        return await fn(...args);
      } finally {
        for (const [key, value] of previous.entries()) {
          if (value === undefined) delete globalThis[key];
          else globalThis[key] = value;
        }
      }
    },
  };
  const client = {
    pupPage: page,
    pupBrowser: { async pages() { return [page]; } },
    async getNumberId() { throw "r"; },
    async getChatById() { throw "r"; },
    async getChats() { throw new Error("getChats must not be called in a real browser session"); },
    async syncHistory() { return false; },
  };
  return { client, pdf, chatId, messageId };
}

test("native Store lookup resolves a phone-number request to its LID chat", async () => {
  const { client, chatId } = createHarness();
  const inspected = await inspectNativeDirectChats(client, ["923360249999@s.whatsapp.net"]);
  assert.equal(inspected.matched.id, chatId);
  assert.equal(inspected.matched.exact, true);
  assert.equal(inspected.diagnostics.scannedChats, 1);
});

test("native chat adapter loads message metadata and downloads media by stable ID", async () => {
  const { client, pdf, chatId, messageId } = createHarness();
  const resolved = await resolveNativeDirectChat(client, ["923360249999@s.whatsapp.net"]);
  assert.equal(resolved.chat.id._serialized, chatId);
  const messages = await resolved.chat.fetchMessages({ limit: 50, fromMe: false });
  assert.equal(messages.length, 1);
  assert.equal(messages[0].id._serialized, messageId);
  assert.equal(messages[0]._data.filename, "complaint.pdf");
  const media = await messages[0].downloadMedia();
  assert.equal(media.mimetype, "application/pdf");
  assert.deepEqual(Buffer.from(media.data, "base64"), pdf);
});

test("direct-chat resolution bypasses the broken high-level getChats serializer", async () => {
  const { client, chatId } = createHarness();
  let getChatsCalls = 0;
  client.getChats = async () => { getChatsCalls += 1; throw "r"; };
  const resolved = await resolveDirectChat(client, ["923360249999@s.whatsapp.net"]);
  assert.equal(resolved.chat.id._serialized, chatId);
  assert.equal(resolved.diagnostics.matchedBy, "nativeStore");
  assert.equal(getChatsCalls, 0);
});

test("an unrelated active chat is never selected only because it is visible", async () => {
  const { client } = createHarness();
  const inspected = await inspectNativeDirectChats(client, ["923001112222@s.whatsapp.net"]);
  assert.equal(inspected.matched, null);
  assert.equal(inspected.diagnostics.topCandidates.length, 0);
});

