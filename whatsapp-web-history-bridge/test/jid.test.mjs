import test from "node:test";
import assert from "node:assert/strict";
import { resolveDirectChat, wwebCandidates } from "../lib/jid.js";

test("candidate conversion keeps phone and LID identities", () => {
  assert.deepEqual(
    wwebCandidates(["923360249999@s.whatsapp.net", "251371852939507@lid"]),
    ["923360249999@c.us", "251371852939507@lid", "251371852939507@c.us"],
  );
});

test("resolver uses the official LID-phone mapping before broad chat scans", async () => {
  const chat = { id: { _serialized: "251371852939507@lid" }, isGroup: false };
  const tried = [];
  const client = {
    async getContactLidAndPhone() {
      return [{ lid: "251371852939507@lid", pn: "923360249999@c.us" }];
    },
    async getNumberId() { return null; },
    async getChatById(id) {
      tried.push(id);
      if (id === "251371852939507@lid") return chat;
      throw new Error("not this id");
    },
    async getChats() { throw new Error("broad scan should not run"); },
  };
  const resolved = await resolveDirectChat(client, ["923360249999@s.whatsapp.net"]);
  assert.equal(resolved.chat, chat);
  assert.equal(resolved.diagnostics.matchedBy, "getChatById");
  assert.ok(tried.includes("251371852939507@lid"));
});
