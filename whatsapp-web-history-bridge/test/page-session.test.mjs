import test from "node:test";
import assert from "node:assert/strict";
import { ensureClientPage, isTransientPageError, withPageRecovery } from "../lib/page-session.js";

function readyPage({ url = "https://web.whatsapp.com/", visible = true } = {}) {
  return {
    isClosed: () => false,
    url: () => url,
    async evaluate() {
      return {
        href: url,
        readyState: "complete",
        visibilityState: visible ? "visible" : "hidden",
        webVersion: "2.3000.test",
        injected: true,
      };
    },
  };
}

test("detached-frame errors are classified as transient", () => {
  assert.equal(isTransientPageError(new Error("Attempted to use detached Frame 'abc'.")), true);
  assert.equal(isTransientPageError(new Error("ordinary application failure")), false);
});

test("operation is retried after a transient detached frame", async () => {
  const page = readyPage();
  const client = {
    pupPage: page,
    pupBrowser: { async pages() { return [page]; } },
  };
  let calls = 0;
  const result = await withPageRecovery(client, "get_chats_page", async () => {
    calls += 1;
    if (calls === 1) throw new Error("Attempted to use detached Frame 'old'.");
    return "ok";
  }, { retryDelayMs: 1, pageTimeoutMs: 1000 });
  assert.equal(result, "ok");
  assert.equal(calls, 2);
});

test("closed stale page is rebound to the live WhatsApp page", async () => {
  const stale = { isClosed: () => true, url: () => "https://web.whatsapp.com/" };
  const live = readyPage();
  const client = {
    pupPage: stale,
    pupBrowser: { async pages() { return [stale, live]; } },
  };
  const result = await ensureClientPage(client, { timeoutMs: 1000 });
  assert.equal(result.page, live);
  assert.equal(client.pupPage, live);
});

test("non-transient operation failures are not repeated", async () => {
  const page = readyPage();
  const client = {
    pupPage: page,
    pupBrowser: { async pages() { return [page]; } },
  };
  let calls = 0;
  await assert.rejects(
    () => withPageRecovery(client, "lookup", async () => {
      calls += 1;
      throw new Error("contact does not exist");
    }, { pageTimeoutMs: 1000 }),
    /lookup: contact does not exist/,
  );
  assert.equal(calls, 1);
});
