import assert from "node:assert/strict";
import { chromium } from "playwright-core";

const webBase = process.env.CRM_WEB_BASE_URL || "http://127.0.0.1:4321";
const apiBase = process.env.CRM_API_BASE_URL || "http://127.0.0.1:8020";
const executablePath = process.env.CHROMIUM_PATH || "/usr/bin/chromium";

const listingResponse = await fetch(
  `${apiBase}/api/v1/crm/reply-workspace/cases?scope=all&page=1&page_size=200`,
);
assert.equal(listingResponse.ok, true, `reply listing failed with ${listingResponse.status}`);
const listing = await listingResponse.json();
const record = listing.items?.find((item) => item.case_id && item.reply_preview);
assert.ok(record, "a complaint with reply text is required for the editor browser smoke test");

const browser = await chromium.launch({ executablePath, headless: true, args: ["--no-sandbox"] });
try {
  const page = await browser.newPage();
  const pageErrors = [];
  const failedRequests = [];
  const badResponses = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("requestfailed", (request) => failedRequests.push(`${request.url()} ${request.failure()?.errorText || "failed"}`));
  page.on("response", (response) => {
    if (response.status() >= 400) badResponses.push(`${response.status()} ${response.url()}`);
  });

  await page.goto(`${webBase}/crm/replies/?scope=all&page=1&page_size=25`, {
    waitUntil: "networkidle",
  });
  const editorLink = page.locator(`a[href^="/crm/replies/${record.case_id}/?"]`).first();
  await editorLink.waitFor({ state: "visible" });
  await editorLink.click();
  await page.waitForFunction(
    (number) => document.querySelector("#editor-title")?.textContent?.includes(number),
    record.complaint_number,
  );
  assert.match(await page.locator("#editor-title").textContent(), new RegExp(record.complaint_number));
  assert.equal((await page.locator("#final-reply").inputValue()).trim().length > 0, true);
  assert.equal(await page.locator("#reply-editor").getAttribute("data-editor-state"), "ready");
  assert.deepEqual(pageErrors, [], `page errors: ${pageErrors.join(" | ")}`);
  assert.deepEqual(failedRequests, [], `failed requests: ${failedRequests.join(" | ")}`);
  assert.deepEqual(badResponses, [], `bad responses: ${badResponses.join(" | ")}`);

  const degraded = await browser.newPage();
  await degraded.route("**/api/v1/crm/taxonomy?**", (route) =>
    route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ detail: "taxonomy test outage" }) }),
  );
  await degraded.route(`**/api/v1/crm/reply-workspace/cases/${record.case_id}/refresh`, (route) =>
    route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ detail: "Helpdesk refresh test outage" }) }),
  );
  await degraded.goto(`${webBase}/crm/replies/${record.case_id}/`, { waitUntil: "networkidle" });
  await degraded.waitForFunction(
    (number) => document.querySelector("#editor-title")?.textContent?.includes(number),
    record.complaint_number,
  );
  assert.equal((await degraded.locator("#final-reply").inputValue()).trim().length > 0, true);
  const warning = await degraded.locator("#editor-warning").textContent();
  assert.match(warning || "", /Taxonomy could not be loaded/);
  assert.match(warning || "", /Helpdesk refresh failed/);

  const bootstrapFailure = await browser.newPage();
  await bootstrapFailure.addInitScript(() => {
    const nativeTimeout = window.setTimeout.bind(window);
    window.setTimeout = (handler, timeout, ...args) =>
      nativeTimeout(handler, timeout === 12000 ? 25 : timeout, ...args);
  });
  await bootstrapFailure.route("**/src/pages/crm/replies/**.astro?astro&type=script&index=0&lang.ts", (route) => route.abort());
  await bootstrapFailure.goto(`${webBase}/crm/replies/${record.case_id}/`, { waitUntil: "domcontentloaded" });
  await bootstrapFailure.getByText("Reply editor could not start").waitFor({ state: "visible" });
  await bootstrapFailure.getByRole("button", { name: "Reload editor" }).waitFor({ state: "visible" });

  process.stdout.write(`Reply editor browser smoke passed for ${record.complaint_number}\n`);
} finally {
  await browser.close();
}
