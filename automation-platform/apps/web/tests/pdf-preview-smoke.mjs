import assert from "node:assert/strict";
import { chromium } from "playwright-core";

const baseUrl = process.env.CRM_WEB_BASE_URL || "http://127.0.0.1:4321";
const caseId = process.env.CRM_OFFICIAL_LETTER_CASE_ID;
const executablePath = process.env.CHROMIUM_PATH || "/usr/bin/chromium";

if (!caseId) {
  throw new Error("CRM_OFFICIAL_LETTER_CASE_ID is required for the PDF preview smoke test");
}

const browser = await chromium.launch({ executablePath, headless: true });
try {
  const page = await browser.newPage();
  const previewResponses = [];
  page.on("response", (response) => {
    if (new URL(response.url()).pathname.startsWith("/crm/pdf-preview/")) {
      previewResponses.push(response);
    }
  });

  await page.goto(`${baseUrl}/crm/replies/${caseId}/official-letter/`, {
    waitUntil: "networkidle",
  });

  const frame = page.locator("#letter-preview-frame");
  await assert.doesNotReject(() => frame.waitFor({ state: "visible" }));
  const source = await frame.getAttribute("src");
  assert.match(source || "", /^\/crm\/pdf-preview\/[0-9a-f-]+\.pdf$/);
  assert.equal(new URL(source, baseUrl).origin, new URL(baseUrl).origin);

  await page.waitForFunction(() => {
    const loading = document.querySelector("#preview-loading");
    return loading instanceof HTMLElement && loading.hidden;
  });
  assert.equal(await page.locator("#preview-load-error").isVisible(), false);

  assert.ok(previewResponses.length > 0, "the browser did not request the PDF proxy");
  const response = previewResponses.find((candidate) =>
    (candidate.headers()["content-type"] || "").startsWith("application/pdf"),
  );
  assert.ok(
    response,
    `no PDF response was observed: ${JSON.stringify(previewResponses.map((candidate) => ({ status: candidate.status(), headers: candidate.headers() })))}`,
  );
  assert.ok([200, 206].includes(response.status()));
  assert.match(response.headers()["content-type"] || "", /^application\/pdf/);
  assert.equal(response.headers()["x-frame-options"], "SAMEORIGIN");
  assert.match(response.headers()["content-security-policy"] || "", /frame-ancestors 'self'/);

  process.stdout.write(`PDF preview smoke test passed: ${source}\n`);
} finally {
  await browser.close();
}
