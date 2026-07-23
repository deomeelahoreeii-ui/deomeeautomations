import assert from "node:assert/strict";
import { chromium } from "playwright-core";

const baseUrl = process.env.CRM_WEB_BASE_URL || "http://127.0.0.1:4321";
const executablePath = process.env.CHROMIUM_PATH || "/usr/bin/chromium";
const expectedBatch = process.env.CRM_SENT_BATCH_NUMBER;
const expectedPackets = Number(process.env.CRM_SENT_PACKET_COUNT || "15");

if (!expectedBatch) {
  throw new Error("CRM_SENT_BATCH_NUMBER is required for the sent-compliances smoke test");
}

const browser = await chromium.launch({ executablePath, headless: true });
try {
  const page = await browser.newPage();
  await page.goto(`${baseUrl}/crm/dispatch/`, { waitUntil: "networkidle" });
  await page.locator('[data-direction="upward"]').click();
  await page.locator("#available-count").waitFor();
  await page.waitForFunction(() =>
    document.querySelector("#available-count")?.textContent?.includes("0 available"),
  );
  assert.equal(await page.locator('#eligible-sources input[type="checkbox"]').count(), 0);
  assert.equal((await page.locator("#metric-upward").textContent())?.trim(), "0");

  await page.goto(`${baseUrl}/crm/dispatch/sent/`, { waitUntil: "networkidle" });
  await page.getByText(expectedBatch, { exact: true }).waitFor();
  const summary = page.locator("details.dispatch-batch-packets summary", {
    hasText: `${expectedPackets} packets`,
  });
  await summary.waitFor();
  await summary.click();
  assert.equal(
    await page.locator("details.dispatch-batch-packets[open] .configuration-row").count(),
    expectedPackets,
  );
  assert.match((await page.locator("#submission-count").textContent()) || "", /1 batch/);

  process.stdout.write(
    `Sent-compliances smoke test passed: ${expectedBatch} contains ${expectedPackets} packets\n`,
  );
} finally {
  await browser.close();
}
