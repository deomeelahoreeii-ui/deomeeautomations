import assert from "node:assert/strict";
import { chromium } from "playwright-core";

const webBase = process.env.CRM_WEB_BASE_URL || "http://127.0.0.1:4321";
const apiBase = process.env.CRM_API_BASE_URL || "http://127.0.0.1:8020";
const executablePath = process.env.CHROMIUM_PATH || "/usr/bin/chromium";
const listing = await (await fetch(
  `${apiBase}/api/v1/crm/reply-workspace/cases?scope=all&page=1&page_size=200`,
)).json();
const record = listing.items?.find((item) => ["Approved", "Issued"].includes(item.reply_status));
assert.ok(record, "an approved or issued complaint is required for the packet UI smoke test");
const editor = await (await fetch(`${apiBase}/api/v1/crm/reply-workspace/cases/${record.case_id}`)).json();
const revision = editor.revisions?.find((item) => ["Approved", "Issued"].includes(item.approval_status));
assert.ok(revision, "the selected complaint needs an immutable approved revision");

const fakeArtifactId = "11111111-1111-4111-8111-111111111111";
const fakeLetterId = "22222222-2222-4222-8222-222222222222";
const defaults = {
  configuration_updated_at: "2026-07-22T08:00:00",
  letter_number: "1595/PMDU/CRM",
  letter_date: "2026-07-13",
  recipient_name: "The Chief Executive Officer (DEA),",
  recipient_location: "Lahore",
  subject_prefix: "COMPLAINT NO.",
  cc_entries: "Office Record.",
  template: { id: "33333333-3333-4333-8333-333333333333", name: "DEO CRM Official Letter", version: 1, active: true },
  signature: { id: "44444444-4444-4444-8444-444444444444", name: "DEO M-EE Lahore", designation: "DISTRICT EDUCATION OFFICER (M-EE)", active: true },
};
const letter = {
  id: fakeLetterId,
  status: "finalized",
  letter_number: defaults.letter_number,
  letter_date: defaults.letter_date,
  letter_url: `/crm/replies/${record.case_id}/official-letter/?letter=${fakeLetterId}`,
  artifacts: [{
    id: fakeArtifactId,
    kind: "complete_pdf",
    name: "complete.pdf",
    size_bytes: 1000,
    download_url: `/api/v1/crm/official-letters/artifacts/${fakeArtifactId}/download`,
  }],
};
let created = false;
let submittedBody;
const preparation = () => ({
  action: created ? "existing" : "create",
  reason: created ? "The packet already exists" : "The approved reply is ready for a final PDF packet",
  confirmation_token: "a".repeat(64),
  case: { id: record.case_id, complaint_number: record.complaint_number },
  approved_revision: { id: revision.id, approval_status: revision.approval_status, captured_at: revision.captured_at },
  defaults,
  source_documents: [{ id: "doc", role: "main_complaint", filename: "complaint.pdf" }],
  current_letter: created ? letter : null,
  active_preview: null,
});

const browser = await chromium.launch({ executablePath, headless: true, args: ["--no-sandbox"] });
try {
  const page = await browser.newPage();
  await page.route(`**/api/v1/crm/official-letters/cases/${record.case_id}/final-packet/prepare`, (route) =>
    route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(preparation()) }),
  );
  await page.route(`**/api/v1/crm/official-letters/cases/${record.case_id}/final-packet`, async (route) => {
    submittedBody = route.request().postDataJSON();
    created = true;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ ...letter, action_performed: "create", idempotent: false }),
    });
  });
  await page.goto(`${webBase}/crm/replies/${record.case_id}/`, { waitUntil: "networkidle" });
  const quick = page.locator("#quick-final-packet");
  await assert.doesNotReject(() => quick.waitFor({ state: "visible" }));
  assert.equal(await quick.textContent(), "Create final PDF packet");
  await quick.click();
  await page.getByText(defaults.letter_number, { exact: false }).waitFor({ state: "visible" });
  await page.getByText(defaults.recipient_name, { exact: false }).waitFor({ state: "visible" });
  await page.locator("#confirm-final-packet").click();
  await page.getByText("Final PDF packet is ready").waitFor({ state: "visible" });
  assert.equal(submittedBody.confirmation_token, "a".repeat(64));
  assert.equal(submittedBody.approved_revision_id, revision.id);
  assert.equal(submittedBody.supersede_current, false);
  assert.equal(await quick.textContent(), "Open final PDF packet");
  assert.equal(
    await page.getByRole("link", { name: "Preview complete PDF" }).getAttribute("href"),
    `/crm/pdf-preview/${fakeArtifactId}.pdf`,
  );
  assert.equal(await page.getByRole("link", { name: "Prepare dispatch" }).getAttribute("href"), `/crm/dispatch/?letter_id=${fakeLetterId}`);
  process.stdout.write(`Reply final-packet UI smoke passed for ${record.complaint_number}\n`);
} finally {
  await browser.close();
}
