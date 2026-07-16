import { formatError, phaseError } from "./errors.js";

const TRANSIENT_PAGE_PATTERNS = [
  /detached\s+frame/i,
  /execution context was destroyed/i,
  /cannot find context with specified id/i,
  /most likely because of a navigation/i,
  /navigating frame was detached/i,
  /target closed/i,
  /session closed/i,
  /protocol error.*runtime\.callfunctionon/i,
];

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function errorText(error) {
  if (typeof error === "string") return error;
  return String(error?.message || error || "");
}

export function isTransientPageError(error) {
  const text = `${errorText(error)}\n${String(error?.stack || "")}`;
  return TRANSIENT_PAGE_PATTERNS.some((pattern) => pattern.test(text));
}

async function inspectPage(page) {
  if (!page || page.isClosed?.()) return null;
  try {
    return await page.evaluate(() => ({
      href: String(globalThis.location?.href || ""),
      readyState: String(globalThis.document?.readyState || ""),
      visibilityState: String(globalThis.document?.visibilityState || ""),
      webVersion: globalThis.Debug?.VERSION || null,
      injected: typeof globalThis.WWebJS !== "undefined",
    }));
  } catch (error) {
    if (isTransientPageError(error)) return null;
    throw error;
  }
}

function pageUrl(page) {
  try {
    return String(page?.url?.() || "");
  } catch {
    return "";
  }
}

async function chooseWhatsAppPage(client) {
  const pages = await client?.pupBrowser?.pages?.();
  if (!Array.isArray(pages) || !pages.length) return null;
  const ranked = [];
  for (const page of pages) {
    if (!page || page.isClosed?.()) continue;
    const url = pageUrl(page);
    if (!url.startsWith("https://web.whatsapp.com")) continue;
    const state = await inspectPage(page).catch(() => null);
    let score = 10;
    if (state?.readyState === "complete") score += 4;
    if (state?.injected) score += 8;
    if (state?.visibilityState === "visible") score += 2;
    ranked.push({ page, state, score });
  }
  ranked.sort((left, right) => right.score - left.score);
  return ranked[0] || null;
}

export async function ensureClientPage(client, {
  timeoutMs = 30000,
  requireInjected = true,
  settleMs = 300,
} = {}) {
  if (!client?.pupBrowser) return null;
  const deadline = Date.now() + Math.max(1000, timeoutMs);
  let lastError = null;

  while (Date.now() < deadline) {
    let page = client.pupPage;
    let state = null;
    try {
      state = await inspectPage(page);
    } catch (error) {
      lastError = error;
    }

    if (!state || !String(state.href || "").startsWith("https://web.whatsapp.com")) {
      const selected = await chooseWhatsAppPage(client).catch((error) => {
        lastError = error;
        return null;
      });
      if (selected?.page) {
        page = selected.page;
        state = selected.state;
        client.pupPage = page;
      }
    }

    if (page && state && ["interactive", "complete"].includes(state.readyState)) {
      if (requireInjected && !state.injected && typeof client.inject === "function") {
        try {
          await client.inject();
          await delay(settleMs);
          state = await inspectPage(page);
        } catch (error) {
          lastError = error;
          if (!isTransientPageError(error)) throw phaseError("page_reinject", error);
        }
      }
      if (!requireInjected || state?.injected) {
        return { page, state };
      }
    }

    await delay(250);
  }

  const detail = lastError ? formatError(lastError, "last_page_error") : "no usable WhatsApp Web page was found";
  throw new Error(`WhatsApp Web page did not become stable within ${timeoutMs}ms: ${detail}`);
}

export async function withPageRecovery(client, phase, operation, {
  attempts = 3,
  pageTimeoutMs = 30000,
  retryDelayMs = 500,
  requireInjected = true,
} = {}) {
  if (!client?.pupBrowser) return operation();
  let lastError = null;
  const totalAttempts = Math.max(1, attempts);

  for (let attempt = 1; attempt <= totalAttempts; attempt += 1) {
    try {
      await ensureClientPage(client, { timeoutMs: pageTimeoutMs, requireInjected });
      return await operation();
    } catch (error) {
      lastError = error;
      if (!isTransientPageError(error) || attempt >= totalAttempts) break;
      await delay(retryDelayMs * attempt);
    }
  }

  throw phaseError(phase, lastError);
}
