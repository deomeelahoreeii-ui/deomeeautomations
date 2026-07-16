const MAX_ERROR_TEXT = 6000;

function safeJson(value) {
  try {
    return JSON.stringify(value, Object.getOwnPropertyNames(value));
  } catch {
    return null;
  }
}

export function errorDetails(error) {
  if (error == null) return { name: "Error", message: "Unknown error", stack: null };
  if (typeof error === "string") return { name: "Error", message: error || "Empty error string", stack: null };
  if (typeof error !== "object") return { name: "Error", message: String(error), stack: null };

  const name = String(error.name || error.constructor?.name || "Error");
  const message = String(error.message || safeJson(error) || String(error));
  const stack = typeof error.stack === "string" ? error.stack : null;
  const cause = error.cause ? errorDetails(error.cause) : null;
  return { name, message, stack, cause };
}

export function formatError(error, phase = null) {
  const details = errorDetails(error);
  const prefix = phase ? `${phase}: ` : "";
  const headline = `${prefix}${details.name === "Error" ? "" : `${details.name}: `}${details.message}`;
  const stackLines = String(details.stack || "")
    .split("\n")
    .slice(1, 7)
    .map((line) => line.trim())
    .filter(Boolean);
  const cause = details.cause
    ? ` | caused by ${details.cause.name === "Error" ? "" : `${details.cause.name}: `}${details.cause.message}`
    : "";
  const text = [headline, stackLines.length ? ` | ${stackLines.join(" | ")}` : "", cause].join("");
  return text.slice(0, MAX_ERROR_TEXT);
}

export function phaseError(phase, error) {
  const wrapped = new Error(formatError(error, phase));
  wrapped.cause = error instanceof Error ? error : undefined;
  wrapped.phase = phase;
  return wrapped;
}
