import test from "node:test";
import assert from "node:assert/strict";
import { formatError } from "../lib/errors.js";

test("operator-facing errors never contain stack traces", () => {
  const error = new Error("Target closed");
  error.name = "TargetCloseError";
  const text = formatError(error, "bridge_initialize");
  assert.equal(text, "bridge_initialize: TargetCloseError: Target closed");
  assert.equal(text.includes(" at "), false);
  assert.equal(text.includes("node_modules"), false);
});
