import assert from "node:assert/strict";
import test from "node:test";
import {
  installConsoleNoiseFilter,
  isKnownSessionNoise,
} from "../lib/console-noise-filter.js";

test("recognizes only the noisy WhatsApp signal-session dumps", () => {
  assert.equal(isKnownSessionNoise(["Closing session:", { secret: true }]), true);
  assert.equal(isKnownSessionNoise(["Removing old closed session:", {}]), true);
  assert.equal(isKnownSessionNoise(["WhatsApp Web history bridge ready"]), false);
  assert.equal(isKnownSessionNoise([{ message: "Closing session:" }]), false);
});

test("filter suppresses session dumps but preserves normal logs", () => {
  const seen = [];
  const target = {
    log: (...args) => seen.push(["log", ...args]),
    info: (...args) => seen.push(["info", ...args]),
    debug: (...args) => seen.push(["debug", ...args]),
  };
  const restore = installConsoleNoiseFilter({ target });

  target.log("Closing session:", { privateKey: "hidden" });
  target.info("Removing old closed session:", { rootKey: "hidden" });
  target.log("bridge ready", { workerId: "default" });
  restore();
  target.log("Closing session:", { visibleAfterRestore: true });

  assert.deepEqual(seen, [
    ["log", "bridge ready", { workerId: "default" }],
    ["log", "Closing session:", { visibleAfterRestore: true }],
  ]);
});

test("disabled filter leaves console methods untouched", () => {
  const original = () => {};
  const target = { log: original, info: original, debug: original };
  installConsoleNoiseFilter({ enabled: false, target });
  assert.equal(target.log, original);
});
