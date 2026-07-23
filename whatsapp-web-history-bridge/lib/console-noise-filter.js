const SESSION_NOISE_PREFIXES = Object.freeze([
  "Closing session:",
  "Removing old closed session:",
]);

export function isKnownSessionNoise(args) {
  const first = args?.[0];
  return typeof first === "string"
    && SESSION_NOISE_PREFIXES.some((prefix) => first.startsWith(prefix));
}

export function installConsoleNoiseFilter({ enabled = true, target = console } = {}) {
  if (!enabled) return () => {};
  const originals = new Map();
  for (const method of ["log", "info", "debug"]) {
    if (typeof target[method] !== "function") continue;
    const original = target[method].bind(target);
    originals.set(method, target[method]);
    target[method] = (...args) => {
      if (isKnownSessionNoise(args)) return;
      original(...args);
    };
  }
  return () => {
    for (const [method, original] of originals) target[method] = original;
  };
}
