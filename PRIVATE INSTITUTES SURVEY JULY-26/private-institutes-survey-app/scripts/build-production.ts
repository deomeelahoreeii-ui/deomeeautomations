/// <reference types="bun" />

import { readdir } from "node:fs/promises";

export {};

const devVarsPath = ".dev.vars";
const buildTimeoutMs = Number(process.env.PRODUCTION_BUILD_TIMEOUT_MS ?? 180000);

async function findNodePath(): Promise<string | null> {
  const probe = Bun.spawn(["sh", "-lc", "command -v node"], {
    stdout: "pipe",
    stderr: "ignore",
  });
  const output = await new Response(probe.stdout).text();
  const exitCode = await probe.exited;
  const nodePath = output.trim();

  if (exitCode === 0 && nodePath && !nodePath.includes("/bun-node-")) {
    return nodePath;
  }

  const nvmNodePath = `${process.env.NVM_BIN ?? ""}/node`;
  if (process.env.NVM_BIN && await Bun.file(nvmNodePath).exists()) {
    return nvmNodePath;
  }

  const home = process.env.HOME ?? "/home/ahmad";
  const nvmVersionsPath = `${home}/.nvm/versions/node`;

  try {
    const versions = await readdir(nvmVersionsPath);
    const sortedVersions = versions
      .filter((version) => /^v\d+\.\d+\.\d+$/.test(version))
      .sort((left, right) => left.localeCompare(right, undefined, { numeric: true }));

    for (const version of sortedVersions.reverse()) {
      const candidate = `${nvmVersionsPath}/${version}/bin/node`;
      if (await Bun.file(candidate).exists()) {
        return candidate;
      }
    }
  } catch {
    // No NVM installation visible to this process.
  }

  return null;
}

function upsertEnvValue(source: string, key: string, value: string): string {
  const line = `${key}=${JSON.stringify(value)}`;
  const pattern = new RegExp(`^${key}=.*$`, "m");

  if (pattern.test(source)) {
    return source.replace(pattern, line);
  }

  return `${source.trimEnd()}\n${line}\n`;
}

function parseEnvFile(source: string): Record<string, string> {
  const values: Record<string, string> = {};

  for (const line of source.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;

    const separatorIndex = trimmed.indexOf("=");
    if (separatorIndex === -1) continue;

    const key = trimmed.slice(0, separatorIndex).trim();
    const rawValue = trimmed.slice(separatorIndex + 1).trim();
    values[key] = rawValue.replace(/^(['"])(.*)\1$/, "$2");
  }

  return values;
}

const originalDevVars = await Bun.file(devVarsPath).text();
const localEnv = parseEnvFile(originalDevVars);
const clerkPublishableKey =
  process.env.PRODUCTION_PUBLIC_CLERK_PUBLISHABLE_KEY ??
  localEnv.PUBLIC_CLERK_PUBLISHABLE_KEY;
const clerkSecretKey =
  process.env.PRODUCTION_CLERK_SECRET_KEY ??
  localEnv.CLERK_SECRET_KEY ??
  "sk_live_runtime_secret_from_cloudflare";

if (!clerkPublishableKey) {
  throw new Error(
    "PUBLIC_CLERK_PUBLISHABLE_KEY is missing from .dev.vars. Set it there or provide PRODUCTION_PUBLIC_CLERK_PUBLISHABLE_KEY.",
  );
}

let productionDevVars = originalDevVars;
productionDevVars = upsertEnvValue(productionDevVars, "AUTH_ENABLED", "false");
productionDevVars = upsertEnvValue(
  productionDevVars,
  "PUBLIC_CLERK_PUBLISHABLE_KEY",
  clerkPublishableKey,
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "CLERK_PUBLISHABLE_KEY",
  clerkPublishableKey,
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "CLERK_SECRET_KEY",
  clerkSecretKey,
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "PUBLIC_CLERK_SIGN_IN_URL",
  "/admin/login",
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "PUBLIC_CLERK_SIGN_UP_URL",
  "/admin/login",
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "PUBLIC_CLERK_PROXY_URL",
  "https://private-institutes-survey-app.deo-mee-lahore-eii.workers.dev/__clerk",
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "ADMIN_EMAILS",
  process.env.ADMIN_EMAILS ?? "deo.mee.lahore.eii@gmail.com",
);

await Bun.write(devVarsPath, productionDevVars);

try {
  console.log("Building production bundle with production Clerk runtime values.");

  const nodePath = await findNodePath();
  const buildCommand = nodePath
    ? [nodePath, "node_modules/astro/bin/astro.mjs", "build"]
    : [process.execPath || "bun", "x", "astro", "build"];

  if (!nodePath) {
    console.warn(
      "Node was not found, so the build is falling back to Bun. If this hangs at a Cloudflare workerd WebSocket warning, install Node 22+ and rerun deploy:production.",
    );
  } else {
    console.log(`Using Node for Astro build: ${nodePath}`);
  }

  const build = Bun.spawn(buildCommand, {
    stdout: "inherit",
    stderr: "inherit",
  });
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    build.kill();
  }, buildTimeoutMs);

  const exitCode = await build.exited;
  clearTimeout(timeout);

  if (timedOut) {
    throw new Error(
      `Production build timed out after ${Math.round(buildTimeoutMs / 1000)}s. Install Node 22+ so Astro can build outside Bun's current Cloudflare workerd limitation, then rerun deploy:production.`,
    );
  }

  if (exitCode !== 0) {
    throw new Error(`Production build failed with exit code ${exitCode}.`);
  }
} finally {
  await Bun.write(devVarsPath, originalDevVars);
  console.log("Restored local development .dev.vars.");
}
