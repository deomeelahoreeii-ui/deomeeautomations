/// <reference types="bun" />

const devVarsPath = ".dev.vars";

const requiredProductionEnv = ["PRODUCTION_PUBLIC_CLERK_PUBLISHABLE_KEY"] as const;

for (const key of requiredProductionEnv) {
  if (!process.env[key]) {
    throw new Error(`${key} is required for a production build.`);
  }
}

function upsertEnvValue(source: string, key: string, value: string): string {
  const line = `${key}=${JSON.stringify(value)}`;
  const pattern = new RegExp(`^${key}=.*$`, "m");

  if (pattern.test(source)) {
    return source.replace(pattern, line);
  }

  return `${source.trimEnd()}\n${line}\n`;
}

const originalDevVars = await Bun.file(devVarsPath).text();

let productionDevVars = originalDevVars;
productionDevVars = upsertEnvValue(
  productionDevVars,
  "PUBLIC_CLERK_PUBLISHABLE_KEY",
  process.env.PRODUCTION_PUBLIC_CLERK_PUBLISHABLE_KEY!,
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "CLERK_SECRET_KEY",
  process.env.PRODUCTION_CLERK_SECRET_KEY ?? "sk_live_runtime_secret_from_cloudflare",
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
  "https://teacher-nomination-app.deo-mee-lahore-eii.workers.dev/__clerk",
);
productionDevVars = upsertEnvValue(
  productionDevVars,
  "ADMIN_EMAILS",
  process.env.ADMIN_EMAILS ?? "deo.mee.lahore.eii@gmail.com",
);

await Bun.write(devVarsPath, productionDevVars);

try {
  console.log("Building production bundle with production Clerk runtime values.");

  const build = Bun.spawn(["bunx", "astro", "build"], {
    stdout: "inherit",
    stderr: "inherit",
  });

  const exitCode = await build.exited;

  if (exitCode !== 0) {
    throw new Error(`Production build failed with exit code ${exitCode}.`);
  }
} finally {
  await Bun.write(devVarsPath, originalDevVars);
  console.log("Restored local development .dev.vars.");
}
