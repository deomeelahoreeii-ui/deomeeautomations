/// <reference types="bun" />

export {};

const generatedConfigPath = "dist/server/wrangler.json";
const productionDatabase = {
  binding: "DB",
  database_name: "private_institutes_survey_production_db",
  database_id: "4e45c15e-5d72-42a0-80ff-7e9449a1e612",
  remote: true,
};
const productionVars = {
  AUTH_ENABLED: "false",
  PUBLIC_CLERK_PUBLISHABLE_KEY: "pk_test_dHJ1c3Rpbmcta3JpbGwtOTYuY2xlcmsuYWNjb3VudHMuZGV2JA",
  CLERK_PUBLISHABLE_KEY: "pk_test_dHJ1c3Rpbmcta3JpbGwtOTYuY2xlcmsuYWNjb3VudHMuZGV2JA",
  PUBLIC_CLERK_SIGN_IN_URL: "/admin/login",
  PUBLIC_CLERK_SIGN_UP_URL: "/admin/login",
  PUBLIC_CLERK_PROXY_URL:
    "https://private-institutes-survey-app.deo-mee-lahore-eii.workers.dev/__clerk",
  ADMIN_EMAILS: "deo.mee.lahore.eii@gmail.com,pomubbsher@gmail.com",
};

const configFile = Bun.file(generatedConfigPath);

if (!(await configFile.exists())) {
  throw new Error(`Generated Wrangler config not found: ${generatedConfigPath}`);
}

const config = await configFile.json();
config.d1_databases = [productionDatabase];
config.vars = {
  ...(config.vars ?? {}),
  ...productionVars,
};

await Bun.write(generatedConfigPath, `${JSON.stringify(config)}\n`);

console.log(
  `Patched ${generatedConfigPath}: DB -> ${productionDatabase.database_name}, Clerk vars enabled`,
);
