/// <reference types="bun" />

const generatedConfigPath = "dist/server/wrangler.json";
const productionDatabase = {
  binding: "DB",
  database_name: "pectaa_production_db",
  database_id: "497d2c4a-7e49-42ac-835e-236b821c502f",
  remote: true,
};

const configFile = Bun.file(generatedConfigPath);

if (!(await configFile.exists())) {
  throw new Error(`Generated Wrangler config not found: ${generatedConfigPath}`);
}

const config = await configFile.json();
config.d1_databases = [productionDatabase];

await Bun.write(generatedConfigPath, `${JSON.stringify(config)}\n`);

console.log(
  `Patched ${generatedConfigPath}: DB -> ${productionDatabase.database_name}`,
);
