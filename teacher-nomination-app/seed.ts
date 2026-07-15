/// <reference types="bun" />

type NullableSqlValue = string | number | null;

const DEFAULT_CSV_PATH =
  "/home/ahmad/code/deomeeautomations/teacher-nomination-app/data/teachers_data_lahore.csv";
const DEFAULT_OUTPUT_PATH =
  "/home/ahmad/code/deomeeautomations/teacher-nomination-app/data/seed-teachers.generated.sql";
const DEFAULT_DB_NAME = "pectaa_db";
const DEFAULT_BATCH_SIZE = 50;

const teacherColumns = [
  "cnic",
  "teacher_name",
  "mobile_no",
  "gender",
  "designation",
  "subject",
  "grade",
  "cadre",
  "employment_type",
  "degree_level",
  "degree_subject",
  "date_joining_service",
  "date_joining_school",
  "date_joining_post",
  "status",
  "district",
  "tehsil",
  "markaz",
  "emis",
  "school",
  "school_level",
  "personal_no",
  "age",
] as const;

const columnHeaderMap: Record<(typeof teacherColumns)[number], string[]> = {
  cnic: ["CNIC"],
  teacher_name: ["Teacher"],
  mobile_no: ["Mobile#"],
  gender: ["Gender"],
  designation: ["Designation"],
  subject: ["Subject"],
  grade: ["Grade"],
  cadre: ["Cadre"],
  employment_type: ["Type"],
  degree_level: ["Degree Level"],
  degree_subject: ["Degree Subject"],
  date_joining_service: ["Date of Joining Service"],
  date_joining_school: ["Date of Joining School"],
  date_joining_post: ["Date of Joinig Post", "Date of Joining Post"],
  status: ["Status"],
  district: ["District"],
  tehsil: ["Tehsil"],
  markaz: ["Markaz"],
  emis: ["EMIS"],
  school: ["School"],
  school_level: ["Level"],
  personal_no: ["Personal No"],
  age: ["Age"],
};

type ScriptConfig = {
  csvPath: string;
  outputPath: string;
  dbName: string;
  batchSize: number;
  generateOnly: boolean;
  remote: boolean;
};

function parseArgs(argv: string[]): ScriptConfig {
  const config: ScriptConfig = {
    csvPath: DEFAULT_CSV_PATH,
    outputPath: DEFAULT_OUTPUT_PATH,
    dbName: DEFAULT_DB_NAME,
    batchSize: DEFAULT_BATCH_SIZE,
    generateOnly: false,
    remote: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];

    if (arg === "--csv" && next) {
      config.csvPath = next;
      index += 1;
    } else if (arg === "--out" && next) {
      config.outputPath = next;
      index += 1;
    } else if (arg === "--db" && next) {
      config.dbName = next;
      index += 1;
    } else if (arg === "--batch-size" && next) {
      const parsed = Number.parseInt(next, 10);
      if (!Number.isFinite(parsed) || parsed < 1) {
        throw new Error(`Invalid --batch-size value: ${next}`);
      }
      config.batchSize = parsed;
      index += 1;
    } else if (arg === "--generate-only" || arg === "--dry-run") {
      config.generateOnly = true;
    } else if (arg === "--remote") {
      config.remote = true;
    } else if (arg === "--local") {
      config.remote = false;
    } else {
      throw new Error(`Unknown or incomplete argument: ${arg}`);
    }
  }

  return config;
}

function parseCsv(input: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const next = input[index + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
    } else if (char === ",") {
      row.push(field);
      field = "";
    } else if (char === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (char !== "\r") {
      field += char;
    }
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  return rows;
}

function normalizeHeader(header: string): string {
  return header
    .replace(/^\uFEFF/, "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function createHeaderIndex(headers: string[]): Map<string, number> {
  const headerIndex = new Map<string, number>();

  headers.forEach((header, index) => {
    headerIndex.set(normalizeHeader(header), index);
  });

  return headerIndex;
}

function resolveColumnIndexes(headers: string[]): Record<string, number> {
  const headerIndex = createHeaderIndex(headers);
  const result: Record<string, number> = {};
  const missing: string[] = [];

  for (const column of teacherColumns) {
    const aliases = columnHeaderMap[column];
    const found = aliases
      .map((alias) => headerIndex.get(normalizeHeader(alias)))
      .find((index) => index !== undefined);

    if (found === undefined) {
      missing.push(`${column} (${aliases.join(" / ")})`);
    } else {
      result[column] = found;
    }
  }

  if (missing.length > 0) {
    throw new Error(`CSV is missing required headers: ${missing.join(", ")}`);
  }

  return result;
}

function cleanText(value: string | undefined): string | null {
  const cleaned = (value ?? "").trim();
  return cleaned.length > 0 ? cleaned : null;
}

function cleanCnic(value: string | undefined): string | null {
  const cleaned = (value ?? "").trim().replace(/-/g, "");
  return cleaned.length > 0 ? cleaned : null;
}

function cleanInteger(value: string | undefined): number | null {
  const cleaned = cleanText(value);
  if (cleaned === null) return null;

  const normalized = cleaned.replace(/,/g, "");
  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function sqlLiteral(value: NullableSqlValue): string {
  if (value === null) return "NULL";
  if (typeof value === "number") return Number.isFinite(value) ? String(value) : "NULL";
  return `'${value.replace(/\u0000/g, "").replace(/'/g, "''")}'`;
}

function mapRow(row: string[], indexes: Record<string, number>): NullableSqlValue[] | null {
  const cnic = cleanCnic(row[indexes.cnic]);
  if (cnic === null) return null;

  return [
    cnic,
    cleanText(row[indexes.teacher_name]),
    cleanText(row[indexes.mobile_no]),
    cleanText(row[indexes.gender]),
    cleanText(row[indexes.designation]),
    cleanText(row[indexes.subject]),
    cleanInteger(row[indexes.grade]),
    cleanText(row[indexes.cadre]),
    cleanText(row[indexes.employment_type]),
    cleanText(row[indexes.degree_level]),
    cleanText(row[indexes.degree_subject]),
    cleanText(row[indexes.date_joining_service]),
    cleanText(row[indexes.date_joining_school]),
    cleanText(row[indexes.date_joining_post]),
    cleanText(row[indexes.status]),
    cleanText(row[indexes.district]),
    cleanText(row[indexes.tehsil]),
    cleanText(row[indexes.markaz]),
    cleanInteger(row[indexes.emis]),
    cleanText(row[indexes.school]),
    cleanText(row[indexes.school_level]),
    cleanText(row[indexes.personal_no]),
    cleanText(row[indexes.age]),
  ];
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function buildInsertStatement(rows: NullableSqlValue[][]): string {
  const columnList = teacherColumns.join(", ");
  const values = rows
    .map((row) => `(${row.map(sqlLiteral).join(", ")})`)
    .join(",\n");

  return `INSERT OR IGNORE INTO teachers (${columnList}) VALUES\n${values};`;
}

async function executeSqlFile(config: ScriptConfig): Promise<void> {
  const process = Bun.spawn(
    [
      "bunx",
      "wrangler",
      "d1",
      "execute",
      config.dbName,
      config.remote ? "--remote" : "--local",
      "--file",
      config.outputPath,
      "--yes",
    ],
    {
      stdout: "inherit",
      stderr: "inherit",
    },
  );

  const exitCode = await process.exited;
  if (exitCode !== 0) {
    throw new Error(`Wrangler exited with code ${exitCode}`);
  }
}

async function main() {
  const config = parseArgs(Bun.argv.slice(2));
  const csvFile = Bun.file(config.csvPath);

  if (!(await csvFile.exists())) {
    throw new Error(`CSV file not found: ${config.csvPath}`);
  }

  const csvText = await csvFile.text();
  const parsedRows = parseCsv(csvText).filter((row) =>
    row.some((field) => field.trim().length > 0),
  );

  if (parsedRows.length < 2) {
    throw new Error("CSV does not contain any data rows.");
  }

  const [headers, ...dataRows] = parsedRows;
  const indexes = resolveColumnIndexes(headers);
  const mappedRows: NullableSqlValue[][] = [];
  let skippedRows = 0;

  dataRows.forEach((row) => {
    const mapped = mapRow(row, indexes);
    if (mapped === null) {
      skippedRows += 1;
      return;
    }
    mappedRows.push(mapped);
  });

  if (mappedRows.length === 0) {
    throw new Error("No rows with a valid CNIC were found.");
  }

  const statements = chunk(mappedRows, config.batchSize).map(buildInsertStatement);
  const sql = config.remote
    ? [...statements, ""].join("\n\n")
    : ["BEGIN TRANSACTION;", ...statements, "COMMIT;", ""].join("\n\n");

  await Bun.write(config.outputPath, sql);

  console.log(`CSV rows read: ${dataRows.length}`);
  console.log(`Rows prepared: ${mappedRows.length}`);
  console.log(`Rows skipped without CNIC: ${skippedRows}`);
  console.log(`Insert batches: ${statements.length} (${config.batchSize} rows per batch)`);
  console.log(`SQL file written: ${config.outputPath}`);
  console.log(`Wrangler target: ${config.remote ? "remote" : "local"}`);

  if (config.generateOnly) {
    console.log("Generate-only mode enabled; skipped Wrangler execution.");
    return;
  }

  await executeSqlFile(config);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
