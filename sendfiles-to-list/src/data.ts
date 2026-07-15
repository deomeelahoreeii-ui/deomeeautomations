import * as fs from "fs";
import * as path from "path";
import xlsx from "xlsx";
import { PATHS } from "./config.js";

// Ensure directories exist
Object.values(PATHS).forEach((dir) => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

export function getContacts(): any[] {
  const files = fs.readdirSync(PATHS.CONTACTS);
  const contactFile = files.find(
    (f) => f.endsWith(".xlsx") || f.endsWith(".csv"),
  );

  if (!contactFile) {
    console.error("❌ No .xlsx or .csv file found in contacts/ directory.");
    return [];
  }

  console.log(`📄 Using contacts file: ${contactFile}`);
  const workbook = xlsx.readFile(path.join(PATHS.CONTACTS, contactFile));
  const sheetName = workbook.SheetNames[0];

  return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
}

export function getFilesToSend(): string[] {
  return fs
    .readdirSync(PATHS.FILES)
    .map((file) => path.join(PATHS.FILES, file))
    .filter((filePath) => fs.statSync(filePath).isFile());
}

export function getBroadcastMessage(): string | null {
  const files = fs.readdirSync(PATHS.MESSAGES);
  const mdFile = files.find((f) => f.endsWith(".md") || f.endsWith(".txt"));

  if (!mdFile) return null;

  console.log(`📝 Loaded markdown message from: ${mdFile}`);
  return fs.readFileSync(path.join(PATHS.MESSAGES, mdFile), "utf-8");
}
