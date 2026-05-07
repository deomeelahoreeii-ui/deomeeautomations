import fs from "fs";
import path from "path";
import { parse } from "csv-parse/sync";

function escapeCsv(value) {
  const text = String(value ?? "");
  return `"${text.replaceAll('"', '""')}"`;
}

export function createGroupRegistry(filePath) {
  let isLoaded = false;
  const knownGroups = new Set();

  function ensureLoaded() {
    if (isLoaded) {
      return;
    }

    fs.mkdirSync(path.dirname(filePath), { recursive: true });

    if (!fs.existsSync(filePath)) {
      fs.writeFileSync(filePath, "discovered_at,group_id,group_name\n", "utf8");
      isLoaded = true;
      return;
    }

    const content = fs.readFileSync(filePath, "utf8");

    if (content.trim()) {
      const rows = parse(content, {
        bom: true,
        columns: true,
        skip_empty_lines: true,
        trim: true,
      });

      for (const row of rows) {
        if (row.group_id) {
          knownGroups.add(row.group_id);
        }
      }
    }

    isLoaded = true;
  }

  return {
    has(groupId) {
      ensureLoaded();
      return knownGroups.has(groupId);
    },
    record(entry) {
      ensureLoaded();

      if (!entry?.groupId || knownGroups.has(entry.groupId)) {
        return false;
      }

      knownGroups.add(entry.groupId);

      const line = [
        escapeCsv(entry.discoveredAt || new Date().toISOString()),
        escapeCsv(entry.groupId),
        escapeCsv(entry.groupName || ""),
      ].join(",");

      fs.appendFileSync(filePath, `${line}\n`, "utf8");
      return true;
    },
  };
}
