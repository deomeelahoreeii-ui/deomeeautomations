import fs from "fs";
import path from "path";

const authDir = path.resolve(process.cwd(), process.env.WA_AUTH_DIR || "auth_info_baileys");
const phones = process.argv.slice(2).map((value) => value.replace(/\D/g, "")).filter(Boolean);

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch {
    return null;
  }
}

function tokenLength(entry) {
  const data = entry?.token?.data;
  if (Array.isArray(data)) {
    return data.length;
  }
  if (typeof data === "string") {
    return data.length;
  }
  return 0;
}

function isExpired(timestamp) {
  const parsed = Number(timestamp);
  if (!Number.isFinite(parsed)) {
    return true;
  }
  const bucketDuration = 604800;
  const bucketCount = 4;
  const currentBucket = Math.floor(Date.now() / 1000 / bucketDuration);
  const cutoffTimestamp = (currentBucket - (bucketCount - 1)) * bucketDuration;
  return parsed < cutoffTimestamp;
}

function lidForPhone(phone) {
  const mappingPath = path.join(authDir, `lid-mapping-${phone}.json`);
  const mapped = readJson(mappingPath);
  if (typeof mapped === "string" && mapped.endsWith("@lid")) {
    return mapped;
  }

  for (const filename of fs.readdirSync(authDir)) {
    const match = filename.match(/^lid-mapping-(\d+)_reverse\.json$/);
    if (!match) {
      continue;
    }
    const reverse = String(readJson(path.join(authDir, filename)) || "").replace(/\D/g, "");
    if (reverse === phone) {
      return `${match[1]}@lid`;
    }
  }

  return null;
}

function report(phone) {
  const lid = lidForPhone(phone);
  if (!lid) {
    return {
      phone,
      lid: "",
      status: "missing_lid_mapping",
      tokenLength: 0,
      timestamp: "",
    };
  }

  const tokenPath = path.join(authDir, `tctoken-${lid}.json`);
  const entry = readJson(tokenPath);
  const length = tokenLength(entry);
  const timestamp = entry?.timestamp || "";
  const expired = length > 0 ? isExpired(timestamp) : "";
  return {
    phone,
    lid,
    status: length > 0 && expired === false ? "usable" : "missing_or_expired",
    tokenLength: length,
    timestamp,
    senderTimestamp: entry?.senderTimestamp || "",
    expired,
    tokenPath,
  };
}

if (!fs.existsSync(authDir)) {
  console.error(`Auth directory not found: ${authDir}`);
  process.exit(1);
}

if (phones.length === 0) {
  console.error("Usage: node scripts/check-tctokens.mjs 923005363198 923331461236 ...");
  process.exit(1);
}

for (const row of phones.map(report)) {
  console.log(JSON.stringify(row));
}
