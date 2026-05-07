import fs from "fs";
import path from "path";
import { randomUUID } from "crypto";

export class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = "ValidationError";
  }
}

function parseDelay(delayMs) {
  if (delayMs == null || delayMs === "") {
    return 0;
  }

  const parsed = Number.parseInt(delayMs, 10);

  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new ValidationError("delay_ms must be a non-negative integer");
  }

  return parsed;
}

function parseAttachmentTextMode(value) {
  if (value == null || value === "") {
    return "caption";
  }

  const normalized = String(value).trim().toLowerCase();

  if (normalized === "caption" || normalized === "separate") {
    return normalized;
  }

  throw new ValidationError(
    "attachment_text_mode must be either 'caption' or 'separate'",
  );
}

function parseDocument(input) {
  const rawPath = input.path ?? input.documentPath ?? input.document_path;
  if (!rawPath) {
    throw new ValidationError("document path is required");
  }

  const documentPath = path.resolve(String(rawPath));
  const filename =
    input.filename ?? input.documentFilename ?? input.document_filename;
  const mimetype =
    input.mimetype ?? input.mimeType ?? input.documentMimetype ?? input.document_mimetype;
  const caption = input.caption == null ? null : String(input.caption).trim();

  return {
    caption: caption || null,
    filename: filename ? String(filename).trim() : path.basename(documentPath),
    mimetype: mimetype ? String(mimetype).trim() : "application/octet-stream",
    path: documentPath,
  };
}

function parseDocuments(input) {
  const documents = [];

  if (Array.isArray(input.documents)) {
    for (const item of input.documents) {
      if (!item || typeof item !== "object") {
        throw new ValidationError("documents entries must be objects");
      }
      documents.push(parseDocument(item));
    }
  }

  const singleDocumentPath = input.documentPath ?? input.document_path;
  if (singleDocumentPath) {
    documents.push(
      parseDocument({
        path: singleDocumentPath,
        filename: input.documentFilename ?? input.document_filename,
        mimetype: input.documentMimetype ?? input.document_mimetype,
      }),
    );
  }

  return documents;
}

function normalizeContactTarget(target) {
  if (target.endsWith("@s.whatsapp.net")) {
    return target;
  }

  const digits = target.replace(/\D/g, "");

  if (!digits) {
    throw new ValidationError(`Invalid contact target: ${target}`);
  }

  return `${digits}@s.whatsapp.net`;
}

function normalizeGroupTarget(target) {
  if (!target.endsWith("@g.us")) {
    throw new ValidationError(
      `Group targets must be full WhatsApp group JIDs ending in @g.us: ${target}`,
    );
  }

  return target;
}

function inferType(target) {
  if (target.endsWith("@g.us")) {
    return "group";
  }

  return "contact";
}

export function normalizeTarget(target, type) {
  const trimmed = String(target || "").trim();

  if (!trimmed) {
    throw new ValidationError("target is required");
  }

  const normalizedType = type || inferType(trimmed);

  if (normalizedType === "group") {
    return {
      type: normalizedType,
      target: normalizeGroupTarget(trimmed),
    };
  }

  if (normalizedType === "contact") {
    return {
      type: normalizedType,
      target: normalizeContactTarget(trimmed),
    };
  }

  throw new ValidationError(`Unsupported recipient type: ${normalizedType}`);
}

export function createJobPayload(input) {
  const { target, type } = normalizeTarget(input.target, input.type);
  const attachmentTextMode = parseAttachmentTextMode(
    input.attachmentTextMode ?? input.attachment_text_mode,
  );
  const text = input.text ? String(input.text).trim() : null;
  const imagePath = input.imagePath ? path.resolve(input.imagePath) : null;
  const excelPath = input.excelPath ? path.resolve(input.excelPath) : null;
  const excelFilename = input.excelFilename
    ? String(input.excelFilename).trim()
    : excelPath
      ? path.basename(excelPath)
      : null;
  const documents = parseDocuments(input);
  const delayMs = parseDelay(input.delayMs);

  if (!text && !imagePath && !excelPath && documents.length === 0) {
    throw new ValidationError(
      `Job for ${target} must include text, imagePath, excelPath, documentPath, or documents`,
    );
  }

  return {
    attachmentTextMode,
    delayMs,
    documents,
    excelFilename,
    excelPath,
    imagePath,
    jobId: input.jobId || randomUUID(),
    recipientName: input.recipientName ? String(input.recipientName) : null,
    target,
    text,
    type,
  };
}

export function resolveJobPayload(raw) {
  if (!raw || typeof raw !== "object") {
    throw new ValidationError("Queue payload must be a JSON object");
  }

  return createJobPayload({
    attachmentTextMode:
      raw.attachment_text_mode ?? raw.attachmentTextMode,
    delayMs: raw.delay_ms ?? raw.delayMs,
    excelFilename: raw.excel_filename ?? raw.excelFilename,
    excelPath: raw.excel_path ?? raw.excelPath,
    imagePath: raw.image_path ?? raw.imagePath,
    documentFilename: raw.document_filename ?? raw.documentFilename,
    documentMimetype: raw.document_mimetype ?? raw.documentMimetype,
    documentPath: raw.document_path ?? raw.documentPath,
    documents: raw.documents,
    jobId: raw.job_id ?? raw.jobId,
    recipientName: raw.recipient_name ?? raw.recipientName,
    target: raw.target,
    text: raw.text,
    type: raw.type,
  });
}

export function assertFilesExist(job) {
  const missing = [];

  if (job.imagePath && !fs.existsSync(job.imagePath)) {
    missing.push(job.imagePath);
  }

  if (job.excelPath && !fs.existsSync(job.excelPath)) {
    missing.push(job.excelPath);
  }

  for (const document of job.documents || []) {
    if (document.path && !fs.existsSync(document.path)) {
      missing.push(document.path);
    }
  }

  if (missing.length > 0) {
    throw new ValidationError(`Missing file(s): ${missing.join(", ")}`);
  }
}

export function renderTemplate(template, variables) {
  return String(template).replaceAll(
    /\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g,
    (_, key) => String(variables[key] ?? ""),
  );
}
