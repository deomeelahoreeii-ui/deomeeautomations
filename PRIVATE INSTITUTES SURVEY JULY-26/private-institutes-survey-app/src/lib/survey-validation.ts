import { fieldLabels, surveyFields } from "../config/survey";

export type SurveyBody = Record<string, unknown>;
export type NormalizedSurvey = Record<string, string | number | null>;

function cleanText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string" && typeof value !== "number") return null;

  const cleaned = String(value).trim();
  return cleaned.length > 0 ? cleaned : null;
}

function normalizeNumber(value: unknown, fieldName: string, min = 0): number {
  const cleaned = cleanText(value);
  if (!cleaned) return 0;

  const number = Number(cleaned);

  if (!Number.isInteger(number) || number < min) {
    throw new RangeError(`${fieldLabels[fieldName] ?? fieldName} must be ${min} or a greater whole number.`);
  }

  return number;
}

function validateChoice(
  value: string | null,
  fieldName: string,
  options: readonly string[],
): string {
  if (!value) {
    throw new RangeError(`${fieldLabels[fieldName] ?? fieldName} is required.`);
  }

  const matched = options.find((option) => option.toLowerCase() === value?.toLowerCase());

  if (!matched) {
    throw new RangeError(`${fieldLabels[fieldName] ?? fieldName} has an invalid value.`);
  }

  return matched;
}

function validateEmail(value: string | null, fieldName: string): string | null {
  if (!value) return null;

  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)) {
    throw new RangeError(`${fieldLabels[fieldName] ?? fieldName} must be valid.`);
  }

  return value;
}

function validateHttpsUrl(value: string | null, fieldName: string): string {
  if (!value) {
    throw new RangeError(`${fieldLabels[fieldName] ?? fieldName} is required.`);
  }

  try {
    const url = new URL(value);
    if (url.protocol !== "https:") {
      throw new Error("Invalid protocol.");
    }

    return url.toString();
  } catch {
    throw new RangeError(`${fieldLabels[fieldName] ?? fieldName} must be a valid HTTPS URL.`);
  }
}

const zonalHeadFieldNames = new Set([
  "zonal_head_institute_name",
  "zonal_head_name",
  "zonal_head_contact_no",
]);
const seSubmittedByOptions = ["Zonal Head", "AEO"] as const;
const seAeoWingOptions = ["MEE", "WEE"] as const;

export function normalizeSurvey(body: SurveyBody): NormalizedSurvey {
  const normalized: NormalizedSurvey = {};
  const selectedWing = cleanText(body.wing);
  const isSeWing = selectedWing?.toUpperCase() === "SE";
  const seSubmittedBy = cleanText(body.se_submitted_by);
  const seAeoWing = cleanText(body.se_aeo_wing);
  const isSeSubmittedByZonalHead = seSubmittedBy?.toLowerCase() === "zonal head";
  const isSeSubmittedByAeo = seSubmittedBy?.toLowerCase() === "aeo";

  for (const field of surveyFields) {
    const value = cleanText(body[field.name]);

    if (field.name === "se_submitted_by") {
      normalized[field.name] = isSeWing
        ? validateChoice(seSubmittedBy, field.name, seSubmittedByOptions)
        : "Not applicable";
      continue;
    }

    if (field.name === "se_aeo_wing") {
      normalized[field.name] = isSeWing && isSeSubmittedByAeo
        ? validateChoice(seAeoWing, field.name, seAeoWingOptions)
        : "Not applicable";
      continue;
    }

    if (field.name === "aeo_name" && isSeWing && isSeSubmittedByZonalHead) {
      normalized[field.name] = "SE Zonal Head";
      continue;
    }

    if (zonalHeadFieldNames.has(field.name) && (!isSeWing || !isSeSubmittedByZonalHead)) {
      normalized[field.name] = "Not applicable";
      continue;
    }

    if (field.required && !value) {
      throw new RangeError(`${field.label} is required.`);
    }

    if (field.kind === "number") {
      normalized[field.name] = normalizeNumber(body[field.name], field.name, field.min ?? 0);
      continue;
    }

    if (!value) {
      normalized[field.name] = "";
      continue;
    }

    if (field.kind === "select" || field.kind === "yesNo") {
      normalized[field.name] = validateChoice(value, field.name, field.options ?? []);
      continue;
    }

    if (field.kind === "email") {
      normalized[field.name] = validateEmail(value, field.name);
      continue;
    }

    if (field.kind === "image") {
      normalized[field.name] = validateHttpsUrl(value, field.name);
      continue;
    }

    normalized[field.name] = value;
  }

  return normalized;
}
