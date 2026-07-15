import type { APIContext, APIRoute } from "astro";
import { env } from "cloudflare:workers";

type TeacherRecord = {
  cnic: string;
  teacher_name: string | null;
  mobile_no: string | null;
  gender: string | null;
  designation: string | null;
  subject: string | null;
  grade: number | null;
  cadre: string | null;
  employment_type: string | null;
  degree_level: string | null;
  degree_subject: string | null;
  date_joining_service: string | null;
  date_joining_school: string | null;
  date_joining_post: string | null;
  status: string | null;
  district: string | null;
  tehsil: string | null;
  markaz: string | null;
  emis: number | null;
  school: string | null;
  school_level: string | null;
  personal_no: string | null;
  age: string | null;
};

type TeacherLookupResponse = TeacherRecord & {
  is_auto_match: boolean;
  matched_subject: TargetSubject | null;
  has_existing_nomination: boolean;
  existing_submission_timestamp: string | null;
};

type TargetSubject =
  | "Math"
  | "Islamiat"
  | "Computer Science"
  | "Social Studies";

type NominationBody = {
  cnic?: unknown;
  applied_subject?: unknown;
  custom_passing_year?: unknown;
  custom_percentage?: unknown;
  degree_image_base64?: unknown;
  degree_file_name?: unknown;
  file_label?: unknown;
};

type D1RunResult = {
  success: boolean;
  meta: Record<string, unknown>;
};

type D1PreparedStatement = {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T = unknown>(): Promise<T | null>;
  run(): Promise<D1RunResult>;
};

type D1Database = {
  prepare(query: string): D1PreparedStatement;
};

type CloudflareRuntime = {
  env: {
    DB: D1Database;
    IMAGEKIT_PUBLIC_KEY?: string;
    IMAGEKIT_PRIVATE_KEY?: string;
    IMAGEKIT_URL_ENDPOINT?: string;
  };
};

type RuntimeLocals = APIContext["locals"] & {
  runtime?: CloudflareRuntime;
};

type CloudflareWorkerEnv = {
  DB?: D1Database;
  IMAGEKIT_PUBLIC_KEY?: string;
  IMAGEKIT_PRIVATE_KEY?: string;
  IMAGEKIT_URL_ENDPOINT?: string;
};

type ImageKitUploadResponse = {
  url?: string;
  message?: string;
};

type NominationRecord = {
  timestamp: string | null;
};

const targetSubjects: TargetSubject[] = [
  "Math",
  "Islamiat",
  "Computer Science",
  "Social Studies",
];

const teacherSelectSql = `
  SELECT
    cnic,
    teacher_name,
    mobile_no,
    gender,
    designation,
    subject,
    grade,
    cadre,
    employment_type,
    degree_level,
    degree_subject,
    date_joining_service,
    date_joining_school,
    date_joining_post,
    status,
    district,
    tehsil,
    markaz,
    emis,
    school,
    school_level,
    personal_no,
    age
  FROM teachers
  WHERE cnic = ?
  LIMIT 1
`;

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function getRuntimeEnv(context: APIContext): CloudflareWorkerEnv {
  const importedEnv = env as CloudflareWorkerEnv;
  if (importedEnv.DB) {
    return importedEnv;
  }

  try {
    return ((context.locals as RuntimeLocals).runtime?.env ?? {}) as CloudflareWorkerEnv;
  } catch {
    return {};
  }
}

function getDb(context: APIContext): D1Database {
  const db = getRuntimeEnv(context).DB;
  if (!db) {
    throw new Error("Cloudflare D1 binding DB is not available.");
  }
  return db;
}

function normalizeCnic(value: unknown): string | null {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }

  const cnic = String(value).replace(/[\s-]/g, "").trim();
  return /^\d{13}$/.test(cnic) ? cnic : null;
}

function cleanText(value: unknown): string | null {
  if (typeof value !== "string" && typeof value !== "number") {
    return null;
  }

  const cleaned = String(value).trim();
  return cleaned.length > 0 ? cleaned : null;
}

function normalizeAppliedSubject(value: unknown): TargetSubject | null {
  const cleaned = cleanText(value);
  if (!cleaned) return null;

  const matched = targetSubjects.find(
    (subject) => subject.toLowerCase() === cleaned.toLowerCase(),
  );
  return matched ?? null;
}

function getMatchedSubject(teacher: TeacherRecord): TargetSubject | null {
  const source = `${teacher.degree_level ?? ""} ${teacher.degree_subject ?? ""}`.toLowerCase();

  if (/\b(computer\s*science|cs)\b/.test(source)) return "Computer Science";
  if (/\b(math|mathematics)\b/.test(source)) return "Math";
  if (/\b(islamiat|islamic\s*studies)\b/.test(source)) return "Islamiat";
  if (/\b(social\s*studies|pak\s*studies)\b/.test(source)) return "Social Studies";

  return null;
}

function isValidYear(value: string | null): boolean {
  if (!value) return false;
  const year = Number.parseInt(value, 10);
  const currentYear = new Date().getFullYear();
  return /^\d{4}$/.test(value) && year >= 1950 && year <= currentYear;
}

function isValidPercentage(value: string | null): boolean {
  if (!value) return false;
  const normalized = value.replace("%", "").trim();
  const percentage = Number.parseFloat(normalized);
  return Number.isFinite(percentage) && percentage >= 0 && percentage <= 100;
}

function sanitizeFileName(fileName: string, cnic: string): string {
  const cleaned = fileName
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .replace(/_{2,}/g, "_")
    .slice(0, 120);

  if (cleaned.includes(".")) {
    return `${cnic}-${cleaned}`;
  }

  return `${cnic}-${cleaned || "degree-certificate"}.jpg`;
}

function validateBase64Image(value: string | null): string | null {
  if (!value) return null;

  const normalized = value.trim();
  const hasDataUrlPrefix = /^data:image\/(png|jpe?g|webp);base64,/i.test(normalized);
  const payload = hasDataUrlPrefix ? normalized.split(",", 2)[1] : normalized;
  const approximateBytes = Math.ceil((payload.length * 3) / 4);

  if (approximateBytes > 5 * 1024 * 1024) {
    throw new RangeError("Degree image must be 5 MB or smaller.");
  }

  if (!/^[a-zA-Z0-9+/]+={0,2}$/.test(payload)) {
    return null;
  }

  return hasDataUrlPrefix ? normalized : payload;
}

function getImageKitConfig(context: APIContext): {
  publicKey: string;
  privateKey: string;
  urlEndpoint: string;
} {
  const runtimeEnv = getRuntimeEnv(context);
  const publicKey = cleanText(runtimeEnv.IMAGEKIT_PUBLIC_KEY);
  const privateKey = cleanText(runtimeEnv.IMAGEKIT_PRIVATE_KEY);
  const urlEndpoint = cleanText(runtimeEnv.IMAGEKIT_URL_ENDPOINT);

  if (!publicKey || !privateKey || !urlEndpoint) {
    throw new Error("ImageKit environment variables are not configured.");
  }

  return {
    publicKey,
    privateKey,
    urlEndpoint,
  };
}

async function uploadDegreeImage(
  context: APIContext,
  cnic: string,
  base64Image: string,
  fileName: string,
): Promise<string> {
  const imagekit = getImageKitConfig(context);
  const formData = new FormData();
  formData.append("file", base64Image);
  formData.append("fileName", sanitizeFileName(fileName, cnic));
  formData.append("folder", "/pectaa-marking-2026");
  formData.append("useUniqueFileName", "true");
  formData.append("tags", `pectaa-marking-2026,${cnic}`);
  formData.append("isPublished", "true");

  const uploadResponse = await fetch("https://upload.imagekit.io/api/v1/files/upload", {
    method: "POST",
    headers: {
      authorization: `Basic ${btoa(`${imagekit.privateKey}:`)}`,
    },
    body: formData,
  });

  const uploadResult = (await uploadResponse.json()) as ImageKitUploadResponse;

  if (!uploadResponse.ok) {
    throw new Error(uploadResult.message ?? "ImageKit upload failed.");
  }

  if (!uploadResult.url) {
    throw new Error("ImageKit upload did not return a URL.");
  }

  return uploadResult.url;
}

function isUniqueConstraintError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const message = error.message.toLowerCase();
  return (
    message.includes("unique constraint failed") ||
    message.includes("sqlite_constraint") ||
    message.includes("constraint failed")
  );
}

async function parseJsonBody(request: Request): Promise<NominationBody> {
  const contentType = request.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new SyntaxError("Request body must be JSON.");
  }

  const body = await request.json();
  if (typeof body !== "object" || body === null) {
    throw new SyntaxError("Request body must be a JSON object.");
  }

  return body as NominationBody;
}

async function getTeacherByCnic(db: D1Database, cnic: string): Promise<TeacherRecord | null> {
  return db.prepare(teacherSelectSql).bind(cnic).first<TeacherRecord>();
}

async function getNominationByCnic(
  db: D1Database,
  cnic: string,
): Promise<NominationRecord | null> {
  return db
    .prepare(
      `
        SELECT timestamp
        FROM pectaa_marking_2026
        WHERE cnic = ?
        LIMIT 1
      `,
    )
    .bind(cnic)
    .first<NominationRecord>();
}

export const GET: APIRoute = async (context) => {
  try {
    const cnic = normalizeCnic(context.url.searchParams.get("cnic"));
    if (!cnic) {
      return jsonResponse({ error: "A valid 13-digit CNIC query parameter is required." }, 400);
    }

    const db = getDb(context);
    const teacher = await getTeacherByCnic(db, cnic);

    if (!teacher) {
      return jsonResponse({ error: "Teacher not found." }, 404);
    }

    const matchedSubject = getMatchedSubject(teacher);
    const nomination = await getNominationByCnic(db, cnic);
    const response: TeacherLookupResponse = {
      ...teacher,
      is_auto_match: matchedSubject !== null,
      matched_subject: matchedSubject,
      has_existing_nomination: nomination !== null,
      existing_submission_timestamp: nomination?.timestamp ?? null,
    };

    return jsonResponse(response);
  } catch (error) {
    console.error("Teacher lookup failed:", error);
    return jsonResponse({ error: "Unable to fetch teacher." }, 500);
  }
};

export const POST: APIRoute = async (context) => {
  try {
    const body = await parseJsonBody(context.request);
    const cnic = normalizeCnic(body.cnic);

    if (!cnic) {
      return jsonResponse({ error: "A valid 13-digit CNIC is required." }, 400);
    }

    const db = getDb(context);
    const teacher = await getTeacherByCnic(db, cnic);

    if (!teacher) {
      return jsonResponse({ error: "Teacher not found." }, 404);
    }

    const autoMatchedSubject = getMatchedSubject(teacher);
    const requestedSubject = normalizeAppliedSubject(body.applied_subject);
    const appliedSubject = requestedSubject ?? autoMatchedSubject;

    if (!appliedSubject) {
      return jsonResponse({ error: "A valid target subject is required." }, 400);
    }

    let customPassingYear: string | null = null;
    let customPercentage: string | null = null;
    let degreeImageUrl: string | null = null;

    if (!autoMatchedSubject) {
      customPassingYear = cleanText(body.custom_passing_year);
      customPercentage = cleanText(body.custom_percentage);
      const degreeImageBase64 = validateBase64Image(cleanText(body.degree_image_base64));
      const fileName = cleanText(body.degree_file_name) ?? cleanText(body.file_label);

      if (!isValidYear(customPassingYear)) {
        return jsonResponse({ error: "A valid graduation passing year is required." }, 400);
      }

      if (!isValidPercentage(customPercentage)) {
        return jsonResponse({ error: "A valid percentage between 0 and 100 is required." }, 400);
      }

      if (!degreeImageBase64 || !fileName) {
        return jsonResponse({ error: "Degree certificate image and file name are required." }, 400);
      }

      degreeImageUrl = await uploadDegreeImage(context, cnic, degreeImageBase64, fileName);
    }

    await db
      .prepare(
        `
          INSERT INTO pectaa_marking_2026 (
            cnic,
            applied_subject,
            custom_passing_year,
            custom_percentage,
            degree_image_url
          )
          VALUES (?, ?, ?, ?, ?)
        `,
      )
      .bind(cnic, appliedSubject, customPassingYear, customPercentage, degreeImageUrl)
      .run();

    return jsonResponse({
      message: "Nomination submitted.",
      applied_subject: appliedSubject,
      degree_image_url: degreeImageUrl,
    });
  } catch (error) {
    if (error instanceof SyntaxError || error instanceof RangeError) {
      return jsonResponse({ error: error.message }, 400);
    }

    if (isUniqueConstraintError(error)) {
      return jsonResponse({ error: "Nomination already submitted." }, 409);
    }

    console.error("Nomination submission failed:", error);
    return jsonResponse({ error: "Unable to submit nomination." }, 500);
  }
};
