import type { APIContext, APIRoute } from "astro";
import { env } from "cloudflare:workers";
import { requireApprovedAdminJson } from "../../../lib/admin-auth";

type D1RunResult = {
  success: boolean;
  meta: {
    changes?: number;
    [key: string]: unknown;
  };
};

type D1PreparedStatement = {
  bind(...values: unknown[]): D1PreparedStatement;
  first<T = unknown>(): Promise<T | null>;
  run(): Promise<D1RunResult>;
};

type D1Database = {
  prepare(query: string): D1PreparedStatement;
};

type RuntimeLocals = APIContext["locals"] & {
  runtime?: {
    env?: {
      DB?: D1Database;
    };
  };
};

type WorkerEnv = {
  DB?: D1Database;
};

type TargetSubject =
  | "Math"
  | "Islamiat"
  | "Computer Science"
  | "Social Studies";

type SubmissionPatchBody = {
  id?: unknown;
  applied_subject?: unknown;
  custom_passing_year?: unknown;
  custom_percentage?: unknown;
  degree_image_url?: unknown;
};

type SubmissionDeleteBody = {
  id?: unknown;
};

const targetSubjects: TargetSubject[] = [
  "Math",
  "Islamiat",
  "Computer Science",
  "Social Studies",
];

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function getDb(context: APIContext): D1Database {
  const importedEnv = env as WorkerEnv;
  if (importedEnv.DB) {
    return importedEnv.DB;
  }

  try {
    const runtimeDb = (context.locals as RuntimeLocals).runtime?.env?.DB;
    if (runtimeDb) return runtimeDb;
  } catch {
    // Astro v6 Cloudflare locals may throw if runtime.env is accessed directly.
  }

  throw new Error("Cloudflare D1 binding DB is not available.");
}

function parseId(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function cleanOptionalText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "string" && typeof value !== "number") return null;

  const cleaned = String(value).trim();
  return cleaned.length > 0 ? cleaned : null;
}

function normalizeSubject(value: unknown): TargetSubject | null {
  const cleaned = cleanOptionalText(value);
  if (!cleaned) return null;

  return (
    targetSubjects.find(
      (subject) => subject.toLowerCase() === cleaned.toLowerCase(),
    ) ?? null
  );
}

function isValidYear(value: string | null): boolean {
  if (value === null) return true;

  const year = Number.parseInt(value, 10);
  const currentYear = new Date().getFullYear();
  return /^\d{4}$/.test(value) && year >= 1950 && year <= currentYear;
}

function isValidPercentage(value: string | null): boolean {
  if (value === null) return true;

  const normalized = value.replace("%", "").trim();
  const percentage = Number.parseFloat(normalized);
  return Number.isFinite(percentage) && percentage >= 0 && percentage <= 100;
}

function isValidCertificateUrl(value: string | null): boolean {
  if (value === null) return true;

  try {
    const url = new URL(value);
    return url.protocol === "https:";
  } catch {
    return false;
  }
}

async function parseJsonBody<T>(request: Request): Promise<T> {
  const contentType = request.headers.get("content-type") ?? "";
  if (!contentType.toLowerCase().includes("application/json")) {
    throw new SyntaxError("Request body must be JSON.");
  }

  const body = await request.json();
  if (typeof body !== "object" || body === null) {
    throw new SyntaxError("Request body must be a JSON object.");
  }

  return body as T;
}

export const PATCH: APIRoute = async (context) => {
  try {
    const adminAuth = await requireApprovedAdminJson(context);
    if (!adminAuth.authorized) {
      return adminAuth.response;
    }

    const body = await parseJsonBody<SubmissionPatchBody>(context.request);
    const id = parseId(body.id);
    const appliedSubject = normalizeSubject(body.applied_subject);
    const customPassingYear = cleanOptionalText(body.custom_passing_year);
    const customPercentage = cleanOptionalText(body.custom_percentage);
    const degreeImageUrl = cleanOptionalText(body.degree_image_url);

    if (!id) {
      return jsonResponse({ error: "A valid submission id is required." }, 400);
    }

    if (!appliedSubject) {
      return jsonResponse({ error: "A valid applied subject is required." }, 400);
    }

    if (!isValidYear(customPassingYear)) {
      return jsonResponse({ error: "Passing year must be a valid four-digit year." }, 400);
    }

    if (!isValidPercentage(customPercentage)) {
      return jsonResponse({ error: "Percentage must be between 0 and 100." }, 400);
    }

    if (!isValidCertificateUrl(degreeImageUrl)) {
      return jsonResponse({ error: "Certificate URL must be a valid HTTPS URL." }, 400);
    }

    const db = getDb(context);
    const existing = await db
      .prepare("SELECT id FROM pectaa_marking_2026 WHERE id = ? LIMIT 1")
      .bind(id)
      .first<{ id: number }>();

    if (!existing) {
      return jsonResponse({ error: "Submission record not found." }, 404);
    }

    await db
      .prepare(
        `
          UPDATE pectaa_marking_2026
          SET
            applied_subject = ?,
            custom_passing_year = ?,
            custom_percentage = ?,
            degree_image_url = ?
          WHERE id = ?
        `,
      )
      .bind(
        appliedSubject,
        customPassingYear,
        customPercentage,
        degreeImageUrl,
        id,
      )
      .run();

    return jsonResponse({ message: "Submission updated.", id });
  } catch (error) {
    if (error instanceof SyntaxError) {
      return jsonResponse({ error: error.message }, 400);
    }

    console.error("Admin submission update failed:", error);
    return jsonResponse({ error: "Unable to update submission." }, 500);
  }
};

export const DELETE: APIRoute = async (context) => {
  try {
    const adminAuth = await requireApprovedAdminJson(context);
    if (!adminAuth.authorized) {
      return adminAuth.response;
    }

    const body = await parseJsonBody<SubmissionDeleteBody>(context.request);
    const id = parseId(body.id);

    if (!id) {
      return jsonResponse({ error: "A valid submission id is required." }, 400);
    }

    const db = getDb(context);
    const existing = await db
      .prepare("SELECT id FROM pectaa_marking_2026 WHERE id = ? LIMIT 1")
      .bind(id)
      .first<{ id: number }>();

    if (!existing) {
      return jsonResponse({ error: "Submission record not found." }, 404);
    }

    await db
      .prepare("DELETE FROM pectaa_marking_2026 WHERE id = ?")
      .bind(id)
      .run();

    return jsonResponse({ message: "Submission deleted.", id });
  } catch (error) {
    if (error instanceof SyntaxError) {
      return jsonResponse({ error: error.message }, 400);
    }

    console.error("Admin submission delete failed:", error);
    return jsonResponse({ error: "Unable to delete submission." }, 500);
  }
};
