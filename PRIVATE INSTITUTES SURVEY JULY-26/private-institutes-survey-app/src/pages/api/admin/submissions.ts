import type { APIRoute } from "astro";
import { csvColumns, insertColumns } from "../../../config/survey";
import { requireApprovedAdminJson } from "../../../lib/admin-auth";
import { buildSubmissionsWhere, parseSubmissionFilters } from "../../../lib/admin-submission-query";
import { rowsToCsv } from "../../../lib/csv";
import { getDb } from "../../../lib/d1";
import { jsonResponse, parseJsonBody } from "../../../lib/http";
import { normalizeSurvey, type SurveyBody } from "../../../lib/survey-validation";

export const prerender = false;

type SubmissionPatchBody = SurveyBody & {
  id?: unknown;
};

type SubmissionDeleteBody = {
  id?: unknown;
};

function parseId(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function toCsv(records: Record<string, unknown>[]): string {
  const exportRows = records.map((record, index) => {
    const exportRecord: Record<string, unknown> = {
      sr_no: index + 1,
      ...record,
    };

    return exportRecord;
  });

  return rowsToCsv(csvColumns, exportRows);
}

export const GET: APIRoute = async (context) => {
  try {
    const adminAuth = await requireApprovedAdminJson(context);
    if (!adminAuth.authorized) {
      return adminAuth.response;
    }

    const url = new URL(context.request.url);
    const format = url.searchParams.get("format");

    if (format !== "csv") {
      return jsonResponse({ error: "Unsupported export format." }, 400);
    }

    const filters = parseSubmissionFilters(url.searchParams);
    const filterQuery = buildSubmissionsWhere(filters);
    const db = getDb(context);
    const statement = db.prepare(`
      SELECT *
      FROM private_institutes_survey_2026
      ${filterQuery.whereClause}
      ORDER BY submitted_at DESC, id DESC
    `);
    const result = filterQuery.params.length > 0
      ? await statement.bind(...filterQuery.params).all<Record<string, unknown>>()
      : await statement.all<Record<string, unknown>>();
    const csv = toCsv(result.results ?? []);

    return new Response(csv, {
      headers: {
        "content-disposition": 'attachment; filename="PRIVATE_INSTITUTES_SURVEY_JULY_2026.csv"',
        "content-type": "text/csv;charset=utf-8",
      },
    });
  } catch (error) {
    console.error("Admin survey export failed:", error);
    return jsonResponse({ error: "Unable to export survey records." }, 500);
  }
};

export const PATCH: APIRoute = async (context) => {
  try {
    const adminAuth = await requireApprovedAdminJson(context);
    if (!adminAuth.authorized) {
      return adminAuth.response;
    }

    const body = await parseJsonBody<SubmissionPatchBody>(context.request);
    const id = parseId(body.id);

    if (!id) {
      return jsonResponse({ error: "A valid submission id is required." }, 400);
    }

    const survey = normalizeSurvey(body);
    const db = getDb(context);
    const existing = await db
      .prepare("SELECT id FROM private_institutes_survey_2026 WHERE id = ? LIMIT 1")
      .bind(id)
      .first<{ id: number }>();

    if (!existing) {
      return jsonResponse({ error: "Survey record not found." }, 404);
    }

    await db
      .prepare(
        `
          UPDATE private_institutes_survey_2026
          SET ${insertColumns.map((column) => `${column} = ?`).join(",\n              ")}
          WHERE id = ?
        `,
      )
      .bind(...insertColumns.map((column) => survey[column]), id)
      .run();

    return jsonResponse({ message: "Survey record updated.", id });
  } catch (error) {
    if (error instanceof SyntaxError || error instanceof RangeError) {
      return jsonResponse({ error: error.message }, 400);
    }

    console.error("Admin survey update failed:", error);
    return jsonResponse({ error: "Unable to update survey record." }, 500);
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
      .prepare("SELECT id FROM private_institutes_survey_2026 WHERE id = ? LIMIT 1")
      .bind(id)
      .first<{ id: number }>();

    if (!existing) {
      return jsonResponse({ error: "Survey record not found." }, 404);
    }

    await db
      .prepare("DELETE FROM private_institutes_survey_2026 WHERE id = ?")
      .bind(id)
      .run();

    return jsonResponse({ message: "Survey record deleted.", id });
  } catch (error) {
    if (error instanceof SyntaxError) {
      return jsonResponse({ error: error.message }, 400);
    }

    console.error("Admin survey delete failed:", error);
    return jsonResponse({ error: "Unable to delete survey record." }, 500);
  }
};
