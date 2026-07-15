import type { APIContext, APIRoute } from "astro";
import { insertColumns } from "../../config/survey";
import { getDb } from "../../lib/d1";
import { jsonResponse, parseJsonBody } from "../../lib/http";
import { uploadSurveyImage, validateBase64Image } from "../../lib/imagekit";
import { normalizeSurvey, type SurveyBody } from "../../lib/survey-validation";

export const prerender = false;

const insertSql = `
  INSERT INTO private_institutes_survey_2026 (
    ${insertColumns.join(",\n    ")}
  )
  VALUES (${insertColumns.map(() => "?").join(", ")})
`;

function cleanText(value: unknown): string | null {
  if (typeof value !== "string" && typeof value !== "number") return null;

  const cleaned = String(value).trim();
  return cleaned.length > 0 ? cleaned : null;
}

function slugify(value: unknown): string {
  const cleaned = String(value ?? "survey")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64);

  return cleaned || "survey";
}

async function uploadRequiredSurveyImage(
  context: APIContext,
  body: SurveyBody,
  fieldName: string,
  label: string,
  prefix: string,
): Promise<string> {
  const rawImage = cleanText(body[`${fieldName}_base64`]);
  if (!rawImage) return "";

  const base64Image = validateBase64Image(rawImage, label);
  const fileName = cleanText(body[`${fieldName}_file_name`]) ?? `${fieldName}.jpg`;

  return uploadSurveyImage(context, base64Image, fileName, `${prefix}-${fieldName}`);
}

export const POST: APIRoute = async (context) => {
  try {
    const body = await parseJsonBody<SurveyBody>(context.request);
    const imagePrefix = `${slugify(body.institution_name)}-${Date.now()}`;
    body.outside_main_gate_image_url = await uploadRequiredSurveyImage(
      context,
      body,
      "outside_main_gate_image_url",
      "Outside Main Gate Image",
      imagePrefix,
    );
    body.inside_institute_image_url = await uploadRequiredSurveyImage(
      context,
      body,
      "inside_institute_image_url",
      "Inside Institute Image",
      imagePrefix,
    );

    const survey = normalizeSurvey(body);
    const db = getDb(context);

    await db
      .prepare(insertSql)
      .bind(...insertColumns.map((column) => survey[column]))
      .run();

    return jsonResponse({
      message: "Survey submitted.",
      institution_name: survey.institution_name,
    });
  } catch (error) {
    if (error instanceof SyntaxError || error instanceof RangeError) {
      return jsonResponse({ error: error.message }, 400);
    }

    console.error("Survey submission failed:", error);
    return jsonResponse({ error: "Unable to submit survey." }, 500);
  }
};
