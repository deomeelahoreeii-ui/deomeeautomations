import type { APIRoute } from "astro";
import { requireApprovedAdminJson } from "../../../lib/admin-auth";
import { getAeoDistribution } from "../../../lib/admin-aeo-distribution";
import { rowsToCsv } from "../../../lib/csv";
import { getDb } from "../../../lib/d1";
import { jsonResponse } from "../../../lib/http";

export const prerender = false;

type AeoExportRow = {
  sr_no: number;
  aeo_name: string;
  reporting_wing: string;
  submitted: number;
  share_percent: string;
  students: number;
  teachers: number;
  registered: number;
  attention_records: number;
  facility_gap_records: number;
  issue_records: number;
  tehsils: string;
};

const aeoExportColumns = [
  { key: "sr_no", label: "Sr. No." },
  { key: "aeo_name", label: "AEO Name" },
  { key: "reporting_wing", label: "Wing" },
  { key: "submitted", label: "Submitted Records" },
  { key: "share_percent", label: "Share %" },
  { key: "students", label: "Students" },
  { key: "teachers", label: "Teachers" },
  { key: "registered", label: "Registered" },
  { key: "attention_records", label: "Unregistered/Expired" },
  { key: "facility_gap_records", label: "Facility Gap Records" },
  { key: "issue_records", label: "Total Issue Signals" },
  { key: "tehsils", label: "Tehsils" },
] satisfies Array<{ key: keyof AeoExportRow; label: string }>;

function displayValue(value: unknown): string {
  const cleaned = String(value ?? "").trim();
  return cleaned.length > 0 ? cleaned : "Not available";
}

function toNumber(value: unknown): number {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number : 0;
}

export const GET: APIRoute = async (context) => {
  try {
    const adminAuth = await requireApprovedAdminJson(context);
    if (!adminAuth.authorized) {
      return adminAuth.response;
    }

    const url = new URL(context.request.url);
    if (url.searchParams.get("format") !== "csv") {
      return jsonResponse({ error: "Unsupported export format." }, 400);
    }

    const db = getDb(context);
    const rows = await getAeoDistribution(db);
    const totalSubmissions = rows.reduce((sum, row) => sum + toNumber(row.total), 0);
    const exportRows = rows.map<AeoExportRow>((row, index) => {
      const submitted = toNumber(row.total);
      const attentionRecords = toNumber(row.attention_records);
      const facilityGapRecords = toNumber(row.facility_gap_records);
      const share = totalSubmissions > 0 ? (submitted / totalSubmissions) * 100 : 0;

      return {
        sr_no: index + 1,
        aeo_name: displayValue(row.aeo_name),
        reporting_wing: displayValue(row.reporting_wing),
        submitted,
        share_percent: share.toFixed(1),
        students: toNumber(row.students),
        teachers: toNumber(row.teachers),
        registered: toNumber(row.registered),
        attention_records: attentionRecords,
        facility_gap_records: facilityGapRecords,
        issue_records: attentionRecords + facilityGapRecords,
        tehsils: displayValue(row.tehsils),
      };
    });
    const csv = rowsToCsv(aeoExportColumns, exportRows);

    return new Response(csv, {
      headers: {
        "content-disposition": 'attachment; filename="PRIVATE_INSTITUTES_AEO_DISTRIBUTION_JULY_2026.csv"',
        "content-type": "text/csv;charset=utf-8",
      },
    });
  } catch (error) {
    console.error("AEO distribution export failed:", error);
    return jsonResponse({ error: "Unable to export AEO distribution." }, 500);
  }
};
