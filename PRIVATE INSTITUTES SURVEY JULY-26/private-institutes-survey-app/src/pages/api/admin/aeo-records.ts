import type { APIRoute } from "astro";
import { requireApprovedAdminJson } from "../../../lib/admin-auth";
import { getAeoRecords, parseAeoRecordFilter, type AeoRecordRow } from "../../../lib/admin-aeo-distribution";
import { rowsToCsv } from "../../../lib/csv";
import { getDb } from "../../../lib/d1";
import { jsonResponse } from "../../../lib/http";

export const prerender = false;

type AeoRecordExportRow = {
  sr_no: number;
  aeo_name: string;
  reporting_wing: string;
  institute_name: string;
  institute_type: string;
  tehsil: string;
  union_council_name: string;
  union_council_number: string;
  pp_no: string;
  na_no: string;
  complete_address: string;
  owner_name: string;
  owner_contact_no: string;
  principal_name: string;
  principal_contact_no: string;
  registration_status: string;
  registration_license_no: string;
  building_status: string;
  building_ownership: string;
  classrooms: number;
  students: number;
  teachers: number;
  facility_gap: string;
  drinking_water_facility: string;
  fire_safety_available: string;
  cctv_installed: string;
  ramp_available: string;
  psca_app_installed: string;
  wing: string;
  se_submitted_by: string;
  se_aeo_wing: string;
  date_of_visit_report: string;
  submitted_at: string;
  location_link_pin: string;
  hidden_location_coordinates: string;
  outside_main_gate_image_url: string;
  inside_institute_image_url: string;
};

const aeoRecordExportColumns = [
  { key: "sr_no", label: "Sr. No." },
  { key: "aeo_name", label: "AEO Name" },
  { key: "reporting_wing", label: "Reporting Wing" },
  { key: "institute_name", label: "Institute Name" },
  { key: "institute_type", label: "Institute Type" },
  { key: "tehsil", label: "Tehsil" },
  { key: "union_council_name", label: "Union Council Name" },
  { key: "union_council_number", label: "Union Council Number" },
  { key: "pp_no", label: "PP NO." },
  { key: "na_no", label: "NA NO" },
  { key: "complete_address", label: "Complete Address" },
  { key: "owner_name", label: "Owner Name" },
  { key: "owner_contact_no", label: "Owner Contact No." },
  { key: "principal_name", label: "Principal Name" },
  { key: "principal_contact_no", label: "Principal Contact No." },
  { key: "registration_status", label: "Registration Status" },
  { key: "registration_license_no", label: "Registration / License No." },
  { key: "building_status", label: "Building Status" },
  { key: "building_ownership", label: "Building Ownership" },
  { key: "classrooms", label: "Classrooms" },
  { key: "students", label: "Students" },
  { key: "teachers", label: "Teachers" },
  { key: "facility_gap", label: "Facility Gap" },
  { key: "drinking_water_facility", label: "Drinking Water Facility" },
  { key: "fire_safety_available", label: "Fire Safety Available" },
  { key: "cctv_installed", label: "CCTV Installed" },
  { key: "ramp_available", label: "Ramp Available" },
  { key: "psca_app_installed", label: "PSCA App Installed" },
  { key: "wing", label: "Stored Wing" },
  { key: "se_submitted_by", label: "SE Submitted By" },
  { key: "se_aeo_wing", label: "SE AEO Wing" },
  { key: "date_of_visit_report", label: "Date of Visit/Report" },
  { key: "submitted_at", label: "Submitted At" },
  { key: "location_link_pin", label: "Location Link/Pin" },
  { key: "hidden_location_coordinates", label: "Hidden Location Coordinates" },
  { key: "outside_main_gate_image_url", label: "Outside Main Gate Image URL" },
  { key: "inside_institute_image_url", label: "Inside Institute Image URL" },
] satisfies Array<{ key: keyof AeoRecordExportRow; label: string }>;

function displayValue(value: unknown): string {
  const cleaned = String(value ?? "").trim();
  return cleaned.length > 0 ? cleaned : "Not available";
}

function toNumber(value: unknown): number {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number : 0;
}

function toExportRows(records: AeoRecordRow[]): AeoRecordExportRow[] {
  return records.map((record, index) => ({
    sr_no: index + 1,
    aeo_name: displayValue(record.aeo_group_name),
    reporting_wing: displayValue(record.aeo_reporting_wing),
    institute_name: displayValue(record.institution_name),
    institute_type: displayValue(record.institute_type),
    tehsil: displayValue(record.tehsil),
    union_council_name: displayValue(record.union_council_name),
    union_council_number: displayValue(record.union_council_number),
    pp_no: displayValue(record.pp_no),
    na_no: displayValue(record.na_no),
    complete_address: displayValue(record.complete_address),
    owner_name: displayValue(record.owner_name),
    owner_contact_no: displayValue(record.owner_contact_no),
    principal_name: displayValue(record.principal_name),
    principal_contact_no: displayValue(record.principal_contact_no),
    registration_status: displayValue(record.registration_status),
    registration_license_no: displayValue(record.registration_license_no),
    building_status: displayValue(record.building_status),
    building_ownership: displayValue(record.building_ownership),
    classrooms: toNumber(record.no_of_classrooms),
    students: toNumber(record.no_of_students),
    teachers: toNumber(record.no_of_teachers),
    facility_gap: toNumber(record.facility_gap) > 0 ? "Gap found" : "No gap marked",
    drinking_water_facility: displayValue(record.drinking_water_facility),
    fire_safety_available: displayValue(record.fire_safety_available),
    cctv_installed: displayValue(record.cctv_installed),
    ramp_available: displayValue(record.ramp_available),
    psca_app_installed: displayValue(record.psca_app_installed),
    wing: displayValue(record.wing),
    se_submitted_by: displayValue(record.se_submitted_by),
    se_aeo_wing: displayValue(record.se_aeo_wing),
    date_of_visit_report: displayValue(record.date_of_visit_report),
    submitted_at: displayValue(record.submitted_at),
    location_link_pin: displayValue(record.location_link_pin),
    hidden_location_coordinates: displayValue(record.hidden_location_coordinates),
    outside_main_gate_image_url: displayValue(record.outside_main_gate_image_url),
    inside_institute_image_url: displayValue(record.inside_institute_image_url),
  }));
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

    const filter = parseAeoRecordFilter(url.searchParams);
    if (!filter) {
      return jsonResponse({ error: "AEO name and reporting wing are required." }, 400);
    }

    const db = getDb(context);
    const records = await getAeoRecords(db, filter);
    const csv = rowsToCsv(aeoRecordExportColumns, toExportRows(records));

    return new Response(csv, {
      headers: {
        "content-disposition": 'attachment; filename="PRIVATE_INSTITUTES_AEO_RECORDS_JULY_2026.csv"',
        "content-type": "text/csv;charset=utf-8",
      },
    });
  } catch (error) {
    console.error("AEO records export failed:", error);
    return jsonResponse({ error: "Unable to export AEO records." }, 500);
  }
};
