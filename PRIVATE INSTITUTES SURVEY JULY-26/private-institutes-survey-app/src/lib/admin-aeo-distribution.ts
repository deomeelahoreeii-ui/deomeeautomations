import { facilityGapSql } from "./admin-submission-query";
import type { D1Database } from "./d1";

export const aeoDisplayNameSql = `
  CASE
    WHEN aeo_name IS NULL OR TRIM(aeo_name) = '' OR aeo_name = 'Not applicable' THEN 'Not assigned'
    ELSE aeo_name
  END
`;

export const aeoReportingWingSql = `
  CASE
    WHEN wing = 'SE' AND se_submitted_by = 'AEO' THEN COALESCE(NULLIF(se_aeo_wing, ''), 'Not available')
    ELSE COALESCE(NULLIF(wing, ''), 'Not available')
  END
`;

export const aeoTrackedRecordsWhereSql = `
  NOT (wing = 'SE' AND se_submitted_by = 'Zonal Head')
  AND COALESCE(aeo_name, '') <> 'SE Zonal Head'
`;

export type AeoDistributionRow = {
  aeo_name: string | null;
  reporting_wing: string | null;
  total: number;
  students: number | null;
  teachers: number | null;
  registered: number | null;
  attention_records: number | null;
  facility_gap_records: number | null;
  tehsils: string | null;
};

export type AeoRecordFilter = {
  aeoName: string;
  reportingWing: string;
};

export type AeoRecordRow = Record<string, unknown> & {
  id: number;
  institution_name: string | null;
  institute_type: string | null;
  complete_address: string | null;
  location_link_pin: string | null;
  hidden_location_coordinates: string | null;
  tehsil: string | null;
  union_council_name: string | null;
  union_council_number: string | null;
  pp_no: string | null;
  na_no: string | null;
  owner_name: string | null;
  owner_contact_no: string | null;
  principal_name: string | null;
  principal_contact_no: string | null;
  registration_status: string | null;
  registration_license_no: string | null;
  building_status: string | null;
  building_ownership: string | null;
  no_of_classrooms: number | null;
  no_of_students: number | null;
  no_of_teachers: number | null;
  separate_washrooms_available: string | null;
  drinking_water_facility: string | null;
  fire_safety_available: string | null;
  cctv_installed: string | null;
  ramp_available: string | null;
  school_council_available: string | null;
  psca_app_installed: string | null;
  punjab_green_school_registration: string | null;
  wing: string | null;
  se_submitted_by: string | null;
  se_aeo_wing: string | null;
  aeo_name: string | null;
  date_of_visit_report: string | null;
  outside_main_gate_image_url: string | null;
  inside_institute_image_url: string | null;
  submitted_at: string | null;
  aeo_group_name: string | null;
  aeo_reporting_wing: string | null;
  facility_gap: number | null;
};

export function parseAeoRecordFilter(searchParams: URLSearchParams): AeoRecordFilter | null {
  const aeoName = String(searchParams.get("aeo") ?? "").trim();
  const reportingWing = String(searchParams.get("wing") ?? "").trim();

  if (!aeoName || !reportingWing) {
    return null;
  }

  return { aeoName, reportingWing };
}

export function buildAeoRecordsUrl(basePath: string, filter: AeoRecordFilter): string {
  const params = new URLSearchParams();
  params.set("aeo", filter.aeoName);
  params.set("wing", filter.reportingWing);

  return `${basePath}?${params.toString()}`;
}

export async function getAeoDistribution(db: D1Database): Promise<AeoDistributionRow[]> {
  const result = await db
    .prepare(
      `
        SELECT
          ${aeoDisplayNameSql} AS aeo_name,
          ${aeoReportingWingSql} AS reporting_wing,
          COUNT(*) AS total,
          COALESCE(SUM(no_of_students), 0) AS students,
          COALESCE(SUM(no_of_teachers), 0) AS teachers,
          SUM(CASE WHEN registration_status = 'Registered' THEN 1 ELSE 0 END) AS registered,
          SUM(CASE WHEN registration_status IN ('Unregistered', 'Expired') THEN 1 ELSE 0 END) AS attention_records,
          SUM(CASE WHEN ${facilityGapSql} THEN 1 ELSE 0 END) AS facility_gap_records,
          GROUP_CONCAT(DISTINCT COALESCE(NULLIF(tehsil, ''), 'Not available')) AS tehsils
        FROM private_institutes_survey_2026
        WHERE ${aeoTrackedRecordsWhereSql}
        GROUP BY ${aeoDisplayNameSql}, ${aeoReportingWingSql}
        ORDER BY total DESC, reporting_wing, aeo_name
      `,
    )
    .all<AeoDistributionRow>();

  return result.results ?? [];
}

export async function getAeoRecords(
  db: D1Database,
  filter: AeoRecordFilter,
): Promise<AeoRecordRow[]> {
  const result = await db
    .prepare(
      `
        SELECT
          *,
          ${aeoDisplayNameSql} AS aeo_group_name,
          ${aeoReportingWingSql} AS aeo_reporting_wing,
          CASE WHEN ${facilityGapSql} THEN 1 ELSE 0 END AS facility_gap
        FROM private_institutes_survey_2026
        WHERE ${aeoTrackedRecordsWhereSql}
          AND ${aeoDisplayNameSql} = ?
          AND ${aeoReportingWingSql} = ?
        ORDER BY submitted_at DESC, id DESC
      `,
    )
    .bind(filter.aeoName, filter.reportingWing)
    .all<AeoRecordRow>();

  return result.results ?? [];
}
