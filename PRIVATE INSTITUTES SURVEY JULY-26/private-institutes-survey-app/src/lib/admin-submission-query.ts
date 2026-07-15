export const adminPageSizeOptions = [25, 50, 100, 200] as const;

export type AdminPageSize = (typeof adminPageSizeOptions)[number];

export type SubmissionFilters = {
  search: string;
  tehsil: string;
  instituteType: string;
  wing: string;
  registration: string;
  facility: string;
};

const searchableColumns = [
  "institution_name",
  "complete_address",
  "tehsil",
  "union_council_name",
  "union_council_number",
  "institute_type",
  "wing",
  "se_submitted_by",
  "se_aeo_wing",
  "owner_name",
  "owner_contact_no",
  "principal_name",
  "principal_contact_no",
  "aeo_name",
  "zonal_head_institute_name",
  "zonal_head_name",
  "zonal_head_contact_no",
  "registration_status",
  "submitted_at",
] as const;

const displayFilterColumns = new Set(["tehsil", "institute_type", "wing"]);

export const facilityGapSql = `
  (
    separate_washrooms_available = 'No'
    OR drinking_water_facility = 'No'
    OR fire_safety_available = 'No'
    OR cctv_installed = 'No'
    OR ramp_available = 'No'
    OR school_council_available = 'No'
    OR psca_app_installed = 'No'
    OR punjab_green_school_registration = 'No'
  )
`;

function cleanParam(value: string | null): string {
  return String(value ?? "").trim();
}

function getAllowedPageSize(value: string | null): AdminPageSize {
  const parsed = Number(value);
  return adminPageSizeOptions.includes(parsed as AdminPageSize) ? (parsed as AdminPageSize) : 25;
}

function getPositivePage(value: string | null): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : 1;
}

export function parseSubmissionFilters(searchParams: URLSearchParams): SubmissionFilters {
  return {
    search: cleanParam(searchParams.get("search")),
    tehsil: cleanParam(searchParams.get("tehsil")),
    instituteType: cleanParam(searchParams.get("type")),
    wing: cleanParam(searchParams.get("wing")),
    registration: cleanParam(searchParams.get("registration")),
    facility: cleanParam(searchParams.get("facility")),
  };
}

export function parseSubmissionPagination(searchParams: URLSearchParams): {
  page: number;
  pageSize: AdminPageSize;
} {
  return {
    page: getPositivePage(searchParams.get("page")),
    pageSize: getAllowedPageSize(searchParams.get("pageSize")),
  };
}

function addDisplayValueFilter(
  clauses: string[],
  params: unknown[],
  column: string,
  value: string,
) {
  if (!value) return;

  if (displayFilterColumns.has(column) && value === "Not available") {
    clauses.push(`(${column} IS NULL OR TRIM(${column}) = '')`);
    return;
  }

  clauses.push(`${column} = ?`);
  params.push(value);
}

export function buildSubmissionsWhere(filters: SubmissionFilters): {
  whereClause: string;
  params: unknown[];
} {
  const clauses: string[] = [];
  const params: unknown[] = [];

  if (filters.search) {
    clauses.push(`(${searchableColumns.map((column) => `LOWER(COALESCE(${column}, '')) LIKE ?`).join(" OR ")})`);
    const searchParam = `%${filters.search.toLowerCase()}%`;
    searchableColumns.forEach(() => params.push(searchParam));
  }

  addDisplayValueFilter(clauses, params, "tehsil", filters.tehsil);
  addDisplayValueFilter(clauses, params, "institute_type", filters.instituteType);
  addDisplayValueFilter(clauses, params, "wing", filters.wing);

  if (filters.registration) {
    clauses.push("registration_status = ?");
    params.push(filters.registration);
  }

  if (filters.facility === "gap") {
    clauses.push(facilityGapSql);
  } else if (filters.facility === "clear") {
    clauses.push(`NOT ${facilityGapSql}`);
  }

  return {
    whereClause: clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "",
    params,
  };
}

export function buildAdminPageUrl(
  basePath: string,
  filters: SubmissionFilters,
  pageSize: AdminPageSize,
  page: number,
): string {
  const params = new URLSearchParams();

  if (filters.search) params.set("search", filters.search);
  if (filters.tehsil) params.set("tehsil", filters.tehsil);
  if (filters.instituteType) params.set("type", filters.instituteType);
  if (filters.wing) params.set("wing", filters.wing);
  if (filters.registration) params.set("registration", filters.registration);
  if (filters.facility) params.set("facility", filters.facility);
  params.set("pageSize", String(pageSize));
  params.set("page", String(page));

  return `${basePath}?${params.toString()}`;
}
