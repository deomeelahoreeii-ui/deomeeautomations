import { zonalHeadOptions } from "./zonal-heads";

export type SurveyFieldKind =
  | "text"
  | "textarea"
  | "number"
  | "date"
  | "datetime-local"
  | "email"
  | "hidden"
  | "image"
  | "location"
  | "aeo"
  | "select"
  | "yesNo";

export type SurveyField = {
  name: string;
  label: string;
  exportLabel: string;
  kind: SurveyFieldKind;
  required?: boolean;
  options?: readonly string[];
  autocomplete?: string;
  inputMode?: "text" | "tel" | "numeric" | "decimal" | "email";
  min?: number;
  colSpan?: "full" | "wide";
};

export type SurveyStep = {
  id: string;
  title: string;
  description: string;
  fields: readonly SurveyField[];
};

const yesNoOptions = ["Yes", "No"] as const;
export const instituteTypeOptions = [
  "School",
  "Academy",
  "Coaching Center",
  "Tuition Center",
] as const;
export const wingOptions = ["MEE", "WEE", "SE"] as const;
export const seSubmittedByOptions = ["Zonal Head", "AEO"] as const;
export const seAeoWingOptions = ["MEE", "WEE"] as const;
export const tehsilOptions = ["Cantt", "Shalimar", "Raiwind", "Model Town", "City"] as const;
export const zonalHeadInstituteOptions = zonalHeadOptions.map((option) => option.institute);

export const surveySteps: readonly SurveyStep[] = [
  {
    id: "institute",
    title: "Institute",
    description: "School identity and location",
    fields: [
      {
        name: "institution_name",
        label: "Institute Name",
        exportLabel: "Institute Name",
        kind: "text",
        autocomplete: "organization",
        colSpan: "full",
      },
      {
        name: "institute_type",
        label: "Institute Type",
        exportLabel: "Institute Type",
        kind: "select",
        required: true,
        options: instituteTypeOptions,
      },
      {
        name: "complete_address",
        label: "Complete Address",
        exportLabel: "Complete Address",
        kind: "textarea",
        colSpan: "full",
      },
      {
        name: "location_link_pin",
        label: "Location Link/Pin",
        exportLabel: "Location Link/Pin",
        kind: "location",
        colSpan: "full",
      },
      {
        name: "hidden_location_coordinates",
        label: "Hidden Location Coordinates",
        exportLabel: "Hidden Location Cordinates",
        kind: "hidden",
      },
      {
        name: "tehsil",
        label: "Tehsil",
        exportLabel: "Tehsil",
        kind: "select",
        required: true,
        options: tehsilOptions,
      },
      {
        name: "union_council_name",
        label: "Union Council Name",
        exportLabel: "Union Council Name",
        kind: "text",
      },
      {
        name: "union_council_number",
        label: "Union Council Number",
        exportLabel: "Union Council Number",
        kind: "text",
        inputMode: "numeric",
      },
      {
        name: "pp_no",
        label: "PP NO.",
        exportLabel: "PP NO.",
        kind: "text",
      },
      {
        name: "na_no",
        label: "NA NO",
        exportLabel: "NA NO",
        kind: "text",
      },
    ],
  },
  {
    id: "management",
    title: "Management",
    description: "Owner and principal contacts",
    fields: [
      {
        name: "owner_name",
        label: "Owner Name",
        exportLabel: "Owner Name",
        kind: "text",
        autocomplete: "name",
      },
      {
        name: "owner_contact_no",
        label: "Owner Contact No.",
        exportLabel: "Owner Contact No.",
        kind: "text",
        autocomplete: "tel",
        inputMode: "tel",
      },
      {
        name: "principal_name",
        label: "Principal Name",
        exportLabel: "Principal Name",
        kind: "text",
        autocomplete: "name",
      },
      {
        name: "principal_contact_no",
        label: "Principal Contact No.",
        exportLabel: "Principal Contact No.",
        kind: "text",
        autocomplete: "tel",
        inputMode: "tel",
      },
    ],
  },
  {
    id: "registration",
    title: "Registration",
    description: "Legal and building status",
    fields: [
      {
        name: "registration_status",
        label: "Registration Status",
        exportLabel: "Registration Status (Registered / Unregistered / Expired)",
        kind: "select",
        required: true,
        options: ["Registered", "Unregistered", "Expired"],
      },
      {
        name: "registration_license_no",
        label: "Registration / License No.",
        exportLabel: "Registration / License No.",
        kind: "text",
      },
      {
        name: "validity_date",
        label: "Validity Date",
        exportLabel: "Validity Date",
        kind: "date",
      },
      {
        name: "building_status",
        label: "Building Status",
        exportLabel: "Building Status (Satisfactory / Weak / Dilapidated)",
        kind: "select",
        required: true,
        options: ["Satisfactory", "Weak", "Dilapidated"],
      },
      {
        name: "building_ownership",
        label: "Building Ownership",
        exportLabel: "Building Ownership (Owned / Rented)",
        kind: "select",
        required: true,
        options: ["Owned", "Rented"],
      },
    ],
  },
  {
    id: "enrollment",
    title: "Enrollment",
    description: "Rooms, students, teachers, classes",
    fields: [
      {
        name: "no_of_classrooms",
        label: "No of Classrooms",
        exportLabel: "No of Classrooms",
        kind: "number",
        min: 0,
      },
      {
        name: "no_of_students",
        label: "No. of Students",
        exportLabel: "No. of Students",
        kind: "number",
        min: 0,
      },
      {
        name: "no_of_teachers",
        label: "No. of Teachers",
        exportLabel: "No. of Teachers",
        kind: "number",
        min: 0,
      },
      {
        name: "disadvantaged_students_10_percent",
        label: "10% Disadvantage Students",
        exportLabel: "Number of 10% Disadvantage Students Enrolled",
        kind: "number",
        min: 0,
      },
      {
        name: "classes_offered",
        label: "Classes Offered",
        exportLabel: "Classes Offered",
        kind: "text",
        colSpan: "wide",
      },
      {
        name: "quran_teacher_name",
        label: "Quran Teacher Name",
        exportLabel: "Quran Teacher Name",
        kind: "text",
      },
      {
        name: "quran_teacher_mobile_no",
        label: "Quran Teacher Mobile No.",
        exportLabel: "Quran Teacher Mobile No.",
        kind: "text",
        autocomplete: "tel",
        inputMode: "tel",
      },
      {
        name: "monthly_fee_range",
        label: "Monthly Fee Range",
        exportLabel: "Monthly Fee Range",
        kind: "text",
      },
    ],
  },
  {
    id: "facilities",
    title: "Facilities",
    description: "Safety and accessibility checks",
    fields: [
      {
        name: "separate_washrooms_available",
        label: "Separate Washrooms for Male & Female",
        exportLabel: "Separate Washrooms Available for Male & Female (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
      {
        name: "drinking_water_facility",
        label: "Drinking Water Facility",
        exportLabel: "Drinking Water Facility (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
      {
        name: "fire_safety_available",
        label: "Fire Safety Available",
        exportLabel: "Fire Safety Available (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
      {
        name: "cctv_installed",
        label: "CCTV Installed",
        exportLabel: "CCTV Installed (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
      {
        name: "ramp_available",
        label: "Ramp Available",
        exportLabel: "Ramp Available (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
      {
        name: "school_council_available",
        label: "School Council Available",
        exportLabel: "School Council Available (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
      {
        name: "psca_app_installed",
        label: "PSCA App Installed",
        exportLabel: "PSCA App Installed (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
      {
        name: "punjab_green_school_registration",
        label: "Punjab Green School Registration",
        exportLabel: "Punjab Green School Registration (Yes/No)",
        kind: "yesNo",
        required: true,
        options: yesNoOptions,
      },
    ],
  },
  {
    id: "report",
    title: "Report",
    description: "AEO and visit details",
    fields: [
      {
        name: "school_email_address",
        label: "School Email Address",
        exportLabel: "School Email Address",
        kind: "email",
        autocomplete: "email",
      },
      {
        name: "wing",
        label: "Wing",
        exportLabel: "Wing",
        kind: "select",
        required: true,
        options: wingOptions,
      },
      {
        name: "se_submitted_by",
        label: "SE Submitted By",
        exportLabel: "SE Submitted By",
        kind: "select",
        required: true,
        options: seSubmittedByOptions,
      },
      {
        name: "se_aeo_wing",
        label: "AEO Wing",
        exportLabel: "SE AEO Wing",
        kind: "select",
        required: true,
        options: seAeoWingOptions,
      },
      {
        name: "aeo_name",
        label: "AEO Name",
        exportLabel: "AEO Name",
        kind: "aeo",
        required: true,
      },
      {
        name: "zonal_head_institute_name",
        label: "Zonal Head Institute Name",
        exportLabel: "Zonal Head Institute Name",
        kind: "select",
        required: true,
        options: zonalHeadInstituteOptions,
        colSpan: "full",
      },
      {
        name: "zonal_head_name",
        label: "Zonal Head Name",
        exportLabel: "Zonal Head Name",
        kind: "text",
        autocomplete: "name",
      },
      {
        name: "zonal_head_contact_no",
        label: "Zonal Head Contact No.",
        exportLabel: "Zonal Head Contact No.",
        kind: "text",
        autocomplete: "tel",
        inputMode: "tel",
      },
      {
        name: "date_of_visit_report",
        label: "Date of Visit/Report",
        exportLabel: "Date of Visit/Report",
        kind: "datetime-local",
      },
      {
        name: "outside_main_gate_image_url",
        label: "Outside Main Gate Image",
        exportLabel: "Outside Main Gate Image URL",
        kind: "image",
        colSpan: "full",
      },
      {
        name: "inside_institute_image_url",
        label: "Inside Institute Image",
        exportLabel: "Inside Institute Image URL",
        kind: "image",
        colSpan: "full",
      },
    ],
  },
] as const;

export const surveyFields: readonly SurveyField[] = surveySteps.flatMap((step) => step.fields);

export const insertColumns = surveyFields.map((field) => field.name);

export const csvColumns = [
  { key: "sr_no", label: "Sr. No." },
  ...surveyFields.map((field) => ({
    key: field.name,
    label: field.exportLabel,
  })),
  { key: "submitted_at", label: "Submitted At" },
];

export const fieldLabels = Object.fromEntries(
  surveyFields.map((field) => [field.name, field.label]),
);
