const titleCase = (value: unknown) =>
  String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());

const CASE_STATES: Record<string, string> = {
  candidate: "Candidate",
  review_required: "Needs review",
  fresh: "Approved — ready for publication",
  existing: "Already archived in Paperless",
  publishing: "Publishing to Paperless",
  published: "Published to Paperless",
  rejected: "Rejected",
};

const REVIEW_BUCKETS: Record<string, string> = {
  ready: "Ready for decision",
  manual_review: "Manual review",
  approved: "Approved",
  existing: "Already in Paperless",
  rejected: "Rejected",
};

const PAPERLESS_RESULTS: Record<string, string> = {
  fresh: "No Paperless match",
  submitted: "Existing in Paperless · Submitted",
  uploaded_pending: "Existing in Paperless · Pending",
  uploaded_not_relevant: "Existing in Paperless · Not Relevant",
  manual_review: "Conflicting Paperless matches",
  unavailable: "Paperless check unavailable",
  not_checked: "Paperless not checked",
  not_applicable: "Paperless not applicable",
  existing: "Existing in Paperless",
};

export const caseStateLabel = (value: unknown) =>
  CASE_STATES[String(value || "")] || titleCase(value) || "Unknown";

export const reviewBucketLabel = (value: unknown) =>
  REVIEW_BUCKETS[String(value || "")] || titleCase(value) || "Unknown";

export const paperlessResultLabel = (
  category: unknown,
  statuses: unknown[] = [],
) => {
  const key = String(category || "not_checked");
  if (PAPERLESS_RESULTS[key]) return PAPERLESS_RESULTS[key];
  const status = statuses.map(String).filter(Boolean)[0];
  return status ? `Existing in Paperless · ${status}` : titleCase(key);
};

export const processingStatusLabel = (value: unknown) =>
  titleCase(value) || "Unknown";

export const paperlessLinkSummary = (
  documentId: unknown,
  category: unknown,
  statuses: unknown[] = [],
) => {
  const label = paperlessResultLabel(category, statuses);
  return documentId ? `${label} · linked to Paperless #${documentId}` : label;
};
