import { caseStateLabel, paperlessLinkSummary } from "./crm-status";

export function hydrateCaseHeader(data: any) {
  const title = document.getElementById("case-page-title");
  const meta = document.getElementById("case-page-meta");
  const state = document.getElementById("case-page-state");
  const next = document.getElementById("case-next-action") as HTMLAnchorElement | null;
  if (!title || !meta || !state || !next) return;
  title.textContent = data.complaint_number || "Complaint number needs review";
  const paperless = data.paperless_result || {};
  const values = [
    `${Number(data.document_count || 0)} evidence document${Number(data.document_count || 0) === 1 ? "" : "s"}`,
    paperlessLinkSummary(
      paperless.document_id || data.paperless_document_id,
      paperless.category,
      paperless.statuses || [],
    ),
    data.frappe_ticket_id ? `Helpdesk #${data.frappe_ticket_id}` : "Helpdesk not linked",
  ];
  meta.replaceChildren(
    ...values.map((value) => {
      const span = document.createElement("span");
      span.textContent = value;
      return span;
    }),
  );
  state.textContent = caseStateLabel(data.state);
  state.className = `crm-run-state ${String(data.state || "")}`;
  const nextStep = ["candidate", "review_required"].includes(data.state)
    ? ["evidence", "Review evidence"]
    : data.state === "existing"
      ? ["evidence", "Review new evidence"]
    : data.state === "fresh"
      ? ["publication", "Review publication"]
      : data.state === "publishing"
        ? ["publication", "Check publication"]
      : data.state === "published"
        ? ["helpdesk", "Continue to Helpdesk"]
        : ["overview", "Review case overview"];
  next.href = `/crm/cases/${data.id}/${nextStep[0]}`;
  next.textContent = `${nextStep[1]} →`;
}
