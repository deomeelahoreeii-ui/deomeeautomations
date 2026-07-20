from __future__ import annotations

from typing import Any, Iterable

from crm_integrations.frappe_helpdesk.client import (
    FrappeHelpdeskClient,
    FrappeHelpdeskError,
)


COMPLAINT_STATUSES: tuple[dict[str, Any], ...] = (
    {"label_agent": "New", "category": "Open", "color": "Red", "order": 1},
    {"label_agent": "Assigned", "category": "Open", "color": "Blue", "order": 2},
    {"label_agent": "Under Examination", "category": "Open", "color": "Orange", "order": 3},
    {"label_agent": "Awaiting Officer Report", "category": "Paused", "color": "Amber", "order": 4},
    {"label_agent": "Awaiting School Response", "category": "Paused", "color": "Amber", "order": 5},
    {"label_agent": "Reply Under Preparation", "category": "Open", "color": "Cyan", "order": 6},
    {"label_agent": "Pending Approval", "category": "Open", "color": "Violet", "order": 7},
    {"label_agent": "Approved", "category": "Open", "color": "Green", "order": 8},
    {"label_agent": "Reply Issued", "category": "Resolved", "color": "Green", "order": 9},
    {"label_agent": "Reopened", "category": "Open", "color": "Red", "order": 10},
)


CUSTOM_FIELDS: tuple[dict[str, Any], ...] = (
    {
        "fieldname": "custom_complaint_management_section",
        "label": "Complaint Management",
        "fieldtype": "Section Break",
        "insert_after": "description",
    },
    {
        "fieldname": "custom_deomee_case_id",
        "label": "Deomee Case ID",
        "fieldtype": "Data",
        "insert_after": "custom_complaint_management_section",
        "read_only": 1,
        "unique": 1,
        "no_copy": 1,
    },
    {
        "fieldname": "custom_external_complaint_number",
        "label": "External Complaint Number",
        "fieldtype": "Data",
        "insert_after": "custom_deomee_case_id",
        "read_only": 1,
        "in_standard_filter": 1,
        "search_index": 1,
    },
    {
        "fieldname": "custom_complaint_source",
        "label": "Complaint Source",
        "fieldtype": "Data",
        "insert_after": "custom_external_complaint_number",
        "read_only": 1,
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_sub_category",
        "label": "Sub-category",
        "fieldtype": "Data",
        "insert_after": "custom_complaint_source",
        "in_standard_filter": 1,
        "search_index": 1,
    },
    {
        "fieldname": "custom_school_name",
        "label": "School Name",
        "fieldtype": "Data",
        "insert_after": "custom_sub_category",
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_emis_code",
        "label": "EMIS Code",
        "fieldtype": "Data",
        "insert_after": "custom_school_name",
        "in_standard_filter": 1,
        "search_index": 1,
    },
    {
        "fieldname": "custom_tehsil",
        "label": "Tehsil",
        "fieldtype": "Data",
        "insert_after": "custom_emis_code",
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_date_received",
        "label": "Date Received",
        "fieldtype": "Date",
        "insert_after": "custom_tehsil",
        "read_only": 1,
    },
    {
        "fieldname": "custom_reply_due_date",
        "label": "Reply Due Date",
        "fieldtype": "Date",
        "insert_after": "custom_date_received",
    },
    {
        "fieldname": "custom_document_links_column",
        "label": "Document Links",
        "fieldtype": "Column Break",
        "insert_after": "custom_reply_due_date",
    },
    {
        "fieldname": "custom_paperless_document_id",
        "label": "Paperless Document ID",
        "fieldtype": "Int",
        "insert_after": "custom_document_links_column",
        "read_only": 1,
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_paperless_case_url",
        "label": "Paperless Case URL",
        "fieldtype": "Small Text",
        "insert_after": "custom_paperless_document_id",
        "read_only": 1,
    },
    {
        "fieldname": "custom_deomee_case_url",
        "label": "Deomee Case URL",
        "fieldtype": "Small Text",
        "insert_after": "custom_paperless_case_url",
        "read_only": 1,
    },
    {
        "fieldname": "custom_evidence_status",
        "label": "Evidence Status",
        "fieldtype": "Select",
        "options": "Not Archived\nPaperless Archived\nEvidence Incomplete\nEvidence Verified",
        "insert_after": "custom_deomee_case_url",
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_findings_section",
        "label": "Findings and Reply",
        "fieldtype": "Section Break",
        "insert_after": "custom_evidence_status",
    },
    {
        "fieldname": "custom_inquiry_findings",
        "label": "Inquiry Findings",
        "fieldtype": "Long Text",
        "insert_after": "custom_findings_section",
    },
    {
        "fieldname": "custom_school_version",
        "label": "School / Institution Version",
        "fieldtype": "Long Text",
        "insert_after": "custom_inquiry_findings",
    },
    {
        "fieldname": "custom_applicable_policy",
        "label": "Applicable Policy / Rule",
        "fieldtype": "Long Text",
        "insert_after": "custom_school_version",
    },
    {
        "fieldname": "custom_final_approved_reply",
        "label": "Final Approved Reply",
        "fieldtype": "Long Text",
        "insert_after": "custom_applicable_policy",
    },
    {
        "fieldname": "custom_reply_approval_status",
        "label": "Reply Approval Status",
        "fieldtype": "Select",
        "options": "Not Prepared\nDraft\nPending Approval\nApproved\nRejected\nIssued",
        "insert_after": "custom_final_approved_reply",
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_disposal_outcome",
        "label": "Disposal Outcome",
        "fieldtype": "Data",
        "insert_after": "custom_reply_approval_status",
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_ai_eligible",
        "label": "AI Eligible",
        "fieldtype": "Check",
        "insert_after": "custom_disposal_outcome",
        "default": "0",
        "in_standard_filter": 1,
    },
    {
        "fieldname": "custom_sync_audit_section",
        "label": "Deomee Synchronization",
        "fieldtype": "Section Break",
        "insert_after": "custom_ai_eligible",
        "collapsible": 1,
    },
    {
        "fieldname": "custom_deomee_sync_version",
        "label": "Deomee Sync Version",
        "fieldtype": "Int",
        "insert_after": "custom_sync_audit_section",
        "read_only": 1,
    },
    {
        "fieldname": "custom_deomee_payload_hash",
        "label": "Deomee Payload Hash",
        "fieldtype": "Data",
        "insert_after": "custom_deomee_sync_version",
        "read_only": 1,
        "hidden": 1,
    },
)

TEMPLATE_FIELDS: tuple[dict[str, Any], ...] = tuple(
    {
        "fieldname": field["fieldname"],
        "required": 0,
        "hide_from_customer": 1,
        "placeholder": "",
        "url_method": "",
    }
    for field in CUSTOM_FIELDS
    if field["fieldtype"] not in {"Section Break", "Column Break", "Tab Break"}
)


def _existing(client: FrappeHelpdeskClient, doctype: str, name: str) -> dict[str, Any] | None:
    try:
        return client.get_resource(doctype, name)
    except FrappeHelpdeskError as exc:
        if exc.status_code == 404:
            return None
        raise


def _ensure_named_resource(
    client: FrappeHelpdeskClient,
    doctype: str,
    name: str,
    values: dict[str, Any],
) -> str:
    current = _existing(client, doctype, name)
    if current is None:
        client.create_resource(doctype, values)
        return "created"
    changed = {key: value for key, value in values.items() if current.get(key) != value}
    if changed:
        client.update_resource(doctype, name, changed)
        return "updated"
    return "unchanged"


def ensure_helpdesk_foundation(
    client: FrappeHelpdeskClient,
    categories: Iterable[str],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "statuses": {"created": 0, "updated": 0, "unchanged": 0},
        "ticket_types": {"created": 0, "updated": 0, "unchanged": 0},
        "custom_fields": {"created": 0, "updated": 0, "unchanged": 0},
        "template": "unchanged",
    }

    for status in COMPLAINT_STATUSES:
        values = {**status, "enabled": 1, "different_view": 0}
        action = _ensure_named_resource(
            client,
            "HD Ticket Status",
            str(status["label_agent"]),
            values,
        )
        result["statuses"][action] += 1

    category_names = sorted({"Others", *(" ".join(str(item).split()).strip() for item in categories)} - {""})
    for category in category_names:
        values = {
            "name": category,
            "description": "Complaint category synchronized from Deomee CRM.",
            "disabled": 0,
            "is_system": 0,
        }
        action = _ensure_named_resource(client, "HD Ticket Type", category, values)
        result["ticket_types"][action] += 1

    for field in CUSTOM_FIELDS:
        name = f"HD Ticket-{field['fieldname']}"
        values = {"dt": "HD Ticket", **field}
        action = _ensure_named_resource(client, "Custom Field", name, values)
        result["custom_fields"][action] += 1

    template = _existing(client, "HD Ticket Template", "Default")
    if template is not None:
        existing_rows = template.get("fields") if isinstance(template.get("fields"), list) else []
        by_name = {
            str(row.get("fieldname")): row
            for row in existing_rows
            if isinstance(row, dict) and row.get("fieldname")
        }
        changed = False
        for desired in TEMPLATE_FIELDS:
            current = by_name.get(str(desired["fieldname"]))
            if current is None:
                existing_rows.append(dict(desired))
                changed = True
                continue
            for key, value in desired.items():
                if current.get(key) != value:
                    current[key] = value
                    changed = True
        if changed:
            client.update_resource("HD Ticket Template", "Default", {"fields": existing_rows})
            result["template"] = "updated"
    return result
