#!/usr/bin/env python3
"""Bootstrap complaint fields/statuses and an API user inside Frappe.

This script is intentionally standalone. It is copied into the Frappe backend
container and executed with the bench virtualenv Python from the sites folder.
"""

from __future__ import annotations

import json
import os
import secrets
import sys
from pathlib import Path
from typing import Any

import frappe


STATUSES = [
    ("New", "Open", "Red", 1),
    ("Assigned", "Open", "Blue", 2),
    ("Under Examination", "Open", "Orange", 3),
    ("Awaiting Officer Report", "Paused", "Amber", 4),
    ("Awaiting School Response", "Paused", "Amber", 5),
    ("Reply Under Preparation", "Open", "Cyan", 6),
    ("Pending Approval", "Open", "Violet", 7),
    ("Approved", "Open", "Green", 8),
    ("Reply Issued", "Resolved", "Green", 9),
    ("Reopened", "Open", "Red", 10),
]

BASE_CATEGORIES = ["Others", "Administrative", "Unfair treatment", "Teacher behaviour"]



TRUE_CHECK_DEFAULTS = {"1", "true", "yes", "on", "y"}
FALSE_CHECK_DEFAULTS = {"", "0", "false", "no", "off", "n", "none", "null"}


def normalize_check_default(value: Any) -> str:
    """Return a Frappe-valid string default for a Check field.

    Frappe v16 accepts only the literal strings ``0`` and ``1`` when the
    DocType is revalidated. Some Helpdesk metadata versions store boolean-like
    strings such as ``false``. Those values work during app installation but
    block later Custom Field creation because the entire HD Ticket metadata is
    validated again.
    """

    if value is None:
        return "0"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return "1" if value else "0"
    text = str(value).strip()
    if text in {"0", "1"}:
        return text
    folded = text.casefold()
    if folded in TRUE_CHECK_DEFAULTS:
        return "1"
    if folded in FALSE_CHECK_DEFAULTS:
        return "0"
    # An unknown default cannot be represented safely by a checkbox. Default
    # to unchecked and record the repair in the bootstrap report.
    return "0"


def repair_invalid_check_defaults(doctype: str) -> list[dict[str, Any]]:
    """Repair invalid Check defaults before adding Custom Fields.

    Creating a Custom Field causes Frappe to validate every field on the target
    DocType. Helpdesk 1.27 can contain legacy boolean-like defaults on standard
    HD Ticket fields (for example ``false``), while Frappe 16 requires ``0`` or
    ``1``. Repair standard fields, existing custom fields and default property
    setters idempotently before that validation occurs.
    """

    repairs: list[dict[str, Any]] = []

    standard_rows = frappe.db.sql(
        """
        SELECT name, fieldname, label, `default`
        FROM `tabDocField`
        WHERE parent = %s AND fieldtype = 'Check'
        """,
        (doctype,),
        as_dict=True,
    )
    for row in standard_rows:
        raw = row.get("default")
        if raw in (None, "", "0", "1", 0, 1, False, True):
            continue
        normalized = normalize_check_default(raw)
        frappe.db.sql(
            "UPDATE `tabDocField` SET `default` = %s WHERE name = %s",
            (normalized, row["name"]),
        )
        repairs.append(
            {
                "source": "DocField",
                "name": row["name"],
                "fieldname": row.get("fieldname"),
                "label": row.get("label"),
                "before": raw,
                "after": normalized,
            }
        )

    custom_rows = frappe.db.sql(
        """
        SELECT name, fieldname, label, `default`
        FROM `tabCustom Field`
        WHERE dt = %s AND fieldtype = 'Check'
        """,
        (doctype,),
        as_dict=True,
    )
    for row in custom_rows:
        raw = row.get("default")
        if raw in (None, "", "0", "1", 0, 1, False, True):
            continue
        normalized = normalize_check_default(raw)
        frappe.db.sql(
            "UPDATE `tabCustom Field` SET `default` = %s WHERE name = %s",
            (normalized, row["name"]),
        )
        repairs.append(
            {
                "source": "Custom Field",
                "name": row["name"],
                "fieldname": row.get("fieldname"),
                "label": row.get("label"),
                "before": raw,
                "after": normalized,
            }
        )

    property_rows = frappe.db.sql(
        """
        SELECT
          ps.name,
          ps.field_name,
          ps.value,
          COALESCE(df.label, cf.label) AS label
        FROM `tabProperty Setter` AS ps
        LEFT JOIN `tabDocField` AS df
          ON df.parent = ps.doc_type AND df.fieldname = ps.field_name
        LEFT JOIN `tabCustom Field` AS cf
          ON cf.dt = ps.doc_type AND cf.fieldname = ps.field_name
        WHERE ps.doc_type = %s
          AND ps.property = 'default'
          AND COALESCE(df.fieldtype, cf.fieldtype) = 'Check'
        """,
        (doctype,),
        as_dict=True,
    )
    for row in property_rows:
        raw = row.get("value")
        if raw in (None, "", "0", "1", 0, 1, False, True):
            continue
        normalized = normalize_check_default(raw)
        frappe.db.sql(
            "UPDATE `tabProperty Setter` SET value = %s WHERE name = %s",
            (normalized, row["name"]),
        )
        repairs.append(
            {
                "source": "Property Setter",
                "name": row["name"],
                "fieldname": row.get("field_name"),
                "label": row.get("label"),
                "before": raw,
                "after": normalized,
            }
        )

    if repairs:
        frappe.clear_cache(doctype=doctype)
    return repairs


FIELDS: list[dict[str, Any]] = [
    {"fieldname": "custom_complaint_management_section", "label": "Complaint Management", "fieldtype": "Section Break", "insert_after": "description"},
    {"fieldname": "custom_deomee_case_id", "label": "Deomee Case ID", "fieldtype": "Data", "insert_after": "custom_complaint_management_section", "read_only": 1, "unique": 1, "no_copy": 1},
    {"fieldname": "custom_external_complaint_number", "label": "External Complaint Number", "fieldtype": "Data", "insert_after": "custom_deomee_case_id", "read_only": 1, "in_standard_filter": 1, "search_index": 1},
    {"fieldname": "custom_complaint_source", "label": "Complaint Source", "fieldtype": "Data", "insert_after": "custom_external_complaint_number", "read_only": 1, "in_standard_filter": 1},
    {"fieldname": "custom_sub_category", "label": "Sub-category", "fieldtype": "Data", "insert_after": "custom_complaint_source", "in_standard_filter": 1, "search_index": 1},
    {"fieldname": "custom_school_name", "label": "School Name", "fieldtype": "Data", "insert_after": "custom_sub_category", "in_standard_filter": 1},
    {"fieldname": "custom_emis_code", "label": "EMIS Code", "fieldtype": "Data", "insert_after": "custom_school_name", "in_standard_filter": 1, "search_index": 1},
    {"fieldname": "custom_tehsil", "label": "Tehsil", "fieldtype": "Data", "insert_after": "custom_emis_code", "in_standard_filter": 1},
    {"fieldname": "custom_date_received", "label": "Date Received", "fieldtype": "Date", "insert_after": "custom_tehsil", "read_only": 1},
    {"fieldname": "custom_reply_due_date", "label": "Reply Due Date", "fieldtype": "Date", "insert_after": "custom_date_received"},
    {"fieldname": "custom_document_links_column", "label": "Document Links", "fieldtype": "Column Break", "insert_after": "custom_reply_due_date"},
    {"fieldname": "custom_paperless_document_id", "label": "Paperless Document ID", "fieldtype": "Int", "insert_after": "custom_document_links_column", "read_only": 1, "in_standard_filter": 1},
    {"fieldname": "custom_paperless_case_url", "label": "Paperless Case URL", "fieldtype": "Small Text", "insert_after": "custom_paperless_document_id", "read_only": 1},
    {"fieldname": "custom_deomee_case_url", "label": "Deomee Case URL", "fieldtype": "Small Text", "insert_after": "custom_paperless_case_url", "read_only": 1},
    {"fieldname": "custom_evidence_status", "label": "Evidence Status", "fieldtype": "Select", "options": "Not Archived\nPaperless Archived\nEvidence Incomplete\nEvidence Verified", "insert_after": "custom_deomee_case_url", "in_standard_filter": 1},
    {"fieldname": "custom_findings_section", "label": "Findings and Reply", "fieldtype": "Section Break", "insert_after": "custom_evidence_status"},
    {"fieldname": "custom_inquiry_findings", "label": "Inquiry Findings", "fieldtype": "Long Text", "insert_after": "custom_findings_section"},
    {"fieldname": "custom_school_version", "label": "School / Institution Version", "fieldtype": "Long Text", "insert_after": "custom_inquiry_findings"},
    {"fieldname": "custom_applicable_policy", "label": "Applicable Policy / Rule", "fieldtype": "Long Text", "insert_after": "custom_school_version"},
    {"fieldname": "custom_final_approved_reply", "label": "Final Approved Reply", "fieldtype": "Long Text", "insert_after": "custom_applicable_policy"},
    {"fieldname": "custom_reply_approval_status", "label": "Reply Approval Status", "fieldtype": "Select", "options": "Not Prepared\nDraft\nPending Approval\nApproved\nRejected\nIssued", "insert_after": "custom_final_approved_reply", "in_standard_filter": 1},
    {"fieldname": "custom_disposal_outcome", "label": "Disposal Outcome", "fieldtype": "Data", "insert_after": "custom_reply_approval_status", "in_standard_filter": 1},
    {"fieldname": "custom_ai_eligible", "label": "AI Eligible", "fieldtype": "Check", "insert_after": "custom_disposal_outcome", "default": "0", "in_standard_filter": 1},
    {"fieldname": "custom_sync_audit_section", "label": "Deomee Synchronization", "fieldtype": "Section Break", "insert_after": "custom_ai_eligible", "collapsible": 1},
    {"fieldname": "custom_deomee_sync_version", "label": "Deomee Sync Version", "fieldtype": "Int", "insert_after": "custom_sync_audit_section", "read_only": 1},
    {"fieldname": "custom_deomee_payload_hash", "label": "Deomee Payload Hash", "fieldtype": "Data", "insert_after": "custom_deomee_sync_version", "read_only": 1, "hidden": 1},
]


def ensure_named(doctype: str, name: str, values: dict[str, Any]) -> str:
    if not frappe.db.exists(doctype, name):
        frappe.get_doc({"doctype": doctype, "name": name, **values}).insert(ignore_permissions=True)
        return "created"
    doc = frappe.get_doc(doctype, name)
    changed = False
    for field, value in values.items():
        if getattr(doc, field, None) != value:
            setattr(doc, field, value)
            changed = True
    if changed:
        doc.save(ignore_permissions=True)
        return "updated"
    return "unchanged"


def ensure_custom_field(spec: dict[str, Any]) -> str:
    name = f"HD Ticket-{spec['fieldname']}"
    values = {"dt": "HD Ticket", **spec}
    return ensure_named("Custom Field", name, values)


def ensure_template() -> str:
    if not frappe.db.exists("HD Ticket Template", "Default"):
        return "missing"
    template = frappe.get_doc("HD Ticket Template", "Default")
    existing = {row.fieldname: row for row in template.fields}
    changed = False
    for spec in FIELDS:
        if spec["fieldtype"] in {"Section Break", "Column Break", "Tab Break"}:
            continue
        row = existing.get(spec["fieldname"])
        desired = {
            "fieldname": spec["fieldname"],
            "required": 0,
            "hide_from_customer": 1,
            "placeholder": "",
            "url_method": "",
        }
        if row is None:
            template.append("fields", desired)
            changed = True
        else:
            for field, value in desired.items():
                if getattr(row, field, None) != value:
                    setattr(row, field, value)
                    changed = True
    if changed:
        template.save(ignore_permissions=True)
        return "updated"
    return "unchanged"


def ensure_api_user() -> dict[str, str]:
    email = os.getenv("DEOMEE_INTEGRATION_USER", "deomee.integration@example.local")
    api_key = os.getenv("DEOMEE_API_KEY", "").strip() or secrets.token_hex(16)
    api_secret = os.getenv("DEOMEE_API_SECRET", "").strip() or secrets.token_urlsafe(36)
    if not frappe.db.exists("User", email):
        user = frappe.get_doc(
            {
                "doctype": "User",
                "email": email,
                "first_name": "Deomee",
                "last_name": "Integration",
                "enabled": 1,
                "user_type": "System User",
                "send_welcome_email": 0,
            }
        ).insert(ignore_permissions=True)
    else:
        user = frappe.get_doc("User", email)
        user.enabled = 1
        user.user_type = "System User"
    existing_roles = {row.role for row in user.roles}
    for role in ("Agent", "Agent Manager", "System Manager"):
        if role not in existing_roles:
            user.append("roles", {"role": role})
    user.api_key = api_key
    user.api_secret = api_secret
    user.save(ignore_permissions=True)

    if not frappe.db.exists("HD Agent", email):
        frappe.get_doc(
            {
                "doctype": "HD Agent",
                "name": email,
                "user": email,
                "agent_name": email,
                "is_active": 1,
                "availability": "Active",
            }
        ).insert(ignore_permissions=True)
    else:
        agent = frappe.get_doc("HD Agent", email)
        agent.is_active = 1
        agent.availability = "Active"
        agent.save(ignore_permissions=True)
    return {"user": email, "api_key": api_key, "api_secret": api_secret}


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: frappe_helpdesk_bootstrap.py <site>")
    site = sys.argv[1]
    sites_path = Path.cwd()
    frappe.init(site=site, sites_path=str(sites_path))
    frappe.connect()
    frappe.set_user("Administrator")
    try:
        result: dict[str, Any] = {
            "site": site,
            "check_default_repairs": repair_invalid_check_defaults("HD Ticket"),
            "statuses": {"created": 0, "updated": 0, "unchanged": 0},
            "ticket_types": {"created": 0, "updated": 0, "unchanged": 0},
            "custom_fields": {"created": 0, "updated": 0, "unchanged": 0},
        }
        for label, category, color, order in STATUSES:
            action = ensure_named(
                "HD Ticket Status",
                label,
                {
                    "label_agent": label,
                    "category": category,
                    "color": color,
                    "order": order,
                    "enabled": 1,
                    "different_view": 0,
                },
            )
            result["statuses"][action] += 1
        for category in BASE_CATEGORIES:
            action = ensure_named(
                "HD Ticket Type",
                category,
                {
                    "description": "Complaint category synchronized from Deomee CRM.",
                    "disabled": 0,
                    "is_system": 0,
                },
            )
            result["ticket_types"][action] += 1
        for field in FIELDS:
            action = ensure_custom_field(field)
            result["custom_fields"][action] += 1
        result["template"] = ensure_template()
        result["credentials"] = ensure_api_user()
        frappe.clear_cache(doctype="HD Ticket")
        frappe.db.commit()
        print("DEOMEE_BOOTSTRAP_RESULT=" + json.dumps(result, ensure_ascii=False, default=str))
        return 0
    except Exception:
        frappe.db.rollback()
        raise
    finally:
        frappe.destroy()


if __name__ == "__main__":
    raise SystemExit(main())
