from __future__ import annotations

import hashlib
import html
import json
from datetime import datetime
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

from automation_core.config import Settings
from crm_domain.models import ComplaintCase


def normalized_category(value: str | None) -> str:
    category = " ".join(str(value or "").split()).strip()
    return category or "Others"


def _plain_text_html(value: str | None) -> str:
    text = str(value or "").strip()
    return "<p>" + html.escape(text).replace("\n", "<br>") + "</p>" if text else "<p></p>"


def _subject(case: ComplaintCase) -> str:
    number = case.complaint_number or str(case.id)
    category = normalized_category(case.category)
    subject = f"Complaint {number} — {category}"
    return subject[:140]


def _public_paperless_root(settings: Settings) -> str:
    configured = str(settings.paperless_url or "").strip().rstrip("/")
    if not configured:
        return ""
    parts = urlsplit(configured)
    path = parts.path.rstrip("/")
    for suffix in ("/dashboard", "/api"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
    return urlunsplit((parts.scheme, parts.netloc, path, "", "")).rstrip("/")


def paperless_document_url(case: ComplaintCase, settings: Settings) -> str | None:
    if not case.canonical_paperless_document_id:
        return None
    root = _public_paperless_root(settings)
    if not root:
        return None
    template = str(settings.frappe_helpdesk_paperless_url_template or "").strip()
    if template:
        return template.format(
            base_url=root,
            document_id=case.canonical_paperless_document_id,
        )
    return f"{root}/documents/{case.canonical_paperless_document_id}/details"


def crm_case_url(case: ComplaintCase, settings: Settings) -> str | None:
    root = str(settings.frappe_helpdesk_crm_public_url or "").strip().rstrip("/")
    return f"{root}/crm/cases/{case.id}" if root else None


def helpdesk_ticket_url(ticket_id: str, settings: Settings) -> str:
    root = str(
        settings.frappe_helpdesk_public_url or settings.frappe_helpdesk_url
    ).strip().rstrip("/")
    return f"{root}/helpdesk/tickets/{ticket_id}"


def _base_payload(case: ComplaintCase, settings: Settings, sync_version: int) -> dict[str, Any]:
    values: dict[str, Any] = {
        "subject": _subject(case),
        "description": _plain_text_html(case.remarks),
        "ticket_type": normalized_category(case.category),
        "custom_deomee_case_id": str(case.id),
        "custom_external_complaint_number": case.complaint_number or "",
        "custom_complaint_source": case.source_system,
        "custom_sub_category": str(case.sub_category or "").strip(),
        "custom_tehsil": str(case.tehsil or "").strip(),
        "custom_date_received": case.created_at.date().isoformat(),
        "custom_paperless_document_id": case.canonical_paperless_document_id,
        "custom_paperless_case_url": paperless_document_url(case, settings) or "",
        "custom_deomee_case_url": crm_case_url(case, settings) or "",
    }
    return values


def payload_fingerprint(values: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(values),
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def ticket_payloads(
    case: ComplaintCase,
    settings: Settings,
    *,
    sync_version: int,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    update_payload = _base_payload(case, settings, sync_version)
    fingerprint = payload_fingerprint(update_payload)
    update_payload["custom_deomee_sync_version"] = sync_version
    update_payload["custom_deomee_payload_hash"] = fingerprint

    create_payload = {
        **update_payload,
        "raised_by": settings.frappe_helpdesk_default_raised_by,
        "status": settings.frappe_helpdesk_default_status,
        "priority": settings.frappe_helpdesk_default_priority,
        "via_customer_portal": 0,
        "custom_evidence_status": (
            "Paperless Archived" if case.canonical_paperless_document_id else "Not Archived"
        ),
        "custom_reply_approval_status": "Not Prepared",
        "custom_ai_eligible": 0,
    }
    return create_payload, update_payload, fingerprint
