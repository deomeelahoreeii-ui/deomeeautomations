from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from main import load_paperless_settings
from paperless import PaperlessClient, custom_field_label, resolve_metadata

from crm.store import connect, save_paperless_complaint
from crm.text_cleaning import clean_identity, clean_remarks

LOGGER = logging.getLogger("crm_paperless_index")


def custom_field_map(
    document: dict[str, Any], fields_by_name: dict[str, dict[str, Any]]
) -> dict[str, str]:
    result: dict[str, str] = {}
    for label in (
        "Complaint Number",
        "Complainant Name",
        "Complainant Mobile Number",
        "Complainant Address",
        "The Reported Entity Address",
        "District",
        "Tehsil",
        "Remarks",
        "Source",
        "Document Role",
        "Status",
    ):
        result[label] = custom_field_label(
            document.get("custom_fields", []), fields_by_name, label
        )
    return result


def applicant_text_from_fields(fields: dict[str, str]) -> str:
    return " ".join(
        value
        for value in (
            fields.get("Complainant Name", ""),
            fields.get("Complainant Mobile Number", ""),
            fields.get("Complainant Address", ""),
            fields.get("The Reported Entity Address", ""),
            fields.get("District", ""),
            fields.get("Tehsil", ""),
        )
        if value
    ).strip()


def is_crm_main_complaint(fields: dict[str, str]) -> bool:
    source = fields.get("Source", "").strip().lower()
    role = fields.get("Document Role", "").strip().lower()
    if source and source != "crm portal":
        return False
    return not role or role == "main complaint"


async def refresh_paperless_index_async(
    project_root: Path,
    db_path: Path,
    *,
    limit: int | None = None,
) -> int:
    settings = load_paperless_settings(project_root)
    count = 0
    with connect(db_path) as conn:
        async with PaperlessClient(settings) as client:
            metadata = await resolve_metadata(client, settings)
            fields_by_name = metadata["custom_fields"]
            path = (
                f"/api/documents/?document_type__id={metadata['complaint_type_id']}"
                "&page_size=100"
            )
            documents = await client.paginated_results(path)
            for document in documents:
                if limit is not None and count >= limit:
                    break
                document_id = int(document["id"])
                detailed = await client.get_document(document_id)
                fields = custom_field_map(detailed, fields_by_name)
                if not is_crm_main_complaint(fields):
                    continue

                content = str(detailed.get("content") or "")
                applicant_text = applicant_text_from_fields(fields)
                remarks_text = fields.get("Remarks", "") or content
                save_paperless_complaint(
                    conn,
                    document_id=document_id,
                    title=str(detailed.get("title") or ""),
                    complaint_number=fields.get("Complaint Number", "").strip(),
                    applicant_text=applicant_text,
                    applicant_clean=clean_identity(applicant_text),
                    remarks_text=remarks_text,
                    remarks_clean=clean_remarks(remarks_text),
                    content=content,
                    custom_fields=fields,
                )
                count += 1
                LOGGER.info("Indexed Paperless CRM document id=%s", document_id)
    return count


def refresh_paperless_index(
    project_root: Path,
    db_path: Path,
    *,
    limit: int | None = None,
) -> int:
    return asyncio.run(refresh_paperless_index_async(project_root, db_path, limit=limit))
