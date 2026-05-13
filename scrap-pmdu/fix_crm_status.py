"""
Status Backfiller and CRM PDF Extractor for Paperless-ngx
Save this file as fix_crm_status.py in deomeeautomations/scrap-pmdu/
Run with: uv run python fix_crm_status.py
"""

from __future__ import annotations

import asyncio
import io
import logging
import re
import sys
from pathlib import Path

# Import existing configurations and clients from your project
from main import configure_logging, load_paperless_settings
from paperless import (
    PaperlessClient,
    custom_field_id,
    custom_field_payload,
    merge_custom_fields,
    resolve_metadata,
)

try:
    from pypdf import PdfReader
except ImportError:
    print("CRITICAL ERROR: 'pypdf' is required for PDF parsing.")
    print("Please run: uv add pypdf")
    sys.exit(1)

LOGGER_NAME = "pmdu_automation"


async def download_document(client: PaperlessClient, doc_id: int) -> bytes:
    """Safely downloads the original document from Paperless."""
    assert client.session is not None
    download_url = client.url(f"/api/documents/{doc_id}/download/")
    async with client.session.get(download_url, headers=client.headers) as response:
        response.raise_for_status()
        return await response.read()


def parse_crm_pdf(pdf_bytes: bytes) -> dict[str, str]:
    """
    Robust, non-fail strategy to extract CSV-like Key-Value pairs from the PDF.
    Handles broken newlines and weird spacing natively using Regex.
    """
    reader = PdfReader(io.BytesIO(pdf_bytes))
    full_text = ""
    for page in reader.pages:
        extracted = page.extract_text()
        if extracted:
            full_text += extracted + "\n"

    # The CRM PDFs store data in literal CSV format: "Key","Value"
    # This Regex finds all pairs, even if they contain hidden \n or \r
    matches = re.findall(r'"([^"]+)"\s*,\s*"([^"]*)"', full_text)

    data = {}
    for k, v in matches:
        clean_k = k.strip().replace("\n", "").replace("\r", "")
        clean_v = v.strip().replace("\n", "").replace("\r", "")
        if clean_k:
            data[clean_k] = clean_v

    return data


async def process_paperless_documents(project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    settings = load_paperless_settings(project_root)

    async with PaperlessClient(settings) as client:
        logger.info("Resolving Paperless metadata (Custom Fields, Doc Types)...")
        metadata = await resolve_metadata(client, settings)

        complaint_type_id = metadata["complaint_type_id"]
        fields_by_name = metadata["custom_fields"]

        status_field_id = custom_field_id(fields_by_name, "Status")
        if not status_field_id:
            logger.error("Could not find 'Status' custom field in Paperless.")
            return

        logger.info("Fetching ALL documents from Paperless to evaluate and parse...")
        documents = await client.paginated_results("/api/documents/?page_size=100")
        logger.info("Found %d total documents. Processing...", len(documents))

        updated_complaints = 0
        cleared_others = 0
        failed_parse = 0

        for doc in documents:
            doc_id = doc["id"]
            doc_type_id = doc.get("document_type")
            mime_type = doc.get("mime_type", "")
            existing_custom_fields = doc.get("custom_fields", [])

            # Check for Status field
            has_status_field = any(
                int(f["field"]) == status_field_id for f in existing_custom_fields
            )

            # ---------------------------------------------------------
            # 1. Logic for "Complaint" Document Types (The Main PDF)
            # ---------------------------------------------------------
            if doc_type_id == complaint_type_id:
                logger.info(
                    "Processing Complaint '%s' (ID: %s)...", doc.get("title"), doc_id
                )

                new_custom_fields = []

                # A. Always enforce Status = Pending for Complaints
                status_payload = custom_field_payload(
                    fields_by_name, "Status", "Pending"
                )
                if status_payload:
                    new_custom_fields.append(status_payload)

                # B. Download and parse PDF to get deep metadata
                if mime_type == "application/pdf":
                    try:
                        pdf_bytes = await download_document(client, doc_id)
                        extracted_data = parse_crm_pdf(pdf_bytes)

                        if extracted_data:
                            logger.debug(
                                "Extracted data for ID %s: %s", doc_id, extracted_data
                            )

                            # Map extracted PDF fields to Paperless Custom Fields
                            # The dict keys match your paperless_field_defaults.json mapping targets
                            mappings = {
                                "Complaint Number": extracted_data.get("Complaint No"),
                                "Complainant Name": extracted_data.get("Person Name"),
                                "Complainant Mobile Number": extracted_data.get(
                                    "Mobile No"
                                ),
                                "Complaint Category": extracted_data.get("Category"),
                            }

                            for field_name, value in mappings.items():
                                if value:
                                    payload = custom_field_payload(
                                        fields_by_name, field_name, value
                                    )
                                    if payload:
                                        new_custom_fields.append(payload)
                        else:
                            logger.warning(
                                "No tabular data extracted from PDF ID %s", doc_id
                            )

                    except Exception as e:
                        logger.error(
                            "Failed to download/parse PDF for ID %s: %s", doc_id, e
                        )
                        failed_parse += 1

                # C. Merge and Patch
                if settings.dry_run:
                    logger.info(
                        "DRY RUN: Would patch Complaint %s with %d fields",
                        doc_id,
                        len(new_custom_fields),
                    )
                else:
                    merged_fields = merge_custom_fields(
                        existing_custom_fields=existing_custom_fields,
                        new_custom_fields=new_custom_fields,
                        fields_by_name=fields_by_name,
                        clear_field_names=[],
                    )

                    try:
                        await client.patch_json(
                            f"/api/documents/{doc_id}/",
                            {"custom_fields": merged_fields},
                        )
                        updated_complaints += 1
                        logger.info(
                            "Successfully updated fields for Complaint ID %s", doc_id
                        )
                    except Exception as e:
                        logger.error("Failed to patch Complaint ID %s: %s", doc_id, e)

            # ---------------------------------------------------------
            # 2. Logic for Non-"Complaint" Document Types (Attachments)
            # ---------------------------------------------------------
            else:
                if has_status_field:
                    if settings.dry_run:
                        logger.info(
                            "DRY RUN: Would remove Status field from Other Doc '%s' (ID: %s)",
                            doc.get("title"),
                            doc_id,
                        )
                    else:
                        logger.info(
                            "Cleaning Other Doc '%s' (ID: %s) -> Removing Status field",
                            doc.get("title"),
                            doc_id,
                        )

                        # Retain all custom fields EXCEPT the Status field
                        retained_fields = [
                            f
                            for f in existing_custom_fields
                            if int(f["field"]) != status_field_id
                        ]

                        try:
                            await client.patch_json(
                                f"/api/documents/{doc_id}/",
                                {"custom_fields": retained_fields},
                            )
                            cleared_others += 1
                        except Exception as e:
                            logger.error(
                                "Failed to clear Status from Other Doc ID %s: %s",
                                doc_id,
                                e,
                            )

        logger.info("--- Data Backfill Complete ---")
        logger.info("Complaints Parsed & Updated: %d", updated_complaints)
        logger.info("Attachments Stripped of Status: %d", cleared_others)
        if failed_parse > 0:
            logger.warning("PDF Parse Failures: %d", failed_parse)


def main() -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    try:
        logger.info("Starting CRM PDF extraction and status cleanup...")
        asyncio.run(process_paperless_documents(project_root))
        return 0
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        return 130
    except Exception:
        logger.exception("Fatal error during processing.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
