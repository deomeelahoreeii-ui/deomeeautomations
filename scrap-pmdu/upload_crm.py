"""
CRM Complaints Uploader for Paperless-ngx
Save this file as upload_crm.py in deomeeautomations/scrap-pmdu/
Run with: uv run python upload_crm.py
"""

from __future__ import annotations

import asyncio
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path

# Import existing configurations and clients from your project
from main import configure_logging, load_paperless_settings
from paperless import PaperlessClient, custom_field_label, resolve_metadata

LOGGER_NAME = "pmdu_automation"


async def run_crm_upload(project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)

    # Load environment variables (.env) exactly like the main script
    settings = load_paperless_settings(project_root)

    # Define directories based on the user's requirements
    crm_dir = project_root / "crm-main-complaints"
    reports_dir = project_root / "reports"
    crm_reports_dir = reports_dir / "crm"
    pmdu_reports_dir = reports_dir / "pmdu"

    # 1. Create the required folders if they do not exist
    crm_reports_dir.mkdir(parents=True, exist_ok=True)
    pmdu_reports_dir.mkdir(parents=True, exist_ok=True)

    if not crm_dir.exists():
        logger.error(
            "CRM directory does not exist: %s. Please create it and add files.", crm_dir
        )
        return

    # Prepare the CSV report path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = crm_reports_dir / f"crm_existing_report_{timestamp}.csv"
    report_records = []

    # Initialize the async Paperless API Client
    async with PaperlessClient(settings) as client:
        try:
            logger.info(
                "Resolving Paperless metadata (custom fields, document types)..."
            )
            metadata = await resolve_metadata(client, settings)
        except Exception as e:
            logger.error("Failed to resolve paperless metadata: %s", e)
            return

        document_type_id = metadata["complaint_type_id"]
        correspondent_id = metadata["correspondent_id"]
        fields_by_name = metadata["custom_fields"]

        # 2. Scan CRM directory for documents
        files_to_process = [
            f for f in crm_dir.glob("*.*") if f.is_file() and not f.name.startswith(".")
        ]

        if not files_to_process:
            logger.info("No documents found in %s", crm_dir)
            return

        logger.info("Found %d file(s) in %s", len(files_to_process), crm_dir)

        # 3. Process each CRM file
        for file_path in files_to_process:
            title = file_path.stem  # E.g. "CRM-12345" from "CRM-12345.pdf"

            logger.info("Checking Paperless for CRM document: %s", title)
            doc_id = await client.find_document_by_title(title)

            # 4. If it already exists, fetch the current status and add to report
            if doc_id:
                logger.info(
                    "Complaint '%s' already exists (Paperless ID: %s). Fetching status...",
                    title,
                    doc_id,
                )
                existing_doc = await client.get_document(doc_id)

                # Resolving custom field 'Status' using your paperless.py helper
                status = custom_field_label(
                    existing_doc.get("custom_fields", []), fields_by_name, "Status"
                )

                report_records.append(
                    {
                        "Filename": file_path.name,
                        "Title": title,
                        "Paperless ID": doc_id,
                        "Current Status": status or "Pending/Unknown",
                        "Created At": existing_doc.get("created", ""),
                    }
                )

            # 5. If it does not exist, upload it
            else:
                if settings.dry_run:
                    logger.info("DRY RUN: Would upload new CRM complaint '%s'", title)
                else:
                    logger.info("Uploading new CRM complaint: %s", title)
                    try:
                        new_doc_id = await client.upload_document(
                            file_path=file_path,
                            title=title,
                            document_type_id=document_type_id,
                            correspondent_id=correspondent_id,
                        )
                        logger.info(
                            "Successfully uploaded '%s' (New ID: %s)", title, new_doc_id
                        )
                    except Exception as e:
                        logger.error("Failed to upload %s: %s", file_path.name, e)

    # 6. Generate the CSV report if there were duplicate/existing complaints
    if report_records:
        with open(report_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "Filename",
                    "Title",
                    "Paperless ID",
                    "Current Status",
                    "Created At",
                ],
            )
            writer.writeheader()
            writer.writerows(report_records)
        logger.info("Report of existing complaints generated at: %s", report_path)
    else:
        logger.info(
            "No existing complaints were found. All files were uploaded as new (or the folder was empty)."
        )


def main() -> int:
    logger = configure_logging()

    # Automatically resolve the correct project root (/home/ahmad/code/deomeeautomations/scrap-pmdu/)
    project_root = Path(__file__).resolve().parent

    try:
        logger.info("Starting CRM upload process...")
        asyncio.run(run_crm_upload(project_root))
        logger.info("CRM processing completed successfully.")
        return 0
    except KeyboardInterrupt:
        logger.info("Upload interrupted by user.")
        return 130
    except Exception:
        logger.exception("CRM upload failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
