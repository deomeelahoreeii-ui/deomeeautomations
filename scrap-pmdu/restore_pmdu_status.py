"""
DuckDB Status Restorer for Paperless-ngx
Save this file as restore_pmdu_status.py in deomeeautomations/scrap-pmdu/
Run with: uv run python restore_pmdu_status.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import duckdb

# Import existing configurations and clients from your project
from main import configure_logging, load_paperless_settings
from paperless import (
    PaperlessClient,
    custom_field_label,
    custom_field_payload,
    merge_custom_fields,
    resolve_metadata,
)

LOGGER_NAME = "pmdu_automation"


async def restore_statuses_from_duckdb(project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    settings = load_paperless_settings(project_root)

    # ---------------------------------------------------------
    # 1. Read the correct historical statuses from DuckDB
    # ---------------------------------------------------------
    logger.info("Connecting to DuckDB at %s", settings.duckdb_path)
    try:
        # Open in read-only mode to prevent any locking issues
        conn = duckdb.connect(str(settings.duckdb_path), read_only=True)

        # This query partitions the history by document ID and grabs the most recent status
        # that was successfully synced by the PMDU scraper.
        rows = conn.execute("""
            SELECT paperless_document_id, status_after
            FROM (
                SELECT paperless_document_id, status_after,
                       ROW_NUMBER() OVER(PARTITION BY paperless_document_id ORDER BY synced_at DESC) as rn
                FROM paperless_status_history
                WHERE paperless_document_id IS NOT NULL
            )
            WHERE rn = 1 AND status_after != ''
        """).fetchall()
        conn.close()
    except Exception as e:
        logger.error("Failed to read from DuckDB: %s", e)
        return

    # Create a fast lookup dictionary mapping {paperless_document_id: correct_status_label}
    db_statuses = {int(row[0]): str(row[1]) for row in rows}
    logger.info(
        "Found %d PMDU documents with valid historical statuses in DuckDB.",
        len(db_statuses),
    )

    if not db_statuses:
        logger.warning(
            "No historical statuses found. Did the PMDU scraper run at least once?"
        )
        return

    # ---------------------------------------------------------
    # 2. Connect to Paperless to apply the fixes
    # ---------------------------------------------------------
    async with PaperlessClient(settings) as client:
        logger.info("Resolving Paperless metadata...")
        metadata = await resolve_metadata(client, settings)

        complaint_type_id = metadata["complaint_type_id"]
        fields_by_name = metadata["custom_fields"]

        logger.info("Fetching Complaint documents from Paperless to evaluate...")
        documents = await client.paginated_results(
            f"/api/documents/?document_type={complaint_type_id}&page_size=100"
        )

        restored_count = 0
        already_correct_count = 0
        skipped_crm_count = 0

        for doc in documents:
            doc_id = doc["id"]

            # If the document ID is not in our DuckDB history, it's likely a CRM complaint. Leave it alone.
            if doc_id not in db_statuses:
                skipped_crm_count += 1
                continue

            correct_status_label = db_statuses[doc_id]
            existing_custom_fields = doc.get("custom_fields", [])

            # Determine what Paperless CURRENTLY says its status is
            current_status_label = custom_field_label(
                existing_custom_fields, fields_by_name, "Status"
            )

            # If it's already correct, do nothing
            if current_status_label == correct_status_label:
                already_correct_count += 1
                continue

            # We found a mismatch (e.g. Paperless says "Pending", DuckDB says "Resolved")
            if settings.dry_run:
                logger.info(
                    "DRY RUN: Would restore Doc '%s' (ID: %s) from '%s' back to '%s'",
                    doc.get("title"),
                    doc_id,
                    current_status_label,
                    correct_status_label,
                )
            else:
                logger.info(
                    "Restoring Doc '%s' (ID: %s) -> Changing '%s' to '%s'",
                    doc.get("title"),
                    doc_id,
                    current_status_label,
                    correct_status_label,
                )

                # Generate the payload for the correct status
                status_payload = custom_field_payload(
                    fields_by_name, "Status", correct_status_label
                )
                if not status_payload:
                    logger.warning(
                        "Could not resolve option ID for Status '%s'. Skipping.",
                        correct_status_label,
                    )
                    continue

                merged_fields = merge_custom_fields(
                    existing_custom_fields=existing_custom_fields,
                    new_custom_fields=[status_payload],
                    fields_by_name=fields_by_name,
                    clear_field_names=[],
                )

                try:
                    await client.patch_json(
                        f"/api/documents/{doc_id}/", {"custom_fields": merged_fields}
                    )
                    restored_count += 1
                except Exception as e:
                    logger.error("Failed to restore Document ID %s: %s", doc_id, e)

        logger.info("--- Restoration Complete ---")
        logger.info("Documents Restored successfully: %d", restored_count)
        logger.info("Documents Already Correct: %d", already_correct_count)
        logger.info("Ignored non-PMDU (CRM) Documents: %d", skipped_crm_count)


def main() -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    try:
        logger.info("Starting PMDU status restoration from DuckDB history...")
        asyncio.run(restore_statuses_from_duckdb(project_root))
        return 0
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        return 130
    except Exception:
        logger.exception("Fatal error during restoration.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
