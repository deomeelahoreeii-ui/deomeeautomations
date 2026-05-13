"""
Old Version Cleanup for Paperless-ngx (Main Complaints ONLY)
Save this file as delete_old_versions.py in deomeeautomations/scrap-pmdu/
Run with: uv run python delete_old_versions.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

import duckdb

# Import existing configurations and clients from your project
from main import configure_logging, load_paperless_settings
from paperless import PaperlessClient

LOGGER_NAME = "pmdu_automation"


async def delete_old_versions(project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    settings = load_paperless_settings(project_root)

    logger.info("Connecting to DuckDB at %s", settings.duckdb_path)
    conn = duckdb.connect(str(settings.duckdb_path))

    try:
        # ---------------------------------------------------------
        # 1. Query DuckDB to find ONLY older 'main_complaint' files
        # ---------------------------------------------------------
        query = """
            WITH MaxVersions AS (
                SELECT complaint_code, MAX(version) as max_version
                FROM paperless_documents
                WHERE paperless_document_id IS NOT NULL
                GROUP BY complaint_code
            )
            SELECT pd.paperless_document_id, pd.complaint_code, pd.version, pd.artifact_role
            FROM paperless_documents pd
            JOIN MaxVersions mv ON pd.complaint_code = mv.complaint_code
            WHERE pd.version < mv.max_version
              AND pd.artifact_role = 'main_complaint'  -- STRICTLY FILTER FOR MAIN PDFs ONLY
              AND pd.paperless_document_id IS NOT NULL
            ORDER BY pd.complaint_code, pd.version
        """
        old_docs = conn.execute(query).fetchall()

        if not old_docs:
            logger.info("No older main complaints found. Instance is clean.")
            return

        logger.info(
            "Found %d older MAIN COMPLAINTS slated for deletion. (Attachments will be ignored).",
            len(old_docs),
        )

        # ---------------------------------------------------------
        # 2. Delete the documents from Paperless-ngx
        # ---------------------------------------------------------
        deleted_count = 0
        failed_count = 0

        async with PaperlessClient(settings) as client:
            assert client.session is not None

            for doc_id, code, version, role in old_docs:
                doc_id = int(doc_id)

                if settings.dry_run:
                    logger.info(
                        "DRY RUN: Would delete Doc ID %s -> %s (v%s, %s)",
                        doc_id,
                        code,
                        version,
                        role,
                    )
                    continue

                logger.info(
                    "Deleting Doc ID %s -> %s (v%s, %s)...", doc_id, code, version, role
                )

                try:
                    delete_url = client.url(f"/api/documents/{doc_id}/")
                    async with client.session.delete(
                        delete_url, headers=client.headers
                    ) as response:
                        if response.status not in (204, 404):
                            response.raise_for_status()

                    # Clean up local database
                    conn.execute(
                        "DELETE FROM paperless_documents WHERE paperless_document_id = ?",
                        [doc_id],
                    )
                    conn.execute(
                        "DELETE FROM paperless_status_history WHERE paperless_document_id = ?",
                        [doc_id],
                    )

                    deleted_count += 1

                except Exception as e:
                    logger.error("Failed to delete Doc ID %s: %s", doc_id, e)
                    failed_count += 1

        logger.info("--- Cleanup Complete ---")
        if not settings.dry_run:
            logger.info("Successfully deleted %d old main complaints.", deleted_count)

    finally:
        conn.close()


def main() -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    try:
        logger.info("Starting Paperless-ngx old version cleanup (Main PDFs only)...")
        asyncio.run(delete_old_versions(project_root))
        return 0
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        return 130
    except Exception:
        logger.exception("Fatal error during cleanup.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
