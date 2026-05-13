"""
Advanced Reverse Sync & Auto-Healer: Mirror Full Paperless State to DuckDB
Save as pull_paperless_state.py in deomeeautomations/scrap-pmdu/
Run with: uv run python pull_paperless_state.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import duckdb

# Import existing configurations and clients from your project
from main import configure_logging, load_paperless_settings
from paperless import (
    PaperlessClient,
    custom_field_label,
    record_status_history,
    resolve_metadata,
)

LOGGER_NAME = "pmdu_automation"


def safe_connect_duckdb(
    db_path: str, retries: int = 5, delay: int = 2
) -> duckdb.DuckDBPyConnection:
    """Safely connect to DuckDB, waiting if another script (like main.py) currently has it locked."""
    logger = logging.getLogger(LOGGER_NAME)
    for attempt in range(retries):
        try:
            return duckdb.connect(db_path)
        except duckdb.IOException as e:
            if "database is locked" in str(e).lower() and attempt < retries - 1:
                logger.warning(
                    "DuckDB is locked by another process. Retrying in %ds...", delay
                )
                time.sleep(delay)
            else:
                raise
    raise RuntimeError("Could not acquire DuckDB lock after multiple attempts.")


def upgrade_duckdb_schema(
    conn: duckdb.DuckDBPyConnection, logger: logging.Logger
) -> None:
    """Safely adds the live_metadata column to the database if it doesn't exist."""
    try:
        conn.execute("ALTER TABLE paperless_documents ADD COLUMN live_metadata TEXT")
        logger.info("Upgraded DuckDB Schema: Added 'live_metadata' column.")
    except duckdb.ParserException:
        pass
    except Exception as e:
        if "already exists" not in str(e).lower():
            logger.warning("Schema check note: %s", e)


async def pull_full_state(project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    settings = load_paperless_settings(project_root)

    logger.info("Connecting to DuckDB at %s", settings.duckdb_path)
    conn = safe_connect_duckdb(str(settings.duckdb_path))

    try:
        upgrade_duckdb_schema(conn, logger)

        # Fetch all locally tracked documents that we think are successfully uploaded
        db_rows = conn.execute("""
            SELECT paperless_document_id, complaint_code, version, artifact_role, live_metadata,
                   (SELECT status_after FROM paperless_status_history psh
                    WHERE psh.paperless_document_id = pd.paperless_document_id
                    ORDER BY synced_at DESC LIMIT 1) as current_db_status
            FROM paperless_documents pd
            WHERE paperless_document_id IS NOT NULL
              AND upload_status = 'uploaded'
        """).fetchall()

        if not db_rows:
            logger.info("No tracked complaints found in DuckDB.")
            return

        tracked_docs = {int(row[0]): row for row in db_rows}
        logger.info(
            "Found %d tracked files in DuckDB. Fetching live state from Paperless...",
            len(tracked_docs),
        )

        updates_made = 0
        status_changes = 0
        ghosts_deleted = 0

        async with PaperlessClient(settings) as client:
            metadata = await resolve_metadata(client, settings)
            fields_by_name = metadata["custom_fields"]

            # Batch fetch ALL documents currently sitting on the live server
            live_documents = await client.paginated_results(
                "/api/documents/?page_size=100"
            )

            # Create a fast lookup set of all live IDs
            live_doc_ids = {int(doc["id"]) for doc in live_documents}
            live_docs_dict = {int(doc["id"]): doc for doc in live_documents}

            for doc_id, tracked_data in tracked_docs.items():
                _, complaint_code, version, role, db_metadata_str, current_db_status = (
                    tracked_data
                )

                # =========================================================
                # EDGE CASE 1: Ghost Files (Deleted in GUI, tracked in DB)
                # =========================================================
                if doc_id not in live_doc_ids:
                    if settings.dry_run:
                        logger.info(
                            "DRY RUN: Would auto-heal Ghost File %s (ID: %s).",
                            complaint_code,
                            doc_id,
                        )
                    else:
                        logger.warning(
                            "Ghost File Detected: %s (ID: %s) was deleted from the Paperless UI. Clearing local memory.",
                            complaint_code,
                            doc_id,
                        )

                        # Wipe the local memory so the forward-sync can re-upload it
                        conn.execute(
                            "DELETE FROM paperless_documents WHERE paperless_document_id = ?",
                            [doc_id],
                        )
                        conn.execute(
                            "DELETE FROM paperless_status_history WHERE paperless_document_id = ?",
                            [doc_id],
                        )
                        ghosts_deleted += 1
                    continue

                # =========================================================
                # STANDARD SYNC: Checking for manual Human Edits
                # =========================================================
                live_doc = live_docs_dict[doc_id]
                current_db_status = current_db_status or ""

                live_status = custom_field_label(
                    live_doc.get("custom_fields", []), fields_by_name, "Status"
                )

                live_metadata_json = json.dumps(live_doc)

                is_state_changed = db_metadata_str != live_metadata_json
                is_status_changed = (
                    (role == "main_complaint")
                    and bool(live_status)
                    and (live_status != current_db_status)
                )

                if is_state_changed or is_status_changed:
                    if settings.dry_run:
                        if is_status_changed:
                            logger.info(
                                "DRY RUN: Would log Status Update for %s: '%s' -> '%s'",
                                complaint_code,
                                current_db_status,
                                live_status,
                            )
                    else:
                        # Update the JSON Mirror in DuckDB
                        conn.execute(
                            """
                            UPDATE paperless_documents
                            SET live_metadata = ?
                            WHERE paperless_document_id = ?
                        """,
                            [live_metadata_json, doc_id],
                        )
                        updates_made += 1

                        if is_status_changed:
                            logger.info(
                                "Manual Status Edit Detected for %s (v%s): '%s' -> '%s'",
                                complaint_code,
                                version,
                                current_db_status,
                                live_status,
                            )

                            record_status_history(
                                conn=conn,
                                complaint_code=complaint_code,
                                version=version,
                                paperless_document_id=doc_id,
                                status_before=current_db_status,
                                status_after=live_status,
                                reason="manual_ui_update",
                            )
                            status_changes += 1

    finally:
        conn.close()

    logger.info("--- Reverse Sync Complete ---")
    logger.info("Total documents with updated local mirror: %d", updates_made)
    logger.info("Total manual UI status changes tracked: %d", status_changes)
    if ghosts_deleted > 0:
        logger.warning(
            "Total Ghost Files auto-healed (memory wiped): %d", ghosts_deleted
        )
        logger.info(
            "Run `uv run python main.py paperless` to automatically re-upload the deleted files."
        )


def main() -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    try:
        logger.info(
            "Starting Full Metadata Reverse Sync & Auto-Healer from Paperless-ngx..."
        )
        asyncio.run(pull_full_state(project_root))
        return 0
    except KeyboardInterrupt:
        logger.info("Process interrupted by user.")
        return 130
    except Exception:
        logger.exception("Fatal error during reverse sync.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
