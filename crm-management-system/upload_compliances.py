"""
Compliance Reports Uploader for Paperless-ngx
Save this file as upload_compliances.py in deomeeautomations/crm-management-system/
Run with: uv run python upload_compliances.py
"""

from __future__ import annotations

import asyncio
import argparse
import csv
import copy
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# Import existing configurations and clients from your project
from main import configure_logging, load_paperless_settings
from paperless import (
    PaperlessClient,
    PaperlessSettings,
    by_name,
    resolve_metadata,
    select_option_id,
)

LOGGER_NAME = "pmdu_automation"
CRM_COMPLAINT_RE = re.compile(r"^\d{3}-\d{4,}$")
DEFAULT_COMPLIANCES_DIR = "crm-compliances"
DEFAULT_DISPATCH_CSV = "crm-compliances/crm-dispatch-list.csv"


@dataclass
class ReportInfo:
    path: Path
    complaint_code: str
    report_type: str  # 'deo', 'ddeo', or 'institute'
    title: str


class ComplianceUploader:
    def __init__(
        self, client: PaperlessClient, settings: PaperlessSettings, project_root: Path
    ):
        self.client = client
        self.settings = settings
        self.project_root = project_root
        self.logger = logging.getLogger(LOGGER_NAME)

        # State populated during setup
        self.metadata: dict[str, Any] = {}
        self.fields_by_name: dict[str, dict[str, Any]] = {}
        self.correspondent_id: int | None = None

    async def setup(self) -> None:
        """Fetch Paperless metadata and resolve IDs for correspondents and fields."""
        self.logger.info("Resolving Paperless metadata for compliance uploads...")
        self.metadata = await resolve_metadata(self.client, self.settings)
        self.fields_by_name = self.metadata["custom_fields"]
        self.correspondent_id = await self._get_or_create_correspondent("DEO-MEE")

    async def _get_or_create_correspondent(self, name: str) -> int:
        """Finds the correspondent by name, or creates it if it doesn't exist."""
        correspondents = by_name(
            await self.client.paginated_results("/api/correspondents/?page_size=1000")
        )
        if name.lower() in correspondents:
            return int(correspondents[name.lower()]["id"])

        if self.settings.dry_run:
            self.logger.info(f"DRY RUN: Would create missing correspondent '{name}'")
            return 999999

        self.logger.info(f"Correspondent '{name}' not found. Creating it...")
        async with self.client.session.post(
            self.client.url("/api/correspondents/"),
            headers=self.client.headers,
            json={"name": name},
        ) as response:
            response.raise_for_status()
            data = await response.json()
            return int(data["id"])

    def _get_field_id(self, field_name: str) -> int | None:
        field = self.fields_by_name.get(field_name.lower())
        return int(field["id"]) if field else None

    def _get_option_id(self, field_name: str, option_label: str) -> str | None:
        """Returns the alphanumeric ID of a select option."""
        field = self.fields_by_name.get(field_name.lower())
        if not field:
            return None
        return select_option_id(field, option_label)

    def _apply_custom_field(
        self,
        cfs: list[dict[str, Any]],
        field_name: str,
        value: Any,
        is_list: bool = False,
    ) -> None:
        """In-place update or append to a custom fields array, avoiding duplicates for lists."""
        field_id = self._get_field_id(field_name)
        if not field_id:
            self.logger.warning(
                f"Custom field '{field_name}' not found in Paperless. Skipping assignment."
            )
            return

        for cf in cfs:
            if int(cf.get("field", 0)) == field_id:
                if is_list:
                    current = cf.get("value")
                    if not current:
                        current = []
                    elif not isinstance(current, list):
                        current = [current]
                    if value not in current:
                        cf["value"] = current + [value]
                else:
                    cf["value"] = value
                return

        # Add new field object if it didn't exist
        cfs.append({"field": field_id, "value": [value] if is_list else value})

    def scan_directory(self, compliances_dir: Path) -> list[ReportInfo]:
        """Scan the compliance directory and classify files by their naming conventions."""
        reports = []
        if not compliances_dir.exists():
            self.logger.error(
                f"Compliances directory does not exist: {compliances_dir}"
            )
            return reports

        if CRM_COMPLAINT_RE.fullmatch(compliances_dir.name):
            complaint_dirs = [compliances_dir]
        else:
            complaint_dirs = [
                path
                for path in sorted(compliances_dir.iterdir())
                if path.is_dir()
                and path.name.lower() != "sample"
                and CRM_COMPLAINT_RE.fullmatch(path.name)
            ]

        for complaint_dir in complaint_dirs:
            if not complaint_dir.is_dir():
                continue

            complaint_code = complaint_dir.name

            for file_path in complaint_dir.glob("*.*"):
                if file_path.name.startswith("."):
                    continue

                name_lower = file_path.name.lower()
                report_type = None

                # Classify the report type dynamically based on filename patterns
                if "- deo report" in name_lower and "ddeo" not in name_lower:
                    report_type = "deo"
                elif "- ddeo report" in name_lower:
                    report_type = "ddeo"
                elif (
                    "- institute" in name_lower
                    or "- institue" in name_lower
                    or "institute report" in name_lower
                ):
                    report_type = "institute"

                if report_type:
                    reports.append(
                        ReportInfo(
                            path=file_path,
                            complaint_code=complaint_code,
                            report_type=report_type,
                            title=file_path.stem,
                        )
                    )
                else:
                    self.logger.warning(
                        f"Unrecognized compliance file format: {file_path.name}"
                    )

        return reports

    async def process_report(self, report: ReportInfo) -> bool:
        """Upload the report with specific compliance metadata, then link to the main complaint."""
        self.logger.info(
            f"Processing {report.report_type.upper()} compliance report: {report.title}"
        )

        # 1. Determine base Custom Fields for the newly uploaded compliance report
        report_cfs: list[dict[str, Any]] = []
        self._apply_custom_field(report_cfs, "Complaint Number", report.complaint_code)

        role_opt_id = self._get_option_id("Document Role", "Report Submission")
        if role_opt_id:
            self._apply_custom_field(report_cfs, "Document Role", role_opt_id)

        status_opt_id = self._get_option_id("Status", "Submitted")
        if status_opt_id:
            self._apply_custom_field(report_cfs, "Status", status_opt_id)

        # 2. Check if already uploaded (using filename title matching)
        doc_id = await self.client.find_document_by_title(report.title)

        if not doc_id:
            if self.settings.dry_run:
                self.logger.info(
                    f"DRY RUN: Would upload compliance report '{report.title}'"
                )
                doc_id = 999999  # Mock ID for dry-run processing
            else:
                doc_id = await self.client.upload_document(
                    file_path=report.path,
                    title=report.title,
                    document_type_id=self.metadata["attachment_type_id"],
                    correspondent_id=self.correspondent_id,
                )
                self.logger.info(
                    f"Uploaded compliance report '{report.title}' (ID: {doc_id})"
                )

                # Apply the specific fields required for compliance reports
                await self.client.patch_json(
                    f"/api/documents/{doc_id}/", {"custom_fields": report_cfs}
                )
        else:
            if not self.settings.dry_run:
                existing_doc = await self.client.get_document(doc_id)
                original_cfs = existing_doc.get("custom_fields", [])

                # Make a deepcopy to compare later for the FAST SKIP
                merged_cfs = copy.deepcopy(original_cfs)

                # Merge logic - overwriting with our strict standards
                for new_cf in report_cfs:
                    # Look up the name by ID dynamically to feed it to `_apply_custom_field`
                    field_name = next(
                        (
                            k
                            for k, v in self.fields_by_name.items()
                            if int(v["id"]) == new_cf["field"]
                        ),
                        None,
                    )
                    if field_name:
                        self._apply_custom_field(
                            merged_cfs,
                            field_name,
                            new_cf["value"],
                            is_list=isinstance(new_cf["value"], list),
                        )

                # --- FAST SKIP LOGIC ---
                needs_update = False
                if existing_doc.get("correspondent") != self.correspondent_id:
                    needs_update = True
                if original_cfs != merged_cfs:
                    needs_update = True

                if not needs_update:
                    self.logger.info(
                        f"⚡ FAST SKIP: No metadata updates needed for '{report.title}' (doc_id={doc_id}). Skipped API."
                    )
                else:
                    await self.client.patch_json(
                        f"/api/documents/{doc_id}/",
                        {
                            "custom_fields": merged_cfs,
                            "correspondent": self.correspondent_id,
                        },
                    )
                    self.logger.info(
                        f"Updated metadata for '{report.title}' (doc_id={doc_id})"
                    )

        # 3. Handle linking to the Main Complaint document
        return await self._link_to_main_complaint(report, doc_id)

    async def _link_to_main_complaint(
        self, report: ReportInfo, new_doc_id: int
    ) -> bool:
        """Finds the main complaint in Paperless and adds the compliance report reference ID."""
        # Query API for documents holding this complaint number
        results = await self.client.paginated_results(
            f"/api/documents/?query={report.complaint_code}"
        )

        role_field_id = self._get_field_id("Document Role")
        main_role_opt_id = self._get_option_id("Document Role", "Main Complaint")

        main_doc = None
        for doc in results:
            if report.complaint_code not in doc.get("title", ""):
                continue

            # Check if it has Document Role == Main Complaint
            is_main = False
            for cf in doc.get("custom_fields", []):
                if int(cf.get("field", 0)) == role_field_id and str(
                    cf.get("value")
                ) == str(main_role_opt_id):
                    is_main = True
                    break

            if is_main:
                main_doc = doc
                break

        if not main_doc:
            self.logger.warning(
                f"Could not find Main Complaint document '{report.complaint_code}' in Paperless. Link skipped."
            )
            return False

        self.logger.info(
            f"Found Main Complaint ID {main_doc['id']}. Checking compliance links..."
        )
        main_cfs = main_doc.get("custom_fields", [])

        # Deepcopy to check for FAST SKIP
        original_main_cfs = copy.deepcopy(main_cfs)

        # Link mapped appropriately based on the dynamic report type
        link_field_map = {
            "deo": "DEO MEE Report",
            "ddeo": "DDEO Report",
            "institute": "Institute Report",
        }
        target_link_field = link_field_map.get(report.report_type)

        if target_link_field:
            self._apply_custom_field(
                main_cfs, target_link_field, new_doc_id, is_list=True
            )

        # Business Rule: ONLY DEO MEE reports bump the Main Complaint Status to "Submitted"
        if report.report_type == "deo":
            status_opt_id = self._get_option_id("Status", "Submitted")
            if status_opt_id:
                self._apply_custom_field(main_cfs, "Status", status_opt_id)

        # --- FAST SKIP LOGIC ---
        if main_cfs == original_main_cfs:
            self.logger.info(
                f"⚡ FAST SKIP: Main Complaint ID {main_doc['id']} is already properly linked. Skipped API."
            )
            return True

        if not self.settings.dry_run:
            await self.client.patch_json(
                f"/api/documents/{main_doc['id']}/", {"custom_fields": main_cfs}
            )
            self.logger.info(
                f"Successfully linked and patched Main Complaint ID {main_doc['id']}"
            )
        return True


def resolve_project_path(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def append_dispatch_complaints(csv_path: Path, complaint_codes: set[str]) -> int:
    if not complaint_codes:
        return 0

    existing: set[str] = set()
    header = "Complaint No"
    if csv_path.exists():
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            rows = list(reader)
        if rows and rows[0]:
            header = rows[0][0].strip() or header
            data_rows = rows[1:]
        else:
            data_rows = rows
        for row in data_rows:
            if row and row[0].strip():
                existing.add(row[0].strip())

    new_codes = sorted(code for code in complaint_codes if code not in existing)
    if not new_codes:
        return 0

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        if write_header:
            writer.writerow([header])
        for code in new_codes:
            writer.writerow([code])
    return len(new_codes)


async def run_compliance_upload(project_root: Path, args: argparse.Namespace) -> int:
    logger = logging.getLogger(LOGGER_NAME)
    settings = load_paperless_settings(project_root)
    compliances_dir = resolve_project_path(project_root, args.compliances_dir)
    dispatch_csv = resolve_project_path(project_root, args.dispatch_csv)

    async with PaperlessClient(settings) as client:
        uploader = ComplianceUploader(client, settings, project_root)
        await uploader.setup()

        reports = uploader.scan_directory(compliances_dir)
        if not reports:
            logger.info("No compliance reports found to process.")
            return 0

        logger.info(
            "Found %d compliance report(s) under %s.",
            len(reports),
            compliances_dir,
        )

        failed = 0
        dispatch_codes: set[str] = set()
        for report in reports:
            try:
                linked = await uploader.process_report(report)
                if linked and report.report_type == "deo":
                    dispatch_codes.add(report.complaint_code)
            except Exception as e:
                failed += 1
                logger.error(
                    f"Failed to process compliance report {report.path.name}: {e}"
                )
                logger.exception(e)

        if settings.dry_run:
            logger.info("DRY RUN: Dispatch CSV not updated.")
        elif not args.no_update_dispatch_list:
            added = append_dispatch_complaints(dispatch_csv, dispatch_codes)
            logger.info(
                "Dispatch CSV updated: %s new complaint(s) added to %s.",
                added,
                dispatch_csv,
            )

        logger.info(
            "Compliance upload summary: reports=%d dispatch_candidates=%d failed=%d.",
            len(reports),
            len(dispatch_codes),
            failed,
        )
        return 1 if failed else 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Upload CRM compliance reports to Paperless.")
    parser.add_argument(
        "--compliances-dir",
        default=DEFAULT_COMPLIANCES_DIR,
        help="Folder containing compliance report folders, or one complaint folder.",
    )
    parser.add_argument(
        "--dispatch-csv",
        default=DEFAULT_DISPATCH_CSV,
        help="CSV to append DEO-uploaded complaint numbers for dispatch.",
    )
    parser.add_argument(
        "--no-update-dispatch-list",
        action="store_true",
        help="Do not append uploaded DEO report complaint numbers to the dispatch CSV.",
    )
    return parser.parse_args(argv)


def main() -> int:
    logger = configure_logging()
    project_root = Path(__file__).resolve().parent
    args = parse_args(sys.argv[1:])

    try:
        logger.info("Starting Compliance Upload process...")
        exit_code = asyncio.run(run_compliance_upload(project_root, args))
        logger.info("Compliance upload process completed.")
        return exit_code
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        return 130
    except Exception:
        logger.exception("Compliance upload failed critically.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
