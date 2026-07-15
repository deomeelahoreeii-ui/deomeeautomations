"""
Create pending CRM compliance folders from a sample folder.

This script reads Paperless-ngx, finds main CRM complaints whose custom fields are:
  Source = CRM Portal
  Status = Pending

For each complaint number, it creates:
  crm-compliances/pending/<complaint-number>/

and copies the contents of the sample folder into it, replacing the sample complaint
number in filenames and supported file contents.

It also reads the Paperless document content/OCR text, extracts the complaint text
from/below the "Complaint Details" heading, and writes that text into the
"Complaint Details" table column of copied ODT files.

Run with:
  uv run python generate_crm_compliance_folders.py
  uv run python generate_crm_compliance_folders.py --sample-dir crm-compliances/pending/sample --pending-dir crm-compliances/pending
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from main import configure_logging, load_paperless_settings
from paperless import PaperlessClient, custom_field_label, resolve_metadata
from crm.reply_knowledge import (
    CONTEXT_OUTPUT_COMPLAINT_FOLDER,
    DEFAULT_CONTEXT_DIR,
    build_client_from_args,
    build_context_pack,
    read_pending_reports,
    write_context_pack_files,
)

LOGGER_NAME = "pmdu_automation"
CRM_COMPLAINT_RE = re.compile(r"\b\d{3}-\d{4,}\b")
CRM_COMPLAINT_REMARKS_RE = re.compile(
    r"\bcomplaint[\s_]+remarks\b\s*:?", re.IGNORECASE
)
CRM_REMARKS_TRAILING_FIELD_RE = re.compile(
    r"\s+\b(?:"
    r"escalation[\s_]+level|"
    r"created[\s_]+date|"
    r"last[\s_]+activity|"
    r"source[\s_]+call[\s_]+agent|"
    r"call[\s_]+agent|"
    r"attachments?|"
    r"processing[\s_]+history"
    r")\b\s*:?",
    re.IGNORECASE,
)
TEXT_SUFFIXES = {
    ".txt",
    ".csv",
    ".json",
    ".md",
    ".xml",
    ".html",
    ".htm",
    ".rtf",
    ".fodt",
}
DEFAULT_SAMPLE_DIR = "crm-compliances/pending/sample"
DEFAULT_PENDING_DIR = "crm-compliances/pending"
NOCODB_COMPOSE_RELATIVE_PATH = Path("..") / "complaints-remarks-nocodb" / "docker-compose.yml"

NS = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "table": "urn:oasis:names:tc:opendocument:xmlns:table:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "draw": "urn:oasis:names:tc:opendocument:xmlns:drawing:1.0",
    "fo": "urn:oasis:names:tc:opendocument:xmlns:xsl-fo-compatible:1.0",
    "style": "urn:oasis:names:tc:opendocument:xmlns:style:1.0",
    "svg": "urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0",
    "xlink": "http://www.w3.org/1999/xlink",
    "dc": "http://purl.org/dc/elements/1.1/",
    "meta": "urn:oasis:names:tc:opendocument:xmlns:meta:1.0",
    "number": "urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0",
    "presentation": "urn:oasis:names:tc:opendocument:xmlns:presentation:1.0",
    "of": "urn:oasis:names:tc:opendocument:xmlns:of:1.2",
}
for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)

TEXT_P_TAG = f"{{{NS['text']}}}p"
TABLE_ROW_TAG = f"{{{NS['table']}}}table-row"
TABLE_CELL_TAG = f"{{{NS['table']}}}table-cell"
TABLE_REPEATED_ATTR = f"{{{NS['table']}}}number-columns-repeated"


@dataclass(frozen=True)
class PendingComplaint:
    number: str
    details: str


def resolve_path(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def safe_relative(path: Path, base: Path) -> Path:
    try:
        return path.relative_to(base)
    except ValueError:
        return Path(path.name)


def normalize_label_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def clean_complaint_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\xa0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for raw_line in text.split("\n"):
        line = re.sub(r"[ \t\f\v]+", " ", raw_line).strip()
        if line:
            lines.append(line)
    text = "\n".join(lines)
    # For the report cell we want a clean paragraph without OCR-style blank lines or huge spacing.
    return re.sub(r"\s+", " ", text).strip()


def strip_leading_heading(text: str, heading: str) -> str:
    heading_re = re.compile(rf"^\s*{re.escape(heading)}\s*:?\s*", re.IGNORECASE)
    return heading_re.sub("", text).strip()


def extract_crm_complaint_remarks(text: str) -> str:
    match = CRM_COMPLAINT_REMARKS_RE.search(text)
    if not match:
        return ""

    remarks = text[match.end() :]
    stop = CRM_REMARKS_TRAILING_FIELD_RE.search(remarks)
    if stop:
        remarks = remarks[: stop.start()]
    return clean_complaint_text(remarks)


def extract_complaint_details_from_content(content: Any) -> str:
    """Extract the complaint narrative from the Paperless content/OCR text.

    The main expected pattern is a heading named "Complaint Details" followed by
    the complaint text. If another heading such as "Remarks" appears after that,
    text after that heading is removed.
    """
    raw = "" if content is None else str(content)
    raw = raw.replace("\xa0", " ")
    raw = raw.replace("\r\n", "\n").replace("\r", "\n")

    crm_remarks = extract_crm_complaint_remarks(raw)
    if crm_remarks:
        return crm_remarks

    marker = re.search(r"complaint\s+details\s*:?", raw, flags=re.IGNORECASE)
    if marker:
        raw = raw[marker.end() :]
        crm_remarks = extract_crm_complaint_remarks(raw)
        if crm_remarks:
            return crm_remarks

    # Stop at common headings that should not become part of the complaint details cell.
    stop_patterns = [
        r"\n\s*remarks\s*:?\s*\n",
        r"\n\s*complaint\s+status\s*:?\s*\n",
        r"\n\s*processing\s+history\s*:?\s*\n",
        r"\n\s*history\s*:?\s*\n",
        r"\n\s*attachments?\s*:?\s*\n",
    ]
    stop_indexes = []
    for pattern in stop_patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            stop_indexes.append(match.start())
    if stop_indexes:
        raw = raw[: min(stop_indexes)]

    cleaned = clean_complaint_text(raw)
    cleaned = strip_leading_heading(cleaned, "Complaint Details")
    return cleaned


def complaint_number_from_document(
    document: dict[str, Any], fields_by_name: dict[str, dict[str, Any]]
) -> str:
    from_field = custom_field_label(
        document.get("custom_fields", []), fields_by_name, "Complaint Number"
    )
    match = CRM_COMPLAINT_RE.search(from_field)
    if match:
        return match.group(0)

    for candidate in (
        str(document.get("title", "")),
        str(document.get("original_file_name", "")),
    ):
        match = CRM_COMPLAINT_RE.search(candidate)
        if match:
            return match.group(0)

    return ""


def is_pending_crm_complaint(
    document: dict[str, Any], fields_by_name: dict[str, dict[str, Any]]
) -> bool:
    custom_fields = document.get("custom_fields", [])
    source = custom_field_label(custom_fields, fields_by_name, "Source").strip().lower()
    status = custom_field_label(custom_fields, fields_by_name, "Status").strip().lower()
    role = custom_field_label(custom_fields, fields_by_name, "Document Role").strip().lower()

    if source != "crm portal":
        return False
    if status != "pending":
        return False
    # If Document Role is populated, keep only main complaints. If it is empty, let the
    # Paperless document type filter decide.
    if role and role != "main complaint":
        return False
    return True


def document_recency_key(document: dict[str, Any]) -> tuple[str, int]:
    document_id = document.get("id")
    try:
        numeric_id = int(document_id)
    except (TypeError, ValueError):
        numeric_id = 0
    return (str(document.get("added") or document.get("created") or ""), numeric_id)


async def fetch_pending_crm_complaints(project_root: Path) -> list[PendingComplaint]:
    logger = logging.getLogger(LOGGER_NAME)
    settings = load_paperless_settings(project_root)

    async with PaperlessClient(settings) as client:
        logger.info("Resolving Paperless metadata...")
        metadata = await resolve_metadata(client, settings)
        fields_by_name = metadata["custom_fields"]
        complaint_type_id = metadata["complaint_type_id"]

        logger.info("Fetching CRM complaint documents from Paperless...")
        documents = await client.paginated_results(
            f"/api/documents/?document_type__id={complaint_type_id}&page_size=100"
        )

        pending_documents_by_number: dict[str, list[dict[str, Any]]] = {}
        skipped_missing_number = 0
        for document in documents:
            if not is_pending_crm_complaint(document, fields_by_name):
                continue
            complaint_number = complaint_number_from_document(document, fields_by_name)
            if not complaint_number:
                skipped_missing_number += 1
                logger.warning(
                    "Skipping Paperless document without CRM complaint number: id=%s title=%s",
                    document.get("id"),
                    document.get("title"),
                )
                continue

            pending_documents_by_number.setdefault(complaint_number, []).append(document)

        pending_document_count = sum(
            len(items) for items in pending_documents_by_number.values()
        )
        duplicate_document_count = pending_document_count - len(pending_documents_by_number)
        logger.info(
            "Found %d pending CRM document(s) across %d unique complaint number(s).",
            pending_document_count,
            len(pending_documents_by_number),
        )
        if duplicate_document_count:
            logger.warning(
                "Found %d duplicate pending CRM document(s). One folder is created per complaint number.",
                duplicate_document_count,
            )

        result: list[PendingComplaint] = []
        missing_details = 0
        for complaint_number in sorted(pending_documents_by_number):
            matching_documents = sorted(
                pending_documents_by_number[complaint_number],
                key=document_recency_key,
                reverse=True,
            )
            document = matching_documents[0]
            if len(matching_documents) > 1:
                ignored_ids = ", ".join(
                    str(item.get("id", "")) for item in matching_documents[1:]
                )
                logger.warning(
                    "Duplicate pending CRM documents for %s; using newest id=%s and ignoring id(s)=%s.",
                    complaint_number,
                    document.get("id"),
                    ignored_ids,
                )

            # The list endpoint may not always include full OCR/content text. Fetch the
            # detailed document record so the Paperless Content tab is available.
            detail_document = document
            try:
                detail_document = await client.get_document(int(document["id"]))
            except Exception as exc:
                logger.warning(
                    "Could not fetch full Paperless document for %s (id=%s): %s",
                    complaint_number,
                    document.get("id"),
                    exc,
                )

            details = extract_complaint_details_from_content(
                detail_document.get("content") or document.get("content") or ""
            )
            if not details:
                missing_details += 1
                logger.warning(
                    "No complaint details text found in Paperless content for %s.",
                    complaint_number,
                )

            result.append(PendingComplaint(number=complaint_number, details=details))

        logger.info("Prepared %d unique pending CRM complaint(s).", len(result))
        if skipped_missing_number:
            logger.warning("Skipped %d pending CRM document(s) with no complaint number.", skipped_missing_number)
        if missing_details:
            logger.warning("%d complaint(s) had no extracted complaint details.", missing_details)
        return result


async def fetch_pending_crm_complaint_numbers(project_root: Path) -> list[str]:
    """Backward-compatible helper for any external import that used the old API."""
    return [complaint.number for complaint in await fetch_pending_crm_complaints(project_root)]


def detect_sample_complaint_number(sample_dir: Path) -> str:
    candidates: list[str] = []
    for path in sorted(sample_dir.rglob("*")):
        match = CRM_COMPLAINT_RE.search(path.name)
        if match:
            candidates.append(match.group(0))
    if candidates:
        return candidates[0]
    return ""


def replace_text_file(path: Path, replacements: dict[str, str]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return

    changed = False
    for old, new in replacements.items():
        if old and old in text:
            text = text.replace(old, new)
            changed = True
    if changed:
        path.write_text(text, encoding="utf-8")


def cell_text(cell: ET.Element) -> str:
    parts: list[str] = []
    for paragraph in cell.findall(f".//{TEXT_P_TAG}"):
        value = "".join(paragraph.itertext()).strip()
        if value:
            parts.append(value)
    if not parts:
        value = "".join(cell.itertext()).strip()
        if value:
            parts.append(value)
    return clean_complaint_text("\n".join(parts))


def expanded_cells(row: ET.Element) -> list[ET.Element]:
    cells: list[ET.Element] = []
    for cell in row.findall(TABLE_CELL_TAG):
        try:
            repeat_count = int(cell.attrib.get(TABLE_REPEATED_ATTR, "1"))
        except ValueError:
            repeat_count = 1
        repeat_count = max(1, repeat_count)
        cells.extend([cell] * repeat_count)
    return cells


def set_cell_text(cell: ET.Element, value: str) -> None:
    # Keep the cell attributes/style, remove existing paragraph/text children.
    for child in list(cell):
        cell.remove(child)

    paragraphs = [line.strip() for line in value.split("\n") if line.strip()]
    if not paragraphs and value.strip():
        paragraphs = [value.strip()]
    if not paragraphs:
        paragraphs = [""]

    for paragraph_text in paragraphs:
        paragraph = ET.Element(TEXT_P_TAG)
        paragraph.text = paragraph_text
        cell.append(paragraph)


def insert_complaint_details_into_content_xml(content_xml: Path, complaint_details: str) -> bool:
    if not complaint_details:
        return False

    tree = ET.parse(content_xml)
    root = tree.getroot()
    heading_key = normalize_label_text("Complaint Details")

    for table in root.findall(".//table:table", NS):
        rows = table.findall(TABLE_ROW_TAG)
        for row_index, row in enumerate(rows):
            cells = expanded_cells(row)
            for col_index, cell in enumerate(cells):
                text_key = normalize_label_text(cell_text(cell))
                if heading_key and heading_key in text_key:
                    for next_row in rows[row_index + 1 :]:
                        target_cells = expanded_cells(next_row)
                        if col_index < len(target_cells):
                            set_cell_text(target_cells[col_index], complaint_details)
                            tree.write(content_xml, encoding="utf-8", xml_declaration=True)
                            return True
    return False


def replace_odt_file(
    path: Path,
    replacements: dict[str, str],
    complaint_details: str = "",
) -> bool:
    inserted_details = False
    with TemporaryDirectory() as temp_name:
        temp_dir = Path(temp_name)
        with zipfile.ZipFile(path, "r") as source_zip:
            source_zip.extractall(temp_dir)

        for xml_path in temp_dir.rglob("*.xml"):
            replace_text_file(xml_path, replacements)

        content_xml = temp_dir / "content.xml"
        if content_xml.exists() and complaint_details:
            try:
                inserted_details = insert_complaint_details_into_content_xml(
                    content_xml, complaint_details
                )
            except Exception as exc:
                logging.getLogger(LOGGER_NAME).warning(
                    "Could not insert complaint details into %s: %s", path.name, exc
                )

        temp_output = path.with_suffix(path.suffix + ".tmp")
        try:
            with zipfile.ZipFile(temp_output, "w", compression=zipfile.ZIP_DEFLATED) as out_zip:
                mimetype = temp_dir / "mimetype"
                if mimetype.exists():
                    out_zip.write(mimetype, "mimetype", compress_type=zipfile.ZIP_STORED)
                for item in sorted(temp_dir.rglob("*")):
                    if not item.is_file() or item.name == "mimetype":
                        continue
                    out_zip.write(item, item.relative_to(temp_dir).as_posix())
            temp_output.replace(path)
        finally:
            if temp_output.exists():
                temp_output.unlink()
    return inserted_details


def update_existing_odt_details(target_dir: Path, complaint_details: str) -> int:
    if not complaint_details or not target_dir.exists():
        return 0
    updated = 0
    for odt_path in sorted(target_dir.rglob("*.odt")):
        if replace_odt_file(odt_path, {}, complaint_details=complaint_details):
            updated += 1
    return updated


def copy_sample_for_complaint(
    sample_dir: Path,
    target_root: Path,
    complaint_number: str,
    sample_complaint_number: str,
    complaint_details: str = "",
    overwrite: bool = False,
) -> tuple[Path, bool, int]:
    target_dir = target_root / complaint_number
    if target_dir.exists() and any(target_dir.iterdir()) and not overwrite:
        updated_details = update_existing_odt_details(target_dir, complaint_details)
        return target_dir, False, updated_details

    if target_dir.exists() and overwrite:
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    replacements = {
        "{complaint_number}": complaint_number,
        "{{complaint_number}}": complaint_number,
        "[complaint_number]": complaint_number,
        "[Complaint Number]": complaint_number,
        "COMPLAINT_NUMBER": complaint_number,
    }
    if sample_complaint_number:
        replacements[sample_complaint_number] = complaint_number

    updated_details = 0
    for source_path in sorted(sample_dir.rglob("*")):
        relative = safe_relative(source_path, sample_dir)
        relative_parts = []
        for part in relative.parts:
            new_part = part
            for old, new in replacements.items():
                if old:
                    new_part = new_part.replace(old, new)
            relative_parts.append(new_part)
        destination = target_dir.joinpath(*relative_parts)

        if source_path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue

        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination)

        suffix = destination.suffix.lower()
        if suffix == ".odt":
            if replace_odt_file(destination, replacements, complaint_details=complaint_details):
                updated_details += 1
        elif suffix in TEXT_SUFFIXES:
            replace_text_file(destination, replacements)

    return target_dir, True, updated_details


def create_pending_folders(
    complaints: list[PendingComplaint],
    sample_dir: Path,
    pending_dir: Path,
    overwrite: bool = False,
) -> tuple[int, int, int]:
    logger = logging.getLogger(LOGGER_NAME)
    if not sample_dir.exists() or not sample_dir.is_dir():
        raise RuntimeError(f"Sample folder not found: {sample_dir}")

    pending_dir.mkdir(parents=True, exist_ok=True)
    sample_complaint_number = detect_sample_complaint_number(sample_dir)
    if sample_complaint_number:
        logger.info("Detected sample complaint number: %s", sample_complaint_number)
    else:
        logger.warning(
            "No complaint number found in sample filenames. Only placeholder text will be replaced."
        )

    created = 0
    skipped = 0
    details_updated = 0
    for complaint in complaints:
        target_dir, did_create, updated_details = copy_sample_for_complaint(
            sample_dir=sample_dir,
            target_root=pending_dir,
            complaint_number=complaint.number,
            sample_complaint_number=sample_complaint_number,
            complaint_details=complaint.details,
            overwrite=overwrite,
        )
        details_updated += updated_details
        if did_create:
            created += 1
            logger.info("Prepared folder: %s", target_dir)
        else:
            skipped += 1
            logger.info("Skipped existing folder: %s", target_dir)
        if updated_details:
            logger.info("Inserted complaint details into %d ODT file(s) for %s.", updated_details, complaint.number)
        elif complaint.details:
            logger.warning("Could not find a Complaint Details table cell to update for %s.", complaint.number)
    return created, skipped, details_updated


def ensure_nocodb_stack_running(project_root: Path) -> None:
    logger = logging.getLogger(LOGGER_NAME)
    compose_path = (project_root / NOCODB_COMPOSE_RELATIVE_PATH).resolve()
    if not compose_path.exists():
        logger.warning("NocoDB compose file not found; skipping auto-start: %s", compose_path)
        return
    if not shutil.which("docker"):
        raise RuntimeError("Cannot auto-start NocoDB; docker command was not found.")

    logger.info("Ensuring NocoDB stack is running: %s", compose_path)
    try:
        result = subprocess.run(
            ["docker", "compose", "-f", str(compose_path), "up", "-d"],
            cwd=compose_path.parent,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Timed out while auto-starting the NocoDB Docker Compose stack.") from exc

    if result.returncode != 0:
        output = "\n".join(
            part.strip()
            for part in (result.stdout, result.stderr)
            if part and part.strip()
        )
        raise RuntimeError(
            "Could not auto-start the NocoDB Docker Compose stack."
            + (f"\n{output}" if output else "")
        )
    if result.stdout.strip():
        logger.info(result.stdout.strip())
    if result.stderr.strip():
        logger.info(result.stderr.strip())


async def run_async(args: argparse.Namespace, project_root: Path) -> int:
    logger = logging.getLogger(LOGGER_NAME)
    sample_dir = resolve_path(project_root, args.sample_dir)
    pending_dir = resolve_path(project_root, args.pending_dir)

    logger.info("Sample folder: %s", sample_dir)
    logger.info("Pending folder: %s", pending_dir)

    complaints = await fetch_pending_crm_complaints(project_root)
    if args.limit:
        complaints = complaints[: args.limit]
        logger.info("Limited run to %d complaint(s).", len(complaints))

    if not complaints:
        logger.info("No pending CRM complaints found. Nothing to create.")
        return 0

    if args.dry_run:
        logger.info("DRY RUN: Would create/update folders for these complaint numbers:")
        for complaint in complaints:
            logger.info("  %s -> %s", complaint.number, pending_dir / complaint.number)
            if complaint.details:
                logger.info("    Complaint details: %s", complaint.details[:180])
            else:
                logger.info("    Complaint details: [not found]")
        return 0

    created, skipped, details_updated = create_pending_folders(
        complaints=complaints,
        sample_dir=sample_dir,
        pending_dir=pending_dir,
        overwrite=args.overwrite,
    )
    logger.info(
        "Done. Created/updated folders: %d. Skipped existing folders: %d. ODT complaint-detail cells updated: %d.",
        created,
        skipped,
        details_updated,
    )
    if args.build_context_packs:
        try:
            ensure_nocodb_stack_running(project_root)
            client = build_client_from_args(args)
            prepared_numbers = {complaint.number for complaint in complaints}
            reports = [
                report
                for report in read_pending_reports(pending_dir)
                if report.complaint_number in prepared_numbers
            ]
            output_dir = resolve_path(project_root, args.context_output_dir)
            written = 0
            for report in reports:
                context_pack = build_context_pack(
                    client,
                    report,
                    top_examples=max(1, args.context_top_examples),
                    include_unverified_policy=args.include_unverified_policy,
                )
                json_path, prompt_path = write_context_pack_files(
                    context_pack,
                    report,
                    output_dir,
                    CONTEXT_OUTPUT_COMPLAINT_FOLDER,
                    write_prompt_text=True,
                )
                written += 1
                logger.info("Wrote LLM context files: %s%s", json_path, f", {prompt_path}" if prompt_path else "")
            logger.info("Built %d complaint-folder LLM context pack(s).", written)
        except Exception:
            logger.exception("Could not build LLM context packs after preparing folders.")
            return 1
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create CRM compliance pending folders from pending CRM complaints in Paperless."
    )
    parser.add_argument("--sample-dir", default=DEFAULT_SAMPLE_DIR, help="Sample folder to copy from.")
    parser.add_argument("--pending-dir", default=DEFAULT_PENDING_DIR, help="Pending folder where complaint folders are created.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of complaint folders to create.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing complaint folders.")
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating folders.")
    parser.add_argument(
        "--build-context-packs",
        action="store_true",
        help="Write LLM JSON/prompt files beside each prepared complaint ODT.",
    )
    parser.add_argument("--context-output-dir", default=DEFAULT_CONTEXT_DIR, help="Fallback central context folder.")
    parser.add_argument("--context-top-examples", type=int, default=5, help="Relevant examples per prompt.")
    parser.add_argument("--include-unverified-policy", action="store_true", help="Include unverified NocoDB policy rows in prompts.")
    parser.add_argument("--nocodb-url", default="", help="NocoDB base URL.")
    parser.add_argument("--nocodb-email", default="", help="NocoDB login email.")
    parser.add_argument("--nocodb-password", default="", help="NocoDB login password.")
    parser.add_argument("--nocodb-base", default="", help="NocoDB base title.")
    parser.add_argument("--nocodb-verify-ssl", action="store_true", help="Verify NocoDB HTTPS certificate.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_logging()
    project_root = Path(__file__).resolve().parent
    args = parse_args(argv or sys.argv[1:])
    try:
        return asyncio.run(run_async(args, project_root))
    except KeyboardInterrupt:
        logging.getLogger(LOGGER_NAME).info("Interrupted by user.")
        return 130
    except Exception:
        logging.getLogger(LOGGER_NAME).exception("Failed to prepare CRM compliance folders.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
