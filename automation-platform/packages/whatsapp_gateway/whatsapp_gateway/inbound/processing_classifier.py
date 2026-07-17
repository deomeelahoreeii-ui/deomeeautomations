from __future__ import annotations

import csv
import re
import shutil
import subprocess
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import openpyxl

from crm_filters.pdf_extract import extract_crm_pdf

COMPLAINT_HEADING_TERMS = (
    "chief minister complaint cell",
    "complaint details",
    "applicant details",
    "complaint remarks",
    "complaint no",
    "complaint number",
)
REPLY_REPORT_TERMS = (
    "action taken report",
    "inquiry report",
    "field report",
    "compliance report",
    "reply",
    "response",
    "disposal",
    "remarks submitted",
)
OFFICIAL_DOCUMENT_TERMS = (
    "government of the punjab",
    "district education authority",
    "office of the",
    "subject:",
    "notification",
    "memorandum",
)
SUPPORTED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tif", ".tiff"}
SUPPORTED_SHEET_EXTENSIONS = {".xlsx", ".xls", ".csv", ".ods"}


@dataclass(frozen=True)
class ExtractionResult:
    text: str = ""
    method: str = ""
    metadata: dict[str, object] = field(default_factory=dict)
    error: str = ""


@dataclass(frozen=True)
class ClassificationResult:
    category: str
    complaint_number: str | None
    confidence: float
    evidence: list[str]
    all_complaint_numbers: list[str]
    needs_review: bool


def complaint_pattern(prefixes: Iterable[str], suffix_digits: int) -> re.Pattern[str]:
    values = [re.escape(value.strip()) for value in prefixes if value.strip()]
    if not values:
        values = ["104"]
    prefix_group = "|".join(values)
    return re.compile(
        rf"(?<!\d)({prefix_group})\s*(?:[-–—_/:]|\s)?\s*(\d{{{suffix_digits}}})(?!\d)",
        re.IGNORECASE,
    )


def normalize_complaint_numbers(
    text: str,
    *,
    prefixes: Iterable[str] = ("104",),
    suffix_digits: int = 7,
) -> list[str]:
    pattern = complaint_pattern(prefixes, suffix_digits)
    results: list[str] = []
    for match in pattern.finditer(text or ""):
        value = f"{match.group(1)}-{match.group(2)}"
        if value not in results:
            results.append(value)
    return results


def _compact(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _contains_any(value: str, terms: Iterable[str]) -> list[str]:
    folded = (value or "").casefold()
    return [term for term in terms if term.casefold() in folded]


def classify_extracted_document(
    *,
    filename: str,
    mime_type: str | None,
    extracted_text: str,
    extraction_method: str,
    caption: str = "",
    message_text: str = "",
    prefixes: Iterable[str] = ("104",),
    suffix_digits: int = 7,
) -> ClassificationResult:
    name_numbers = normalize_complaint_numbers(
        filename, prefixes=prefixes, suffix_digits=suffix_digits
    )
    content_numbers = normalize_complaint_numbers(
        extracted_text, prefixes=prefixes, suffix_digits=suffix_digits
    )
    caption = caption or ""
    message_text = message_text or ""
    context_numbers = normalize_complaint_numbers(
        "\n".join((caption, message_text)),
        prefixes=prefixes,
        suffix_digits=suffix_digits,
    )
    all_numbers = list(dict.fromkeys([*content_numbers, *name_numbers, *context_numbers]))
    evidence: list[str] = []
    score = 0.0
    if content_numbers:
        score += 0.45 if "tesseract" in extraction_method else 0.55
        evidence.append(
            "complaint_number_in_ocr_text"
            if "tesseract" in extraction_method
            else "complaint_number_in_document_text"
        )
    if name_numbers:
        score += 0.25
        evidence.append("complaint_number_in_filename")
    if context_numbers:
        score += 0.20
        evidence.append("complaint_number_in_whatsapp_context")

    combined = "\n".join((filename, extracted_text, caption, message_text))
    complaint_terms = _contains_any(combined, COMPLAINT_HEADING_TERMS)
    reply_terms = _contains_any(combined, REPLY_REPORT_TERMS)
    official_terms = _contains_any(combined, OFFICIAL_DOCUMENT_TERMS)
    if complaint_terms:
        score += 0.25
        evidence.extend(f"heading:{term}" for term in complaint_terms[:3])
    if "applicant details" in [term.casefold() for term in complaint_terms]:
        score += 0.10
        evidence.append("applicant_details_detected")
    score = min(score, 1.0)

    suffix = Path(filename).suffix.lower()
    is_pdf = suffix == ".pdf" or (mime_type or "").lower() == "application/pdf"
    is_image = suffix in SUPPORTED_IMAGE_EXTENSIONS or (mime_type or "").lower().startswith("image/")
    is_sheet = suffix in SUPPORTED_SHEET_EXTENSIONS or "spreadsheet" in (mime_type or "").lower()

    complaint_number = all_numbers[0] if len(all_numbers) == 1 else None
    needs_review = len(all_numbers) != 1 and bool(all_numbers)
    if len(all_numbers) > 1:
        evidence.append("multiple_complaint_numbers_detected")

    if complaint_number and is_sheet:
        return ClassificationResult(
            category="crm_supporting_document",
            complaint_number=complaint_number,
            confidence=max(score, 0.65),
            evidence=list(dict.fromkeys([*evidence, "spreadsheet_contains_crm_number"])),
            all_complaint_numbers=all_numbers,
            needs_review=True,
        )
    if complaint_number and reply_terms:
        evidence.extend(f"reply_indicator:{term}" for term in reply_terms[:3])
        return ClassificationResult(
            category="crm_reply_or_report",
            complaint_number=complaint_number,
            confidence=max(score, 0.75),
            evidence=list(dict.fromkeys(evidence)),
            all_complaint_numbers=all_numbers,
            needs_review=needs_review,
        )
    if complaint_number and complaint_terms and score >= 0.75:
        return ClassificationResult(
            category="crm_complaint",
            complaint_number=complaint_number,
            confidence=score,
            evidence=list(dict.fromkeys(evidence)),
            all_complaint_numbers=all_numbers,
            needs_review=needs_review,
        )
    if complaint_number and score >= 0.50:
        category = "possible_crm_complaint" if (is_pdf or is_image) else "crm_supporting_document"
        return ClassificationResult(
            category=category,
            complaint_number=complaint_number,
            confidence=score,
            evidence=list(dict.fromkeys(evidence)),
            all_complaint_numbers=all_numbers,
            needs_review=True,
        )
    if all_numbers:
        return ClassificationResult(
            category="possible_crm_complaint",
            complaint_number=None,
            confidence=max(score, 0.45),
            evidence=list(dict.fromkeys(evidence)),
            all_complaint_numbers=all_numbers,
            needs_review=True,
        )
    if is_sheet:
        return ClassificationResult("spreadsheet", None, 0.95, ["spreadsheet_file"], [], False)
    if is_image:
        return ClassificationResult("image", None, 0.85, ["image_file"], [], False)
    if is_pdf and official_terms:
        return ClassificationResult(
            "general_official_document",
            None,
            0.75,
            [*(f"official_indicator:{term}" for term in official_terms[:3])],
            [],
            False,
        )
    if is_pdf:
        return ClassificationResult("unknown", None, 0.35, ["pdf_without_crm_identifier"], [], True)
    return ClassificationResult("unsupported", None, 1.0, ["unsupported_file_type"], [], False)


def _run_tesseract(path: Path) -> str:
    executable = shutil.which("tesseract")
    if not executable:
        raise RuntimeError("Image OCR requires tesseract in PATH")
    process = subprocess.run(
        [executable, str(path), "stdout", "-l", "eng", "--oem", "1", "--psm", "6"],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if process.returncode != 0:
        raise RuntimeError(_compact(process.stderr) or f"tesseract exited with {process.returncode}")
    return process.stdout


def _spreadsheet_text(path: Path, *, limit: int) -> tuple[str, dict[str, object]]:
    suffix = path.suffix.lower()
    values: list[str] = []
    rows = 0
    sheets = 0
    if suffix == ".csv":
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            for row in csv.reader(handle):
                rows += 1
                values.extend(str(value) for value in row if value not in (None, ""))
                if sum(len(value) for value in values) >= limit:
                    break
        sheets = 1
    elif suffix == ".xlsx":
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            sheets = len(workbook.worksheets)
            for sheet in workbook.worksheets:
                for row in sheet.iter_rows(values_only=True):
                    rows += 1
                    values.extend(str(value) for value in row if value not in (None, ""))
                    if sum(len(value) for value in values) >= limit:
                        break
                if sum(len(value) for value in values) >= limit:
                    break
        finally:
            workbook.close()
    elif suffix == ".xls":
        import xlrd

        workbook = xlrd.open_workbook(path, on_demand=True)
        sheets = workbook.nsheets
        for index in range(workbook.nsheets):
            sheet = workbook.sheet_by_index(index)
            for row_index in range(sheet.nrows):
                rows += 1
                values.extend(str(value) for value in sheet.row_values(row_index) if value not in (None, ""))
                if sum(len(value) for value in values) >= limit:
                    break
            if sum(len(value) for value in values) >= limit:
                break
        workbook.release_resources()
    elif suffix == ".ods":
        table_ns = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
        text_ns = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
        with zipfile.ZipFile(path) as archive:
            root = ET.fromstring(archive.read("content.xml"))
        sheets = len(root.findall(f".//{{{table_ns}}}table"))
        rows = len(root.findall(f".//{{{table_ns}}}table-row"))
        for paragraph in root.findall(f".//{{{text_ns}}}p"):
            value = "".join(paragraph.itertext()).strip()
            if value:
                values.append(value)
            if sum(len(item) for item in values) >= limit:
                break
    else:
        raise RuntimeError(f"Unsupported spreadsheet extension: {suffix or '(none)'}")
    text = "\n".join(values)
    return text[:limit], {"rows_inspected": rows, "sheets_inspected": sheets}


def extract_document(path: Path, *, mime_type: str | None = None, text_limit: int = 100_000) -> ExtractionResult:
    suffix = path.suffix.lower()
    try:
        if suffix == ".pdf" or (mime_type or "").lower() == "application/pdf":
            complaint = extract_crm_pdf(path)
            text = "\n".join(part for part in (complaint.raw_text, complaint.ocr_text) if part)
            return ExtractionResult(
                text=text[:text_limit],
                method=complaint.extraction_method or "pdf",
                metadata={
                    "extractor_complaint_number": complaint.complaint_number,
                    "extractor_confidence": complaint.confidence,
                    "applicant_text": complaint.applicant_text[:5000],
                    "remarks_text": complaint.remarks_text[:10000],
                    "needs_review": complaint.needs_review,
                },
                error=complaint.error,
            )
        if suffix in SUPPORTED_IMAGE_EXTENSIONS or (mime_type or "").lower().startswith("image/"):
            text = _run_tesseract(path)
            return ExtractionResult(text=text[:text_limit], method="tesseract", metadata={})
        if suffix in SUPPORTED_SHEET_EXTENSIONS:
            text, metadata = _spreadsheet_text(path, limit=text_limit)
            return ExtractionResult(text=text, method=f"spreadsheet:{suffix.removeprefix('.')}", metadata=metadata)
        return ExtractionResult(method="unsupported", error="Unsupported file type")
    except Exception as exc:
        return ExtractionResult(method="failed", error=str(exc))
