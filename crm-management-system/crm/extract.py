from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

import pdfplumber

from crm.store import ExtractedComplaint
from crm.text_cleaning import clean_identity, clean_remarks

LOGGER = logging.getLogger("crm_extract")
CRM_COMPLAINT_RE = re.compile(r"\b(?:104-\d{7}|000-\d{4,})\b")
CRM_LABEL_RE = re.compile(r"complaint\s*(?:id|no|number|code)", re.IGNORECASE)
CRM_104_LOOSE_RE = re.compile(r"(?<!\d)(104)\s*[-:–—]?\s*(\d{7})(?!\d)")
COMPLAINT_OVERRIDES = "crm-complaint-overrides.json"


FIELD_ALIASES = {
    "complaint_number": (
        "complaint no",
        "complaint number",
        "complaint id",
        "complaint code",
    ),
    "person_name": ("person name", "applicant name", "complainant name", "name"),
    "mobile": ("mobile no", "mobile number", "phone", "contact"),
    "cnic": ("cnic no", "cnic"),
    "address": ("person address", "applicant address", "address"),
    "district": ("complaint district", "district"),
    "tehsil": ("tehsil",),
    "remarks": ("complaint remarks", "remarks", "complaint details", "description"),
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def compact_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def parse_key_values_from_text(text: str) -> dict[str, str]:
    data: dict[str, str] = {}
    for raw_line in (text or "").splitlines():
        line = compact_text(raw_line)
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = normalize_key(key)
        value = compact_text(value)
        if key and value and key not in data:
            data[key] = value
    return data


def parse_pdf_text(pdf_path: Path) -> tuple[str, dict[str, str]]:
    raw_text_parts: list[str] = []
    fields: dict[str, str] = {}
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables() or []:
                for row in table or []:
                    if len(row) < 2 or not row[0] or not row[1]:
                        continue
                    key = normalize_key(str(row[0]).replace(":", ""))
                    if key and key not in fields:
                        fields[key] = compact_text(str(row[1]))

            text = page.extract_text() or ""
            if text:
                raw_text_parts.append(text)
                for key, value in parse_key_values_from_text(text).items():
                    fields.setdefault(key, value)

    raw_text = "\n".join(raw_text_parts)
    return raw_text, fields


def field_value(fields: dict[str, str], logical_name: str) -> str:
    aliases = FIELD_ALIASES.get(logical_name, ())
    for alias in aliases:
        normalized_alias = normalize_key(alias)
        for key, value in fields.items():
            if key == normalized_alias:
                return compact_text(value)
    for alias in aliases:
        normalized_alias = normalize_key(alias)
        for key, value in fields.items():
            if normalized_alias in key:
                return compact_text(value)
    return ""


def complaint_numbers_in(text: str) -> list[str]:
    value = text or ""
    numbers: list[str] = [match.group(0) for match in CRM_COMPLAINT_RE.finditer(value)]

    # OCR often reads the dark "Complaint ID: 104-xxxxxxx" label as
    # "ComplaintID:104:xxxxxxx" or "ComplaintID:104xxxxxxx".  Keep this
    # deliberately narrow so CNIC/mobile numbers do not become complaint IDs.
    search_windows = [value]
    for label_match in CRM_LABEL_RE.finditer(value):
        search_windows.append(value[label_match.start() : label_match.end() + 80])
    for window in search_windows:
        for match in CRM_104_LOOSE_RE.finditer(window):
            numbers.append(f"{match.group(1)}-{match.group(2)}")

    deduped: list[str] = []
    seen: set[str] = set()
    for number in numbers:
        if number not in seen:
            deduped.append(number)
            seen.add(number)
    return deduped


def reviewed_complaint_number_override(pdf_path: Path) -> str:
    for folder in (
        pdf_path.parent,
        pdf_path.parent / "crm-ocr-successful",
        pdf_path.parent / "crm-ocr-unsuccessful",
    ):
        override_path = folder / COMPLAINT_OVERRIDES
        if not override_path.exists():
            continue
        try:
            raw = json.loads(override_path.read_text(encoding="utf-8"))
        except Exception as exc:
            LOGGER.warning("Could not read %s: %s", override_path, exc)
            continue
        value = str(raw.get(pdf_path.name) or "").strip() if isinstance(raw, dict) else ""
        if CRM_COMPLAINT_RE.fullmatch(value):
            return value
    return ""


def render_first_page_for_tesseract(pdf_path: Path, temp_dir: Path) -> Path:
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        raise RuntimeError("OCR needs poppler/pdftoppm. Install package: poppler")
    output_base = temp_dir / pdf_path.stem
    subprocess.run(
        [
            pdftoppm,
            "-png",
            "-singlefile",
            "-f",
            "1",
            "-l",
            "1",
            "-r",
            "350",
            str(pdf_path),
            str(output_base),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return output_base.with_suffix(".png")


def tesseract_ocr_first_page(pdf_path: Path) -> str:
    if not shutil.which("tesseract"):
        raise RuntimeError("OCR needs tesseract. Install package: tesseract")
    try:
        import pytesseract
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError(
            "OCR needs Python packages: pytesseract pillow"
        ) from exc

    with tempfile.TemporaryDirectory(prefix="crm-tesseract-") as temp_dir:
        image_path = render_first_page_for_tesseract(pdf_path, Path(temp_dir))
        image = Image.open(image_path)
        text_parts = [
            pytesseract.image_to_string(
                image,
                lang="eng",
                config="--oem 1 --psm 6",
            )
        ]
        label_numbers = ocr_complaint_label_numbers(image)
        if label_numbers:
            text_parts.extend(f"Complaint ID: {number}" for number in label_numbers)
        return "\n".join(text_parts)


def ocr_complaint_label_candidate_scores(image) -> list[tuple[str, float]]:
    try:
        import pytesseract
        from PIL import ImageEnhance, ImageOps
    except ImportError:
        return []

    width, height = image.size
    crop_boxes = (
        (int(width * 0.38), int(height * 0.08), int(width * 0.62), int(height * 0.14)),
        (int(width * 0.39), int(height * 0.095), int(width * 0.61), int(height * 0.15)),
        (int(width * 0.36), int(height * 0.105), int(width * 0.64), int(height * 0.165)),
        (int(width * 0.40), int(height * 0.115), int(width * 0.60), int(height * 0.155)),
        (int(width * 0.30), int(height * 0.075), int(width * 0.70), int(height * 0.18)),
    )
    scores: dict[str, float] = defaultdict(float)
    for box in crop_boxes:
        crop = image.crop(box)
        gray = ImageOps.grayscale(crop)
        variants = (
            (gray.resize((gray.width * 6, gray.height * 6)), 1.0),
            (ImageOps.invert(gray).resize((gray.width * 6, gray.height * 6)), 1.0),
            (
                ImageEnhance.Sharpness(
                    ImageEnhance.Contrast(ImageOps.invert(gray)).enhance(4.0)
                )
                .enhance(2.0)
                .resize((gray.width * 6, gray.height * 6)),
                1.5,
            ),
        )
        for variant, weight in variants:
            text = pytesseract.image_to_string(
                variant,
                lang="eng",
                config=(
                    "--oem 1 --psm 6 "
                    "-c tessedit_char_whitelist=ComplaintID:0123456789-"
                ),
            )
            for match in CRM_COMPLAINT_RE.finditer(text):
                scores[match.group(0)] += weight
            for match in CRM_104_LOOSE_RE.finditer(text):
                raw = match.group(0)
                number = f"{match.group(1)}-{match.group(2)}"
                separator_bonus = 1.0 if re.search(r"104\s*[-:–—]", raw) else 0.2
                scores[number] += weight * separator_bonus

    if not scores:
        return []
    ranked = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    return [(number, round(score, 3)) for number, score in ranked]


def ocr_complaint_label_numbers(image) -> list[str]:
    ranked = ocr_complaint_label_candidate_scores(image)
    if not ranked:
        return []
    best_number, best_score = ranked[0]
    second_score = ranked[1][1] if len(ranked) > 1 else 0.0
    if best_score >= 2.0 and best_score >= second_score + 1.0:
        return [best_number]
    LOGGER.warning(
        "Ambiguous CRM complaint-label OCR candidates: %s",
        ", ".join(f"{number}={score:.2f}" for number, score in ranked[:5]),
    )
    return []


def complaint_label_review_candidates(pdf_path: Path) -> list[dict[str, Any]]:
    if not shutil.which("tesseract"):
        raise RuntimeError("OCR needs tesseract. Install package: tesseract")
    try:
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError("OCR review needs Python package: pillow") from exc

    with tempfile.TemporaryDirectory(prefix="crm-number-review-") as temp_dir:
        image_path = render_first_page_for_tesseract(pdf_path, Path(temp_dir))
        image = Image.open(image_path)
        return [
            {"complaint_number": number, "score": score}
            for number, score in ocr_complaint_label_candidate_scores(image)
        ]


def build_applicant_text(fields: dict[str, str], raw_text: str) -> str:
    parts = [
        field_value(fields, "person_name"),
        field_value(fields, "mobile"),
        field_value(fields, "cnic"),
        field_value(fields, "address"),
        field_value(fields, "district"),
        field_value(fields, "tehsil"),
    ]
    applicant = " ".join(part for part in parts if part)
    if applicant:
        return compact_text(applicant)

    match = re.search(
        r"(person\s+name|applicant\s+details?|complainant).*?(complaint\s+remarks|remarks|complaint\s+details)",
        raw_text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    return compact_text(match.group(0)) if match else ""


def build_remarks_text(fields: dict[str, str], raw_text: str) -> str:
    remarks = field_value(fields, "remarks")
    if remarks:
        return remarks
    match = re.search(
        r"(complaint\s+remarks|remarks|complaint\s+details)\s*:?\s*(.+)",
        raw_text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    return compact_text(match.group(2)) if match else ""


def extract_crm_pdf(pdf_path: Path) -> ExtractedComplaint:
    source_sha256 = sha256_file(pdf_path)
    raw_text = ""
    ocr_text = ""
    method = "pdf_text"
    error = ""

    try:
        raw_text, fields = parse_pdf_text(pdf_path)
    except Exception as exc:
        fields = {}
        error = f"pdf text extraction failed: {exc}"
        LOGGER.warning("%s: %s", pdf_path.name, error)

    if len(compact_text(raw_text)) < 80:
        try:
            ocr_text = tesseract_ocr_first_page(pdf_path)
            method = "tesseract"
            for key, value in parse_key_values_from_text(ocr_text).items():
                fields.setdefault(key, value)
        except Exception as exc:
            error = f"{error} OCR failed: {exc}".strip()
            LOGGER.warning("%s: %s", pdf_path.name, error)

    combined_text = "\n".join(part for part in (raw_text, ocr_text) if part)
    number_source = ""
    complaint_number = reviewed_complaint_number_override(pdf_path)
    if complaint_number:
        number_source = "reviewed_override"
    if not complaint_number:
        field_numbers = complaint_numbers_in(field_value(fields, "complaint_number"))
        complaint_number = field_numbers[0] if len(set(field_numbers)) == 1 else ""
        if complaint_number:
            number_source = "pdf_text"
    if not complaint_number:
        numbers = complaint_numbers_in(combined_text or pdf_path.name)
        complaint_number = numbers[0] if len(set(numbers)) == 1 else ""
        if complaint_number:
            number_source = "text_search"

    applicant_text = build_applicant_text(fields, combined_text)
    remarks_text = build_remarks_text(fields, combined_text)
    applicant_clean = clean_identity(applicant_text)
    remarks_clean = clean_remarks(remarks_text)

    confidence = 0.0
    if complaint_number:
        confidence += 0.70 if number_source == "reviewed_override" else 0.45
    if applicant_clean:
        confidence += 0.20
    if remarks_clean:
        confidence += 0.30
    if method == "pdf_text":
        confidence += 0.05

    return ExtractedComplaint(
        source_path=str(pdf_path),
        source_sha256=source_sha256,
        complaint_number=complaint_number,
        applicant_text=applicant_text,
        applicant_clean=applicant_clean,
        remarks_text=remarks_text,
        remarks_clean=remarks_clean,
        raw_text=raw_text,
        ocr_text=ocr_text,
        extraction_method=f"{method}+{number_source}" if number_source else method,
        confidence=min(confidence, 1.0),
        needs_review=(
            False
            if number_source == "reviewed_override"
            else not complaint_number or confidence < 0.60 or bool(error)
        ),
        error=error,
    )
