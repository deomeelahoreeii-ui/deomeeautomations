from __future__ import annotations

import hashlib
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageEnhance, ImageOps

from crm_filters.text_cleaning import clean_identity, clean_remarks


CRM_COMPLAINT_RE = re.compile(r"\b(?:104-\d{7}|000-\d{4,})\b")
CRM_LABEL_RE = re.compile(r"complaint\s*(?:id|no|number|code)", re.IGNORECASE)
CRM_104_LOOSE_RE = re.compile(r"(?<!\d)(104)\s*[-:–—]?\s*(\d{7})(?!\d)")

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


@dataclass(frozen=True)
class ExtractedComplaint:
    source_path: str
    source_sha256: str
    complaint_number: str = ""
    applicant_text: str = ""
    applicant_clean: str = ""
    remarks_text: str = ""
    remarks_clean: str = ""
    raw_text: str = ""
    ocr_text: str = ""
    extraction_method: str = ""
    confidence: float = 0.0
    needs_review: bool = False
    error: str = ""
    page_count: int = 0
    ocr_pages: tuple[int, ...] = ()


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
        line = raw_line.strip()
        if not line:
            continue
        if ":" in line:
            key, value = line.split(":", 1)
        else:
            parts = re.split(r"\s{2,}", line, maxsplit=1)
            if len(parts) != 2:
                continue
            key, value = parts
        normalized = normalize_key(key)
        compact = compact_text(value)
        if normalized and compact and normalized not in data:
            data[normalized] = compact
    return data


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
    numbers = [match.group(0) for match in CRM_COMPLAINT_RE.finditer(value)]
    windows = [value]
    for label_match in CRM_LABEL_RE.finditer(value):
        windows.append(value[label_match.start() : label_match.end() + 80])
    for window in windows:
        for match in CRM_104_LOOSE_RE.finditer(window):
            numbers.append(f"{match.group(1)}-{match.group(2)}")
    return list(dict.fromkeys(numbers))


def extract_pdf_text(pdf_path: Path) -> str:
    pdftotext = shutil.which("pdftotext")
    if not pdftotext:
        raise RuntimeError("PDF text extraction needs poppler/pdftotext. Install package: poppler")
    process = subprocess.run(
        [pdftotext, "-layout", "-enc", "UTF-8", str(pdf_path), "-"],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if process.returncode != 0:
        detail = compact_text(process.stderr) or f"pdftotext exited with code {process.returncode}"
        raise RuntimeError(detail)
    return process.stdout


def render_page(pdf_path: Path, output_path: Path, *, page_number: int) -> Path:
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        raise RuntimeError("OCR needs poppler/pdftoppm. Install package: poppler")
    output_base = output_path.with_suffix("")
    process = subprocess.run(
        [
            pdftoppm,
            "-png",
            "-singlefile",
            "-f",
            str(page_number),
            "-l",
            str(page_number),
            "-r",
            "300",
            str(pdf_path),
            str(output_base),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    rendered = output_base.with_suffix(".png")
    if process.returncode != 0 or not rendered.is_file():
        detail = compact_text(process.stderr) or "pdftoppm did not create the first-page image"
        raise RuntimeError(detail)
    return rendered


def _run_tesseract(image_path: Path, *, psm: int = 6, whitelist: str = "") -> str:
    tesseract = shutil.which("tesseract")
    if not tesseract:
        raise RuntimeError("OCR needs tesseract. Install package: tesseract")
    command = [tesseract, str(image_path), "stdout", "-l", "eng", "--oem", "1", "--psm", str(psm)]
    if whitelist:
        command.extend(["-c", f"tessedit_char_whitelist={whitelist}"])
    process = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if process.returncode != 0:
        detail = compact_text(process.stderr) or f"tesseract exited with code {process.returncode}"
        raise RuntimeError(detail)
    return process.stdout


def _ocr_label_crop(image: Image.Image, temp_dir: Path) -> str:
    width, height = image.size
    crop = image.crop(
        (int(width * 0.28), int(height * 0.065), int(width * 0.72), int(height * 0.19))
    )
    gray = ImageOps.grayscale(crop)
    enhanced = ImageEnhance.Sharpness(
        ImageEnhance.Contrast(ImageOps.invert(gray)).enhance(4.0)
    ).enhance(2.0)
    resized = enhanced.resize((max(1, enhanced.width * 5), max(1, enhanced.height * 5)))
    crop_path = temp_dir / "complaint-label.png"
    resized.save(crop_path)
    return _run_tesseract(
        crop_path,
        psm=6,
        whitelist="ComplaintIDNoNumberCode:0123456789-",
    )


def tesseract_ocr_page(pdf_path: Path, *, page_number: int) -> str:
    with tempfile.TemporaryDirectory(prefix="crm-pdf-ocr-") as temp_name:
        temp_dir = Path(temp_name)
        image_path = render_page(
            pdf_path, temp_dir / f"page-{page_number}.png", page_number=page_number
        )
        full_text = _run_tesseract(image_path, psm=6)
        with Image.open(image_path) as image:
            label_text = _ocr_label_crop(image, temp_dir)
        return f"{full_text}\nComplaint label OCR: {label_text}".strip()


def tesseract_ocr_first_page(pdf_path: Path) -> str:
    """Compatibility entrypoint retained for callers and test overrides."""

    return tesseract_ocr_page(pdf_path, page_number=1)


def _pdf_pages(raw_text: str) -> list[str]:
    pages = raw_text.split("\f")
    # pdftotext appends one form-feed terminator. Remove only that synthetic
    # trailing entry; any preceding empty entry represents a real image-only
    # page that must be sent through OCR.
    if raw_text.endswith("\f") and pages:
        pages.pop()
    return pages or [raw_text]


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
    method = "pdftotext"
    errors: list[str] = []
    fields: dict[str, str] = {}
    pages: list[str] = []
    ocr_pages: list[int] = []

    try:
        raw_text = extract_pdf_text(pdf_path)
        pages = _pdf_pages(raw_text)
        fields.update(parse_key_values_from_text(raw_text))
    except Exception as exc:
        errors.append(f"PDF text extraction failed: {exc}")

    initial_numbers = complaint_numbers_in(
        "\n".join((field_value(fields, "complaint_number"), raw_text, pdf_path.name))
    )
    pages_to_ocr = []
    for index, page_text in enumerate(pages[:50], start=1):
        compact_page = compact_text(page_text)
        page_fields = parse_key_values_from_text(page_text)
        # Portal PDFs can contain a tiny selectable title layer over a full-page
        # scanned complaint. A complaint number alone is not sufficient proof
        # that the useful page content was extracted.
        sparse_structured_page = (
            len(compact_page) < 500
            and not field_value(page_fields, "person_name")
            and not field_value(page_fields, "remarks")
        )
        if len(compact_page) < 60 or sparse_structured_page:
            pages_to_ocr.append(index)
    if len(initial_numbers) != 1 and 1 not in pages_to_ocr:
        pages_to_ocr.insert(0, 1)
    ocr_parts: list[str] = []
    for page_number in pages_to_ocr:
        try:
            page_text = (
                tesseract_ocr_first_page(pdf_path)
                if page_number == 1
                else tesseract_ocr_page(pdf_path, page_number=page_number)
            )
            ocr_parts.append(f"[OCR page {page_number}]\n{page_text}")
            ocr_pages.append(page_number)
            for key, value in parse_key_values_from_text(page_text).items():
                fields.setdefault(key, value)
        except Exception as exc:
            errors.append(f"OCR page {page_number} failed: {exc}")
    if ocr_parts:
        ocr_text = "\n".join(ocr_parts)
        method = "pdftotext+selective-tesseract" if raw_text else "tesseract"

    combined_text = "\n".join(part for part in (raw_text, ocr_text) if part)
    field_numbers = complaint_numbers_in(field_value(fields, "complaint_number"))
    all_numbers = complaint_numbers_in(combined_text or pdf_path.name)
    complaint_number = ""
    number_source = ""
    if len(set(field_numbers)) == 1:
        complaint_number = field_numbers[0]
        number_source = "field"
    elif len(set(all_numbers)) == 1:
        complaint_number = all_numbers[0]
        number_source = "text"

    applicant_text = build_applicant_text(fields, combined_text)
    remarks_text = build_remarks_text(fields, combined_text)
    applicant_clean = clean_identity(applicant_text)
    remarks_clean = clean_remarks(remarks_text)

    confidence = 0.0
    if complaint_number:
        confidence += 0.5
    if applicant_clean:
        confidence += 0.2
    if remarks_clean:
        confidence += 0.3
    if raw_text:
        confidence += 0.05
    confidence = min(confidence, 1.0)
    error = " ".join(errors)

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
        confidence=confidence,
        needs_review=not complaint_number or confidence < 0.60 or bool(error),
        error=error,
        page_count=len(pages),
        ocr_pages=tuple(ocr_pages),
    )
