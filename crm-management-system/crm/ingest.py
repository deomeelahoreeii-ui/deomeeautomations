# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pdfplumber",
#     "rapidocr-onnxruntime",
# ]
# ///

"""
CRM Ingestion to Artifacts (Upgraded Version)
Save as ingest_crm.py in deomeeautomations/crm-management-system/
Run with: uv run ingest_crm.py
"""

import json
import logging
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    print("Please run: uv add pdfplumber")
    sys.exit(1)

# Basic logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s"
)
logger = logging.getLogger("crm_ingest")
DEFAULT_CRM_INGEST_DIR = "crm-main-complaints"
DEFAULT_ARTIFACT_DIR = "artifacts"
DEFAULT_CRM_CACHE_DB = "crm-cache.sqlite"
PLACEHOLDER_COMPLAINT_PREFIX = "000"
DEFAULT_COMPLAINT_NUMBER_OVERRIDES = "crm-complaint-overrides.json"
OCR_SUCCESS_DIR_NAME = "crm-ocr-successful"
OCR_UNSUCCESSFUL_DIR_NAME = "crm-ocr-unsuccessful"
OCR_REVIEW_DIR_NAME = "crm-ocr-review"
DEFAULT_OCR_REPEAT_MIN_CONFIDENCE = 0.84
DEFAULT_OCR_SINGLE_MIN_CONFIDENCE = 0.925
DEFAULT_OCR_COMPETING_MARGIN = 0.05
CRM_COMPLAINT_RE = re.compile(r"\b\d{3}-\d{4,}\b")
OCR_COMPLAINT_RE = re.compile(r"\b(\d{3})\s*[-–—]?\s*(\d{7})\b")
COMPLAINT_NUMBER_KEYS = {
    "complaintno",
    "complaintnumber",
    "complaintid",
    "complaintcode",
}
_OCR_READER = None


def clean_text(text: str) -> str:
    """Removes weird PDF line breaks and normalizes spacing."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def normalized_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def complaint_numbers_in(value: str) -> list[str]:
    return [match.group(0) for match in CRM_COMPLAINT_RE.finditer(value or "")]


def normalized_ocr_complaint_numbers(value: str) -> list[str]:
    numbers = []
    for match in OCR_COMPLAINT_RE.finditer(value or ""):
        prefix = match.group(1)
        suffix = match.group(2)
        # OCR often turns 6 into 0 in these scans. A CRM sequence starting with
        # 0 has not appeared in this corpus, so keep those as review-only noise.
        if suffix.startswith("0"):
            continue
        numbers.append(f"{prefix}-{suffix}")
    return numbers


def unique_preserving_order(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def valid_complaint_number(value: str) -> bool:
    return bool(CRM_COMPLAINT_RE.fullmatch(value.strip()))


def ocr_min_confidence() -> float:
    configured = os.getenv("CRM_OCR_MIN_CONFIDENCE", "").strip()
    if not configured:
        return DEFAULT_OCR_REPEAT_MIN_CONFIDENCE
    try:
        return float(configured)
    except ValueError:
        logger.warning(
            "Ignoring invalid CRM_OCR_MIN_CONFIDENCE=%r; using %.3f.",
            configured,
            DEFAULT_OCR_REPEAT_MIN_CONFIDENCE,
        )
        return DEFAULT_OCR_REPEAT_MIN_CONFIDENCE


def ocr_single_min_confidence() -> float:
    configured = os.getenv("CRM_OCR_SINGLE_MIN_CONFIDENCE", "").strip()
    if not configured:
        return DEFAULT_OCR_SINGLE_MIN_CONFIDENCE
    try:
        return float(configured)
    except ValueError:
        logger.warning(
            "Ignoring invalid CRM_OCR_SINGLE_MIN_CONFIDENCE=%r; using %.3f.",
            configured,
            DEFAULT_OCR_SINGLE_MIN_CONFIDENCE,
        )
        return DEFAULT_OCR_SINGLE_MIN_CONFIDENCE


def ocr_enabled() -> bool:
    return os.getenv("CRM_DISABLE_OCR", "").strip().lower() not in {"1", "true", "yes"}


def ocr_reader():
    global _OCR_READER
    if _OCR_READER is None:
        try:
            from rapidocr_onnxruntime import RapidOCR
        except ImportError as exc:
            raise RuntimeError(
                "Image-only CRM PDF needs OCR, but rapidocr-onnxruntime is not installed."
            ) from exc
        _OCR_READER = RapidOCR()
    return _OCR_READER


def write_ocr_review_crop(input_dir: Path, pdf_path: Path, crop) -> Path | None:
    if crop is None:
        return None
    try:
        import cv2
    except ImportError:
        return None

    review_dir = input_dir / OCR_REVIEW_DIR_NAME
    review_dir.mkdir(exist_ok=True)
    review_path = review_dir / f"{pdf_path.stem}-complaint-id.png"
    cv2.imwrite(str(review_path), crop)
    return review_path


def render_pdf_first_page(pdf_path: Path, temp_dir: Path, dpi: int) -> Path:
    pdftoppm = shutil.which("pdftoppm")
    if not pdftoppm:
        raise ValueError("OCR fallback needs the pdftoppm command to render scanned PDFs.")

    output_base = temp_dir / f"{pdf_path.stem}-{dpi}"
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
            str(dpi),
            str(pdf_path),
            str(output_base),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return output_base.with_suffix(".png")


def detect_complaint_id_crop(image_path: Path):
    try:
        import cv2
    except ImportError as exc:
        raise RuntimeError(
            "OCR crop detection needs opencv-python, installed with rapidocr-onnxruntime."
        ) from exc

    image = cv2.imread(str(image_path))
    if image is None:
        return None

    height, width = image.shape[:2]
    header = image[: int(height * 0.30), :]
    gray = cv2.cvtColor(header, cv2.COLOR_BGR2GRAY)
    mask = (gray < 95).astype("uint8") * 255
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (9, 5))
    mask = cv2.morphologyEx(mask, cv2.MORPH_CLOSE, kernel, iterations=2)
    contours, _hierarchy = cv2.findContours(
        mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )

    boxes = []
    for contour in contours:
        x, y, box_width, box_height = cv2.boundingRect(contour)
        area = box_width * box_height
        aspect = box_width / max(box_height, 1)
        if (
            450 < box_width < 1100
            and 70 < box_height < 220
            and 3.8 < aspect < 8.5
            and area > 45000
            and 0.30 * width < x < 0.62 * width
            and y > 0.09 * height
        ):
            boxes.append((x, y, box_width, box_height, area, aspect))

    if not boxes:
        return None

    boxes.sort(key=lambda box: (abs(box[5] - 5.8), -box[4]))
    x, y, box_width, box_height, _area, _aspect = boxes[0]
    pad_x = int(box_width * 0.08)
    pad_y = int(box_height * 0.25)
    return image[
        max(0, y - pad_y) : min(height, y + box_height + pad_y),
        max(0, x - pad_x) : min(width, x + box_width + pad_x),
    ]


def ocr_image_variants(crop):
    import cv2

    variants = [("pill_raw", crop)]
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    big = cv2.resize(gray, None, fx=2.5, fy=2.5, interpolation=cv2.INTER_CUBIC)
    variants.append(("pill_gray_big", cv2.cvtColor(big, cv2.COLOR_GRAY2BGR)))

    equalized = cv2.equalizeHist(big)
    variants.append(("pill_equalized", cv2.cvtColor(equalized, cv2.COLOR_GRAY2BGR)))

    _threshold, white_text = cv2.threshold(big, 150, 255, cv2.THRESH_BINARY)
    inverted = 255 - white_text
    variants.append(("pill_white_text", cv2.cvtColor(inverted, cv2.COLOR_GRAY2BGR)))
    return variants


def collect_ocr_candidates(image_path: Path, source_name: str) -> list[tuple[str, float, str, str]]:
    results, _elapsed = ocr_reader()(str(image_path))
    candidates: list[tuple[str, float, str, str]] = []
    for _box, text, confidence in results or []:
        clean_line = clean_text(str(text))
        for number in normalized_ocr_complaint_numbers(clean_line):
            candidates.append((number, float(confidence), clean_line, source_name))
    return candidates


def choose_ocr_candidate(candidates: list[tuple[str, float, str, str]]) -> tuple[str, float]:
    repeat_min_confidence = ocr_min_confidence()
    single_min_confidence = ocr_single_min_confidence()
    scores: dict[str, dict[str, object]] = {}
    for number, confidence, line, source_name in candidates:
        entry = scores.setdefault(
            number, {"confidence": 0.0, "sources": set(), "lines": []}
        )
        entry["confidence"] = max(float(entry["confidence"]), confidence)
        entry["sources"].add(source_name)  # type: ignore[union-attr]
        entry["lines"].append(line)  # type: ignore[union-attr]

    ranked = []
    for number, entry in scores.items():
        confidence = float(entry["confidence"])
        sources = entry["sources"]  # type: ignore[assignment]
        source_count = len(sources)
        ranked.append((number, confidence, source_count))
    ranked.sort(key=lambda item: (item[2], item[1]), reverse=True)

    if not ranked:
        raise ValueError(
            "OCR could not read a CRM complaint number. Review the saved ID crop "
            f"or add an entry to {DEFAULT_COMPLAINT_NUMBER_OVERRIDES}."
        )

    best_number, best_confidence, best_source_count = ranked[0]
    second = ranked[1] if len(ranked) > 1 else None

    if best_source_count >= 2 and best_confidence >= repeat_min_confidence:
        if second and second[2] >= 2 and second[1] >= repeat_min_confidence:
            raise ValueError(
                "OCR found competing repeated complaint numbers: "
                f"{best_number} and {second[0]}."
            )
        return best_number, best_confidence

    if best_confidence >= single_min_confidence:
        if second and second[1] >= best_confidence - DEFAULT_OCR_COMPETING_MARGIN:
            raise ValueError(
                "OCR found a strong complaint number but a close competitor too: "
                f"{best_number} ({best_confidence:.3f}) vs "
                f"{second[0]} ({second[1]:.3f})."
            )
        return best_number, best_confidence

    candidate_summary = ", ".join(
        f"{number} ({confidence:.3f}, {source_count} source(s))"
        for number, confidence, source_count in ranked[:5]
    )
    raise ValueError(
        "OCR saw possible complaint number(s), but none were repeated/clear enough "
        f"for automatic upload: {candidate_summary}. Review the saved ID crop or add "
        f"an entry to {DEFAULT_COMPLAINT_NUMBER_OVERRIDES}."
    )


def ocr_complaint_number(pdf_path: Path) -> str:
    return ocr_complaint_number_with_review(pdf_path, pdf_path.parent)


def ocr_complaint_number_with_review(pdf_path: Path, input_dir: Path) -> str:
    if not ocr_enabled():
        raise ValueError("OCR fallback is disabled by CRM_DISABLE_OCR.")

    candidates: list[tuple[str, float, str, str]] = []
    review_path: Path | None = None
    with tempfile.TemporaryDirectory(prefix="crm-ocr-") as temp_dir:
        temp_path = Path(temp_dir)
        full_page_300 = render_pdf_first_page(pdf_path, temp_path, 300)
        candidates.extend(collect_ocr_candidates(full_page_300, "page_300"))

        full_page_600 = render_pdf_first_page(pdf_path, temp_path, 600)
        crop = detect_complaint_id_crop(full_page_600)
        if crop is not None:
            review_path = write_ocr_review_crop(input_dir, pdf_path, crop)
            try:
                import cv2
            except ImportError:
                cv2 = None
            if cv2 is not None:
                for source_name, variant in ocr_image_variants(crop):
                    variant_path = temp_path / f"{pdf_path.stem}-{source_name}.png"
                    cv2.imwrite(str(variant_path), variant)
                    candidates.extend(collect_ocr_candidates(variant_path, source_name))

    try:
        number, confidence = choose_ocr_candidate(candidates)
    except ValueError as exc:
        if review_path:
            raise ValueError(f"{exc} Review crop: {review_path}") from exc
        raise

    logger.info(
        "OCR extracted CRM complaint number %s from %s (confidence %.3f).",
        number,
        pdf_path.name,
        confidence,
    )
    return number


def load_complaint_number_overrides(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Complaint number override file must be a JSON object: {path}")

    overrides: dict[str, str] = {}
    for filename, complaint_number in raw.items():
        filename = str(filename).strip()
        complaint_number = str(complaint_number).strip()
        if not filename:
            raise ValueError(f"Blank filename in complaint number override file: {path}")
        if not complaint_number:
            continue
        if not valid_complaint_number(complaint_number):
            raise ValueError(
                "Invalid CRM complaint number override for "
                f"{filename}: {complaint_number!r}"
            )
        overrides[filename] = complaint_number
    return overrides


def extract_complaint_number(
    extracted_data: dict[str, str], pdf_path: Path, manual_override: str = ""
) -> str:
    preferred_candidates: list[str] = []
    fallback_candidates: list[str] = []

    for key, value in extracted_data.items():
        numbers = complaint_numbers_in(str(value))
        if normalized_key(str(key)) in COMPLAINT_NUMBER_KEYS:
            preferred_candidates.extend(numbers)
        else:
            fallback_candidates.extend(numbers)

    text_candidates = unique_preserving_order(preferred_candidates + fallback_candidates)
    if manual_override:
        if not valid_complaint_number(manual_override):
            raise ValueError(
                f"Invalid manual CRM complaint number override: {manual_override!r}"
            )
        if text_candidates and manual_override not in text_candidates:
            raise ValueError(
                "Manual CRM complaint number override conflicts with PDF text: "
                f"{manual_override} vs {', '.join(text_candidates)}"
            )
        return manual_override

    filename_candidates = complaint_numbers_in(pdf_path.name)
    for source_name, candidates in (
        ("CRM complaint-number field", preferred_candidates),
        ("PDF text", fallback_candidates),
        ("filename", filename_candidates),
    ):
        unique_candidates = unique_preserving_order(candidates)
        if len(unique_candidates) == 1:
            return unique_candidates[0]
        if len(unique_candidates) > 1:
            raise ValueError(
                f"Multiple complaint numbers found in {source_name}: "
                f"{', '.join(unique_candidates)}"
            )

    raise ValueError(
        "Could not extract a CRM complaint number like 104-6395857. "
        "If this is a scanned/image-only CRM PDF, add a reviewed entry to "
        f"{DEFAULT_COMPLAINT_NUMBER_OVERRIDES} or run OCR before upload."
    )


def write_rejection_report(input_dir: Path, rejected: list[tuple[Path, str]]) -> None:
    report_path = input_dir / "crm-prepare-rejected.txt"
    legacy_report_path = input_dir / "crm-ingest-rejected.txt"
    if not rejected:
        if report_path.exists():
            report_path.unlink()
        if legacy_report_path.exists():
            legacy_report_path.unlink()
        return

    lines = [
        "CRM PDFs rejected during prepare",
        "",
        "These files were not staged for Paperless upload because the prepare step",
        "could not safely identify a real CRM complaint number.",
        "",
    ]
    for pdf_path, reason in rejected:
        lines.append(f"- {pdf_path.name}: {reason}")
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if legacy_report_path.exists():
        legacy_report_path.unlink()
    logger.error("Rejected CRM prepare report written to: %s", report_path)


def write_json(path: Path, data: dict[str, str]) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def reset_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def copy_ocr_review_crop(input_dir: Path, pdf_path: Path, target_dir: Path) -> None:
    crop_path = input_dir / OCR_REVIEW_DIR_NAME / f"{pdf_path.stem}-complaint-id.png"
    if crop_path.exists():
        shutil.copy2(crop_path, target_dir / crop_path.name)


def write_ocr_result_folders(
    input_dir: Path,
    ocr_successful: list[tuple[Path, str]],
    ocr_unsuccessful: list[tuple[Path, str]],
) -> None:
    if not ocr_successful and not ocr_unsuccessful:
        return

    successful_dir = input_dir / OCR_SUCCESS_DIR_NAME
    unsuccessful_dir = input_dir / OCR_UNSUCCESSFUL_DIR_NAME
    reset_directory(successful_dir)
    reset_directory(unsuccessful_dir)

    successful_overrides: dict[str, str] = {}
    for pdf_path, complaint_number in ocr_successful:
        shutil.copy2(pdf_path, successful_dir / pdf_path.name)
        copy_ocr_review_crop(input_dir, pdf_path, successful_dir)
        successful_overrides[pdf_path.name] = complaint_number

    unsuccessful_overrides: dict[str, str] = {}
    unsuccessful_notes: list[str] = []
    for pdf_path, reason in ocr_unsuccessful:
        shutil.copy2(pdf_path, unsuccessful_dir / pdf_path.name)
        copy_ocr_review_crop(input_dir, pdf_path, unsuccessful_dir)
        unsuccessful_overrides[pdf_path.name] = ""
        unsuccessful_notes.append(f"- {pdf_path.name}: {reason}")

    write_json(successful_dir / DEFAULT_COMPLAINT_NUMBER_OVERRIDES, successful_overrides)
    write_json(
        unsuccessful_dir / DEFAULT_COMPLAINT_NUMBER_OVERRIDES,
        unsuccessful_overrides,
    )

    if unsuccessful_notes:
        (unsuccessful_dir / "ocr-review-notes.txt").write_text(
            "CRM PDFs needing manual complaint-number review\n\n"
            + "\n".join(unsuccessful_notes)
            + "\n",
            encoding="utf-8",
        )

    logger.info(
        "OCR successful review folder: %s (%d PDF(s))",
        successful_dir,
        len(ocr_successful),
    )
    logger.info(
        "OCR unsuccessful review folder: %s (%d PDF(s))",
        unsuccessful_dir,
        len(ocr_unsuccessful),
    )


def parse_crm_pdf(pdf_path: Path) -> dict[str, str]:
    """
    Advanced extraction using pdfplumber.
    Tries Table Extraction -> Quoted Regex -> Line-by-Line Key/Value matching.
    """
    extracted_data = {}
    raw_text_parts: list[str] = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # --- STRATEGY 1: Extract actual Tables ---
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    # If it's a 2-column table (Key -> Value)
                    if len(row) >= 2 and row[0] and row[1]:
                        key = clean_text(row[0]).replace(":", "")
                        val = clean_text(row[1])
                        extracted_data[key] = val

            # --- STRATEGY 2 & 3: Fallback to Raw Text Parsing ---
            text = page.extract_text()
            if text:
                raw_text_parts.append(text)
                # Fallback A: The original quoted regex
                matches = re.findall(r'"([^"]+)"\s*,\s*"([^"]*)"', text)
                for k, v in matches:
                    clean_k = clean_text(k)
                    if clean_k not in extracted_data:
                        extracted_data[clean_k] = clean_text(v)

                # Fallback B: Common "Key: Value" line formatting
                for line in text.split("\n"):
                    if ":" in line:
                        parts = line.split(":", 1)
                        if len(parts) == 2:
                            k, v = clean_text(parts[0]), clean_text(parts[1])
                            if k and k not in extracted_data:
                                extracted_data[k] = v

    if raw_text_parts:
        extracted_data["__raw_text"] = "\n".join(raw_text_parts)

    return extracted_data


def resolve_input_dir(project_root: Path, value: str | None = None) -> Path:
    configured = value or os.getenv("CRM_INGEST_DIR") or DEFAULT_CRM_INGEST_DIR
    path = Path(configured).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def resolve_paperless_status(value: str | None = None) -> str:
    return (value or os.getenv("CRM_PAPERLESS_STATUS") or "").strip()


def resolve_artifact_dir(project_root: Path, value: str | None = None) -> Path:
    configured = (
        value
        or os.getenv("ARTIFACT_DIR")
        or os.getenv("PMDU_ARTIFACT_DIR")
        or DEFAULT_ARTIFACT_DIR
    )
    path = Path(configured).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def resolve_cache_db(project_root: Path, value: str | None = None) -> Path:
    configured = value or os.getenv("CRM_CACHE_DB") or DEFAULT_CRM_CACHE_DB
    path = Path(configured).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve()


def relative_to_project(path: Path, project_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_root.resolve()))
    except ValueError:
        return str(path.resolve())


def cached_extraction(cache_db: Path, pdf_path: Path):
    if not cache_db.exists():
        return None
    try:
        from crm.extract import sha256_file
        from crm.store import connect, get_local_by_sha256

        digest = sha256_file(pdf_path)
        with connect(cache_db) as conn:
            return get_local_by_sha256(conn, digest)
    except Exception:
        logger.warning("Could not read CRM OCR cache for %s.", pdf_path.name, exc_info=True)
        return None


def high_confidence_cached_number(cached) -> str:
    if not cached:
        return ""
    if cached.needs_review:
        return ""
    if float(cached.confidence or 0) < 0.88:
        return ""
    return str(cached.complaint_number or "").strip()


def placeholder_complaint_number(pdf_path: Path, cached=None) -> str:
    if cached and getattr(cached, "source_sha256", ""):
        digest = str(cached.source_sha256)
    else:
        from crm.extract import sha256_file

        digest = sha256_file(pdf_path)
    suffix = str(int(digest[:14], 16) % 10_000_000_000).zfill(10)
    return f"{PLACEHOLDER_COMPLAINT_PREFIX}-{suffix}"


def can_use_placeholder_number(cached, extracted_data: dict[str, str]) -> bool:
    if cached and (
        getattr(cached, "remarks_clean", "")
        or getattr(cached, "applicant_clean", "")
        or getattr(cached, "raw_text", "")
        or getattr(cached, "ocr_text", "")
    ):
        return True
    return bool(
        first_value(
            extracted_data.get("Complaint Remarks", ""),
            extracted_data.get("__raw_text", ""),
        )
    )


def first_value(*values: str) -> str:
    for value in values:
        if str(value or "").strip():
            return str(value).strip()
    return ""


def cached_applicant_field(cached, field_name: str) -> str:
    if not cached:
        return ""
    text = str(getattr(cached, "applicant_text", "") or "")
    if not text:
        return ""
    boundaries = {
        "name": r"(?:CNIC|CHIC|Contact|District|Address|Complaint Information|Department)",
        "contact": r"(?:District|Address|Complaint Information|Department)",
        "address": r"(?:Complaint Information|Department|Category|Sub Category|District\s+[A-Z]|Source|Created Date)",
        "district": r"(?:Address|Complaint Information|Department|Category|Source|Created Date)",
    }
    labels = {
        "name": r"Name",
        "contact": r"Contact",
        "address": r"Address",
        "district": r"District",
    }
    label = labels.get(field_name)
    boundary = boundaries.get(field_name)
    if not label or not boundary:
        return ""
    match = re.search(
        rf"\b{label}\b\s*:?\s*(.+?)(?=\b{boundary}\b|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    return clean_text(match.group(1))


def run_crm_ingest(
    project_root: Path,
    input_dir: Path,
    paperless_status: str = "",
    artifact_dir: Path | None = None,
    clear_artifact_dir: bool = False,
    cache_db: Path | None = None,
    cache_only: bool = False,
    allow_placeholder_number: bool = False,
) -> int:
    raw_crm_dir = input_dir
    artifacts_dir = artifact_dir or resolve_artifact_dir(project_root)
    cache_db = cache_db or resolve_cache_db(project_root)

    if not raw_crm_dir.exists():
        logger.error("CRM intake directory not found: %s", raw_crm_dir)
        return 1

    if clear_artifact_dir:
        default_artifacts_dir = (project_root / DEFAULT_ARTIFACT_DIR).resolve()
        if artifacts_dir == default_artifacts_dir or artifacts_dir == project_root.resolve():
            logger.error(
                "Refusing to clear broad artifact directory: %s. Choose a scoped artifact folder.",
                artifacts_dir,
            )
            return 1
        if artifacts_dir.exists():
            shutil.rmtree(artifacts_dir)
            logger.info("Cleared CRM artifact directory: %s", artifacts_dir)

    pdf_files = sorted(raw_crm_dir.rglob("*.pdf"))
    if not pdf_files:
        logger.info("No PDFs found to process.")
        return 0

    logger.info("CRM prepare input directory: %s", raw_crm_dir)
    logger.info("CRM artifact directory: %s", artifacts_dir)
    if cache_db.exists():
        logger.info("CRM OCR cache DB: %s", cache_db)
    override_path = raw_crm_dir / DEFAULT_COMPLAINT_NUMBER_OVERRIDES
    complaint_number_overrides = load_complaint_number_overrides(override_path)
    if complaint_number_overrides:
        logger.info(
            "Loaded %d reviewed CRM complaint-number override(s): %s",
            len(complaint_number_overrides),
            override_path,
        )
    if paperless_status:
        logger.info("CRM Paperless status override: %s", paperless_status)
    if cache_only:
        logger.info("CRM prepare cache-only mode: OCR fallback disabled.")
    if allow_placeholder_number:
        logger.info(
            "CRM prepare placeholder-number mode enabled for fresh unreadable complaint numbers."
        )
    logger.info("Found %d CRM PDFs. Building Paperless-ready artifacts...", len(pdf_files))

    staged = 0
    rejected: list[tuple[Path, str]] = []
    ocr_successful: list[tuple[Path, str]] = []
    ocr_unsuccessful: list[tuple[Path, str]] = []
    placeholder_numbers: dict[str, dict[str, str]] = {}
    seen_complaint_numbers: dict[str, Path] = {}
    for pdf_path in pdf_files:
        complaint_code = ""
        complaint_source = ""
        ocr_attempted = False
        try:
            # 1. Parse the PDF with the new advanced extractor
            extracted_data = parse_crm_pdf(pdf_path)
            cached = cached_extraction(cache_db, pdf_path)

            # 2. Resolve a real CRM complaint number. Never fall back to arbitrary
            # filenames like o-03; those can create duplicate Paperless complaints.
            manual_override = complaint_number_overrides.get(pdf_path.name, "")
            if manual_override:
                complaint_code = extract_complaint_number(
                    extracted_data, pdf_path, manual_override
                )
                complaint_source = "manual_override"
                logger.info(
                    "Using reviewed CRM complaint-number override for %s: %s",
                    pdf_path.name,
                    complaint_code,
                )
            elif high_confidence_cached_number(cached):
                complaint_code = high_confidence_cached_number(cached)
                complaint_source = "ocr_cache"
                logger.info(
                    "Using high-confidence CRM OCR cache for %s: %s",
                    pdf_path.name,
                    complaint_code,
                )
            else:
                try:
                    complaint_code = extract_complaint_number(extracted_data, pdf_path)
                    complaint_source = "text"
                except ValueError as text_error:
                    if cache_only:
                        if not allow_placeholder_number or not can_use_placeholder_number(
                            cached, extracted_data
                        ):
                            raise ValueError(
                                f"{text_error} No high-confidence cache/override exists; "
                                "rerun Filter CRM PDF Duplicates or add a reviewed override."
                            ) from text_error
                        complaint_code = placeholder_complaint_number(pdf_path, cached)
                        complaint_source = "placeholder"
                        logger.info(
                            "Using placeholder CRM complaint number for %s: %s",
                            pdf_path.name,
                            complaint_code,
                        )
                        placeholder_numbers[pdf_path.name] = {
                            "placeholder_complaint_number": complaint_code,
                            "source_sha256": str(getattr(cached, "source_sha256", "")),
                            "reason": "real complaint number unreadable during OCR/filtering",
                        }
                    else:
                        logger.info(
                            "No safe text complaint number found in %s. Trying OCR fallback...",
                            pdf_path.name,
                        )
                        ocr_attempted = True
                        try:
                            extracted_data["Complaint ID OCR"] = ocr_complaint_number_with_review(
                                pdf_path, raw_crm_dir
                            )
                            complaint_code = extract_complaint_number(extracted_data, pdf_path)
                            complaint_source = "ocr"
                        except Exception as ocr_error:
                            reason = f"{text_error} OCR fallback: {ocr_error}"
                            ocr_unsuccessful.append((pdf_path, reason))
                            raise ValueError(reason) from ocr_error
            if complaint_code in seen_complaint_numbers:
                raise ValueError(
                    "Duplicate complaint number in this ingest batch: "
                    f"{complaint_code} already came from "
                    f"{seen_complaint_numbers[complaint_code].name}"
                )
            seen_complaint_numbers[complaint_code] = pdf_path

            # 3. Build the artifact directory
            case_dir = artifacts_dir / complaint_code / "v1"
            case_dir.mkdir(parents=True, exist_ok=True)

            dest_pdf_path = case_dir / f"{complaint_code}.pdf"
            shutil.copy2(pdf_path, dest_pdf_path)

            # 4. Generate an Enriched snapshot.json
            # We now grab CNIC, Address, District, Sub-category, and Remarks based on standard CRM fields
            snapshot_data = {
                "complaint_code": complaint_code,
                "version": 1,
                "source": "CRM",
                "source_pdf": relative_to_project(pdf_path, project_root),
                "generated_pdf": relative_to_project(dest_pdf_path, project_root),
                "identity": {
                    "citizen_name": extracted_data.get("Person Name", ""),
                    "citizen_contact": extracted_data.get("Mobile No", ""),
                    "citizen_cnic": extracted_data.get("Cnic No", ""),
                    "category": extracted_data.get("Category", ""),
                    "sub_category": extracted_data.get("Sub Category", ""),
                    "address": extracted_data.get("Person Address", ""),
                    "district": extracted_data.get("Complaint District", ""),
                    "tehsil": extracted_data.get("Tehsil", "None"),
                    "level_one": "School Education Department",
                },
                "complaint_remarks": first_value(
                    getattr(cached, "remarks_text", "") if cached else "",
                    extracted_data.get("Complaint Remarks", ""),
                ),
                "ocr": {
                    "cache_db": relative_to_project(cache_db, project_root),
                    "source": getattr(cached, "extraction_method", "") if cached else "",
                    "confidence": getattr(cached, "confidence", 0.0) if cached else 0.0,
                    "needs_review": bool(getattr(cached, "needs_review", False)) if cached else False,
                    "complaint_number_source": complaint_source,
                    "placeholder_complaint_number": complaint_source == "placeholder",
                    "real_complaint_number_missing": complaint_source == "placeholder",
                },
                "attachments": [],
            }
            if cached:
                snapshot_data["identity"]["citizen_name"] = first_value(
                    extracted_data.get("Person Name", ""),
                    cached_applicant_field(cached, "name"),
                )
                snapshot_data["identity"]["citizen_contact"] = first_value(
                    extracted_data.get("Mobile No", ""),
                    cached_applicant_field(cached, "contact"),
                )
                snapshot_data["identity"]["address"] = first_value(
                    extracted_data.get("Person Address", ""),
                    cached_applicant_field(cached, "address"),
                )
                snapshot_data["identity"]["district"] = first_value(
                    extracted_data.get("Complaint District", ""),
                    cached_applicant_field(cached, "district"),
                )
            if paperless_status:
                snapshot_data["paperless"] = {"status": paperless_status}

            snapshot_path = case_dir / "snapshot.json"
            snapshot_path.write_text(
                json.dumps(snapshot_data, indent=2), encoding="utf-8"
            )

            logger.info("Successfully staged enriched artifact: %s", complaint_code)
            staged += 1
            if complaint_source == "ocr":
                ocr_successful.append((pdf_path, complaint_code))

        except Exception as e:
            if ocr_attempted and not any(path == pdf_path for path, _reason in ocr_unsuccessful):
                ocr_unsuccessful.append((pdf_path, str(e)))
            rejected.append((pdf_path, str(e)))
            logger.error("Rejected CRM PDF %s: %s", pdf_path.name, e)

    write_ocr_result_folders(raw_crm_dir, ocr_successful, ocr_unsuccessful)
    write_rejection_report(raw_crm_dir, rejected)
    placeholder_report_path = artifacts_dir / "crm-placeholder-numbers.json"
    if placeholder_numbers:
        placeholder_report_path.write_text(
            json.dumps(placeholder_numbers, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        logger.info("CRM placeholder-number report written: %s", placeholder_report_path)
    elif placeholder_report_path.exists():
        placeholder_report_path.unlink()
    if rejected:
        logger.error(
            "CRM prepare blocked Paperless upload: staged=%d rejected=%d.",
            staged,
            len(rejected),
        )
        return 1

    logger.info("CRM prepare complete! Staged %d file(s). Ready for Paperless upload.", staged)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Prepare CRM complaint PDFs for Paperless upload")
    parser.add_argument(
        "--input-dir",
        default=None,
        help=(
            "Folder containing CRM complaint PDFs. Defaults to CRM_INGEST_DIR "
            f"or {DEFAULT_CRM_INGEST_DIR}."
        ),
    )
    parser.add_argument(
        "--status",
        default=None,
        help=(
            "Optional Paperless Status custom-field value for the main complaint. "
            "Defaults to CRM_PAPERLESS_STATUS or the Paperless field config default."
        ),
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help=(
            "Folder where staged artifact snapshots are written. Defaults to "
            f"ARTIFACT_DIR, PMDU_ARTIFACT_DIR, or {DEFAULT_ARTIFACT_DIR}."
        ),
    )
    parser.add_argument(
        "--clear-artifact-dir",
        action="store_true",
        help="Clear the selected artifact directory before staging this CRM batch.",
    )
    parser.add_argument(
        "--crm-cache-db",
        default=None,
        help=(
            "SQLite OCR/extraction cache DB. Defaults to CRM_CACHE_DB or "
            f"{DEFAULT_CRM_CACHE_DB}."
        ),
    )
    parser.add_argument(
        "--cache-only",
        action="store_true",
        help=(
            "Do not run OCR during prepare. Require complaint numbers from the "
            "SQLite cache, PDF text, or reviewed overrides."
        ),
    )
    parser.add_argument(
        "--allow-placeholder-number",
        action="store_true",
        help=(
            "For fresh PDFs whose real complaint number is unreadable, stage them "
            f"with a deterministic {PLACEHOLDER_COMPLAINT_PREFIX}-prefixed temporary number."
        ),
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    return run_crm_ingest(
        project_root,
        resolve_input_dir(project_root, args.input_dir),
        resolve_paperless_status(args.status),
        resolve_artifact_dir(project_root, args.artifact_dir),
        args.clear_artifact_dir,
        resolve_cache_db(project_root, args.crm_cache_db),
        args.cache_only,
        args.allow_placeholder_number,
    )


if __name__ == "__main__":
    sys.exit(main())
