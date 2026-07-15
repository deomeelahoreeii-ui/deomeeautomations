from __future__ import annotations

import hashlib
import re
import shutil
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any


PDF_EXTENSION = ".pdf"
PDF_BATCH_SCHEMA = "crm_pdf_batch_v1"
MAX_PDF_COUNT = 1000


def safe_pdf_filename(filename: str) -> str:
    name = Path(filename or "complaint.pdf").name
    stem = re.sub(r"[^A-Za-z0-9._ -]+", "_", Path(name).stem).strip(" ._")
    stem = re.sub(r"\s+", " ", stem)[:160] or "complaint"
    return f"{stem}.pdf"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def looks_like_pdf(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            return handle.read(5) == b"%PDF-"
    except OSError:
        return False


def _unique_name(name: str, used: set[str]) -> str:
    candidate = safe_pdf_filename(name)
    if candidate.casefold() not in used:
        used.add(candidate.casefold())
        return candidate
    stem = Path(candidate).stem
    suffix = Path(candidate).suffix
    counter = 2
    while True:
        candidate = f"{stem} ({counter}){suffix}"
        if candidate.casefold() not in used:
            used.add(candidate.casefold())
            return candidate
        counter += 1


def create_pdf_batch_archive(
    files: list[tuple[str, Path]],
    destination: Path,
) -> tuple[str, dict[str, Any], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    if not files:
        return "", {}, ["Choose at least one CRM PDF."], warnings
    if len(files) > MAX_PDF_COUNT:
        return "", {}, [f"A batch may contain at most {MAX_PDF_COUNT} PDFs."], warnings

    used_names: set[str] = set()
    entries: list[dict[str, Any]] = []
    for original_name, path in files:
        if path.suffix.lower() != PDF_EXTENSION:
            errors.append(f"{Path(original_name).name}: only PDF files are supported.")
            continue
        if not looks_like_pdf(path):
            errors.append(f"{Path(original_name).name}: the file does not have a valid PDF header.")
            continue
        stored_name = _unique_name(original_name, used_names)
        entries.append(
            {
                "original_name": Path(original_name).name,
                "stored_name": stored_name,
                "path": path,
                "size_bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )

    if errors:
        return "", {}, errors, warnings

    digest = hashlib.sha256()
    for entry in sorted(entries, key=lambda value: (value["stored_name"].casefold(), value["sha256"])):
        digest.update(entry["stored_name"].encode("utf-8"))
        digest.update(b"\0")
        digest.update(entry["sha256"].encode("ascii"))
        digest.update(b"\0")
    batch_sha256 = digest.hexdigest()

    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for entry in entries:
            info = zipfile.ZipInfo(entry["stored_name"], date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            with entry["path"].open("rb") as source, archive.open(info, "w") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)

    counts = Counter(entry["sha256"] for entry in entries)
    duplicate_files = sum(count - 1 for count in counts.values() if count > 1)
    duplicate_groups = sum(1 for count in counts.values() if count > 1)
    if duplicate_files:
        warnings.append(
            f"{duplicate_files} exact duplicate PDF(s) across {duplicate_groups} content group(s) were detected and will be separated as Duplicate in Batch."
        )

    metadata = {
        "file_count": len(entries),
        "total_uncompressed_bytes": sum(entry["size_bytes"] for entry in entries),
        "duplicate_file_count": duplicate_files,
        "duplicate_group_count": duplicate_groups,
        "files": [
            {
                "original_name": entry["original_name"],
                "stored_name": entry["stored_name"],
                "size_bytes": entry["size_bytes"],
                "sha256": entry["sha256"],
            }
            for entry in entries
        ],
        "ocr_tools": {
            "pdftotext": bool(shutil.which("pdftotext")),
            "pdftoppm": bool(shutil.which("pdftoppm")),
            "tesseract": bool(shutil.which("tesseract")),
        },
    }
    if not metadata["ocr_tools"]["pdftotext"]:
        warnings.append("pdftotext is unavailable; all PDFs will require manual review.")
    if not metadata["ocr_tools"]["pdftoppm"] or not metadata["ocr_tools"]["tesseract"]:
        warnings.append(
            "OCR tools are incomplete. Image-only PDFs without extractable text may be placed in OCR Failed."
        )
    return batch_sha256, metadata, errors, warnings


def validate_pdf_batch_archive(path: Path) -> tuple[str, str | None, dict[str, Any], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    try:
        with zipfile.ZipFile(path) as archive:
            members = [member for member in archive.infolist() if not member.is_dir()]
            if not members:
                errors.append("The CRM PDF batch contains no files.")
            if len(members) > MAX_PDF_COUNT:
                errors.append(f"A batch may contain at most {MAX_PDF_COUNT} PDFs.")
            for member in members:
                pure = PurePosixPath(member.filename)
                if pure.is_absolute() or ".." in pure.parts or len(pure.parts) != 1:
                    errors.append(f"Unsafe archive member: {member.filename}")
                elif pure.suffix.lower() != PDF_EXTENSION:
                    errors.append(f"Unsupported archive member: {member.filename}")
            metadata = {
                "file_count": len(members),
                "total_uncompressed_bytes": sum(member.file_size for member in members),
                "files": [
                    {"stored_name": member.filename, "size_bytes": member.file_size}
                    for member in members
                ],
                "ocr_tools": {
                    "pdftotext": bool(shutil.which("pdftotext")),
                    "pdftoppm": bool(shutil.which("pdftoppm")),
                    "tesseract": bool(shutil.which("tesseract")),
                },
            }
    except (OSError, zipfile.BadZipFile) as exc:
        errors.append(f"The CRM PDF batch could not be read: {exc}")
    return ("invalid" if errors else "valid"), PDF_BATCH_SCHEMA, metadata, errors, warnings


def extract_pdf_batch(path: Path, destination: Path) -> list[Path]:
    destination.mkdir(parents=True, exist_ok=False)
    extracted: list[Path] = []
    with zipfile.ZipFile(path) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            pure = PurePosixPath(member.filename)
            if pure.is_absolute() or ".." in pure.parts or len(pure.parts) != 1:
                raise ValueError(f"Unsafe CRM PDF batch member: {member.filename}")
            if pure.suffix.lower() != PDF_EXTENSION:
                raise ValueError(f"Unsupported CRM PDF batch member: {member.filename}")
            target = destination / safe_pdf_filename(pure.name)
            with archive.open(member) as source, target.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            if not looks_like_pdf(target):
                raise ValueError(f"Invalid PDF in CRM batch: {member.filename}")
            extracted.append(target)
    if not extracted:
        raise ValueError("The CRM PDF batch contains no PDFs.")
    return sorted(extracted, key=lambda item: item.name.casefold())
