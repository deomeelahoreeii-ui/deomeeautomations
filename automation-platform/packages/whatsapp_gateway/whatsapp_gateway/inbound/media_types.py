from __future__ import annotations

import csv
import io
import mimetypes
import re
import zipfile
from pathlib import Path

SUPPORTED_CATEGORIES = frozenset({"image", "pdf", "spreadsheet"})

_MIME_CATEGORY = {
    "application/pdf": ("pdf", ".pdf"),
    "image/jpeg": ("image", ".jpg"),
    "image/png": ("image", ".png"),
    "image/webp": ("image", ".webp"),
    "image/heic": ("image", ".heic"),
    "image/heif": ("image", ".heif"),
    "application/vnd.ms-excel": ("spreadsheet", ".xls"),
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": (
        "spreadsheet",
        ".xlsx",
    ),
    "application/vnd.ms-excel.sheet.macroenabled.12": ("spreadsheet", ".xlsm"),
    "application/vnd.oasis.opendocument.spreadsheet": ("spreadsheet", ".ods"),
    "text/csv": ("spreadsheet", ".csv"),
    "application/csv": ("spreadsheet", ".csv"),
}

_EXTENSION_CATEGORY = {
    ".jpg": ("image", "image/jpeg"),
    ".jpeg": ("image", "image/jpeg"),
    ".png": ("image", "image/png"),
    ".webp": ("image", "image/webp"),
    ".heic": ("image", "image/heic"),
    ".heif": ("image", "image/heif"),
    ".pdf": ("pdf", "application/pdf"),
    ".xls": ("spreadsheet", "application/vnd.ms-excel"),
    ".xlsx": (
        "spreadsheet",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    ".xlsm": ("spreadsheet", "application/vnd.ms-excel.sheet.macroEnabled.12"),
    ".csv": ("spreadsheet", "text/csv"),
    ".ods": ("spreadsheet", "application/vnd.oasis.opendocument.spreadsheet"),
}


def normalize_mime(value: str | None) -> str:
    return str(value or "").split(";", 1)[0].strip().lower()


def safe_filename(value: str | None, *, fallback: str = "attachment") -> str:
    name = Path(str(value or "")).name.strip()
    if not name:
        name = fallback
    name = re.sub(r"[\x00-\x1f\x7f]+", "", name)
    name = re.sub(r"[^A-Za-z0-9._()\- ]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip(" .")
    return (name or fallback)[:180]


def safe_slug(value: str, *, fallback: str = "contact") -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", str(value or "").strip()).strip("-")
    return (slug or fallback)[:80].lower()


def classify_attachment_metadata(
    *,
    media_kind: str | None,
    mime_type: str | None,
    original_filename: str | None,
) -> str | None:
    mime = normalize_mime(mime_type)
    if mime in _MIME_CATEGORY:
        return _MIME_CATEGORY[mime][0]
    extension = Path(str(original_filename or "")).suffix.lower()
    if extension in _EXTENSION_CATEGORY:
        return _EXTENSION_CATEGORY[extension][0]
    if str(media_kind or "").lower() == "image":
        return "image"
    return None


def _looks_like_csv(data: bytes) -> bool:
    if not data or b"\x00" in data[:4096]:
        return False
    try:
        text = data[:65536].decode("utf-8-sig")
    except UnicodeDecodeError:
        return False
    if "\n" not in text:
        return False
    try:
        dialect = csv.Sniffer().sniff(text, delimiters=",;\t|")
        rows = list(csv.reader(io.StringIO(text), dialect))[:5]
    except (csv.Error, UnicodeError):
        return False
    return len(rows) >= 2 and max((len(row) for row in rows), default=0) >= 2


def detect_file_type(
    path: Path,
    *,
    declared_mime: str | None = None,
    original_filename: str | None = None,
) -> tuple[str, str, str]:
    """Return (detected MIME, category, safe extension), or raise ValueError."""
    with path.open("rb") as handle:
        head = handle.read(65536)

    if head.startswith(b"%PDF-"):
        return "application/pdf", "pdf", ".pdf"
    if head.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", "image", ".jpg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", "image", ".png"
    if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
        return "image/webp", "image", ".webp"
    if len(head) >= 12 and head[4:12] in {
        b"ftypheic",
        b"ftypheix",
        b"ftyphevc",
        b"ftyphevx",
        b"ftypmif1",
    }:
        return "image/heic", "image", ".heic"
    if head.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "application/vnd.ms-excel", "spreadsheet", ".xls"
    if head.startswith(b"PK\x03\x04"):
        try:
            with zipfile.ZipFile(path) as archive:
                names = set(archive.namelist())
                if "mimetype" in names:
                    mime = archive.read("mimetype")[:200].decode("ascii", "ignore").strip()
                    if mime == "application/vnd.oasis.opendocument.spreadsheet":
                        return mime, "spreadsheet", ".ods"
                if "[Content_Types].xml" in names and any(
                    name.startswith("xl/") for name in names
                ):
                    content_types = archive.read("[Content_Types].xml").lower()
                    if b"macroenabled" in content_types:
                        return (
                            "application/vnd.ms-excel.sheet.macroEnabled.12",
                            "spreadsheet",
                            ".xlsm",
                        )
                    return (
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "spreadsheet",
                        ".xlsx",
                    )
        except (zipfile.BadZipFile, KeyError, OSError):
            pass
    if _looks_like_csv(head):
        return "text/csv", "spreadsheet", ".csv"

    declared = normalize_mime(declared_mime)
    extension = Path(str(original_filename or "")).suffix.lower()
    guessed = mimetypes.guess_type(str(original_filename or ""))[0]
    detail = ", ".join(
        part for part in [f"declared={declared}" if declared else "", f"extension={extension}" if extension else "", f"guessed={guessed}" if guessed else ""] if part
    )
    raise ValueError(f"Unsupported or mismatched inbound file type{': ' + detail if detail else ''}")
