from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


REQUIRED_COLUMNS = {"username"}
ACTIVITY_COLUMNS = {
    "simple activities",
    "patient activities",
    "vector surveillance activities",
    "larvae case response",
    "tpv activities",
    "total activities",
}
ALLOWED_EXTENSIONS = {".xls", ".xlsx", ".csv"}


def safe_filename(filename: str) -> str:
    name = Path(filename or "report").name
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(name).stem).strip("._-")
    suffix = Path(name).suffix.lower()
    return f"{stem or 'report'}{suffix}"


def _read_report(path: Path) -> pd.DataFrame:
    with path.open("rb") as handle:
        signature = handle.read(8)
    if signature.startswith(b"PK"):
        return pd.read_excel(path, dtype=str, engine="openpyxl")
    if signature.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return pd.read_excel(path, dtype=str, engine="xlrd")
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig")


def validate_antidengue_report(path: Path) -> tuple[str, str | None, dict[str, Any], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}
    try:
        frame = _read_report(path)
    except Exception as exc:
        return "invalid", None, metadata, [f"The report could not be read: {exc}"], warnings

    normalized = {str(column).strip().lower(): str(column).strip() for column in frame.columns}
    missing = sorted(REQUIRED_COLUMNS - set(normalized))
    if missing:
        errors.append("Missing required columns: " + ", ".join(missing))
    if frame.empty:
        errors.append("The report contains no data rows")

    available_activity = sorted(ACTIVITY_COLUMNS & set(normalized))
    missing_activity = sorted(ACTIVITY_COLUMNS - set(normalized))
    if not available_activity:
        errors.append("No activity columns were detected in this user dormancy export")
    elif missing_activity:
        warnings.append("Some activity columns are absent: " + ", ".join(missing_activity))

    username_column = normalized.get("username")
    school_count = 0
    invalid_username_count = 0
    if username_column:
        extracted_emis = frame[username_column].astype(str).str.extract(r"^(\d+)", expand=False)
        school_count = int(extracted_emis.dropna().nunique())
        invalid_username_count = int(extracted_emis.isna().sum())
        if invalid_username_count:
            warnings.append(
                f"{invalid_username_count} username row(s) do not begin with a school EMIS code"
            )

    dormant_candidate_count = None
    total_column = normalized.get("total activities")
    if total_column:
        total_values = pd.to_numeric(frame[total_column], errors="coerce")
        dormant_candidate_count = int(total_values.eq(0).sum())
    metadata = {
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "school_count": school_count,
        "invalid_username_count": invalid_username_count,
        "dormant_candidate_count": dormant_candidate_count,
        "columns": [str(column).strip() for column in frame.columns],
        "available_activity_columns": [normalized[name] for name in available_activity],
    }
    return ("invalid" if errors else "valid"), "antidengue_user_dormancy_v1", metadata, errors, warnings
