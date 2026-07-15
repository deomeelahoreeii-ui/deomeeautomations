from __future__ import annotations

import math
import re
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


COMPLAINT_COLUMN = "Complaint No"
HEADER_SCAN_ROWS = 20
ALLOWED_EXTENSIONS = {".csv", ".xls", ".xlsx", ".xlsm", ".xltx", ".xltm"}


def safe_filename(filename: str) -> str:
    name = Path(filename or "crm-sheet").name
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(name).stem).strip("._-")
    suffix = Path(name).suffix.lower()
    return f"{stem or 'crm-sheet'}{suffix}"


def clean_column_name(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\ufeff", "")).strip()


def normalized_column_name(value: object) -> str:
    return clean_column_name(value).casefold()


def complaint_column_in(columns: Any) -> str | None:
    expected = normalized_column_name(COMPLAINT_COLUMN)
    for column in columns:
        if normalized_column_name(column) == expected:
            return str(column)
    return None


def normalize_complaint_number(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value):
            return ""
        if value.is_integer():
            return str(int(value))
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text or text.casefold() in {"nan", "none", "null", "nat"}:
        return ""
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]
    return text


def _read_csv(path: Path, *, header: int | None, nrows: int | None) -> pd.DataFrame:
    errors: list[Exception] = []
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return pd.read_csv(
                path,
                header=header,
                nrows=nrows,
                dtype=object,
                encoding=encoding,
            )
        except UnicodeDecodeError as exc:
            errors.append(exc)
    raise errors[-1]


def read_tabular_file(
    path: Path,
    *,
    header: int | None = 0,
    nrows: int | None = None,
) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return _read_csv(path, header=header, nrows=nrows)
    return pd.read_excel(path, header=header, nrows=nrows, dtype=object)


def detect_header_row(path: Path) -> int | None:
    raw = read_tabular_file(path, header=None, nrows=HEADER_SCAN_ROWS)
    expected = normalized_column_name(COMPLAINT_COLUMN)
    for index, row in raw.iterrows():
        if any(normalized_column_name(value) == expected for value in row.tolist()):
            return int(index)
    return None


def read_crm_dataframe(path: Path) -> tuple[pd.DataFrame, int]:
    header_row = detect_header_row(path)
    if header_row is None:
        # Read the conventional first-row header to provide useful validation metadata.
        frame = read_tabular_file(path, header=0)
        frame.columns = [clean_column_name(column) for column in frame.columns]
        return frame, 0

    frame = read_tabular_file(path, header=header_row)
    frame.columns = [clean_column_name(column) for column in frame.columns]
    complaint_column = complaint_column_in(frame.columns)
    if complaint_column and complaint_column != COMPLAINT_COLUMN:
        frame = frame.rename(columns={complaint_column: COMPLAINT_COLUMN})
    frame = frame.dropna(how="all").reset_index(drop=True)
    return frame, header_row


def validate_crm_sheet(
    path: Path,
) -> tuple[str, str | None, dict[str, Any], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    metadata: dict[str, Any] = {}

    if path.suffix.lower() not in ALLOWED_EXTENSIONS:
        return (
            "invalid",
            None,
            metadata,
            ["Unsupported file type. Upload an Excel workbook or CSV file."],
            warnings,
        )

    try:
        frame, header_row = read_crm_dataframe(path)
    except Exception as exc:
        return "invalid", None, metadata, [f"The CRM sheet could not be read: {exc}"], warnings

    complaint_column = complaint_column_in(frame.columns)
    if complaint_column is None:
        errors.append(
            f"Required column '{COMPLAINT_COLUMN}' was not found in the first {HEADER_SCAN_ROWS} rows."
        )

    if frame.empty:
        errors.append("The CRM sheet contains no data rows.")

    complaint_values: list[str] = []
    if complaint_column is not None:
        complaint_values = [normalize_complaint_number(value) for value in frame[complaint_column]]
        blank_count = sum(not value for value in complaint_values)
        if blank_count:
            warnings.append(
                f"{blank_count} row(s) have no complaint number and will be placed in Manual Review."
            )
        nonblank = [value for value in complaint_values if value]
        frequencies = Counter(nonblank)
        duplicate_row_count = sum(count - 1 for count in frequencies.values() if count > 1)
        duplicate_value_count = sum(1 for count in frequencies.values() if count > 1)
        if duplicate_row_count:
            warnings.append(
                f"{duplicate_row_count} repeated row(s) across {duplicate_value_count} complaint number(s) were detected; all rows will be preserved."
            )
    else:
        blank_count = 0
        duplicate_row_count = 0
        duplicate_value_count = 0
        nonblank = []

    metadata = {
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "columns": [clean_column_name(column) for column in frame.columns],
        "header_row": int(header_row + 1),
        "complaint_column": complaint_column,
        "blank_complaint_count": int(blank_count),
        "unique_complaint_count": int(len(set(nonblank))),
        "duplicate_row_count": int(duplicate_row_count),
        "duplicate_complaint_count": int(duplicate_value_count),
    }
    return ("invalid" if errors else "valid"), "crm_sheet_v1", metadata, errors, warnings
