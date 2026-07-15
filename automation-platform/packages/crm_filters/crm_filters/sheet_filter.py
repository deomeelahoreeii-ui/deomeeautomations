from __future__ import annotations

import json
import re
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font

from crm_filters.intake import COMPLAINT_COLUMN, normalize_complaint_number, read_crm_dataframe
from crm_filters.paperless import ComplaintLookupResult


CATEGORIES = (
    "fresh",
    "uploaded_pending",
    "uploaded_not_relevant",
    "submitted",
    "manual_review",
)
CATEGORY_LABELS = {
    "fresh": "Fresh",
    "uploaded_pending": "Uploaded / Pending",
    "uploaded_not_relevant": "Uploaded / Not Relevant",
    "submitted": "Submitted",
    "manual_review": "Manual Review",
}


class ComplaintLookup(Protocol):
    def lookup_complaint(self, complaint_no: str) -> ComplaintLookupResult: ...


def _safe_stem(path: Path) -> str:
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", path.stem).strip("._-")
    return stem or "crm_sheet"


def _format_workbook(path: Path) -> None:
    workbook = load_workbook(path)
    sheet = workbook.active
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    for column_cells in sheet.columns:
        values = [str(cell.value or "") for cell in column_cells[:250]]
        width = min(max(max((len(value) for value in values), default=8) + 2, 10), 55)
        sheet.column_dimensions[column_cells[0].column_letter].width = width
    workbook.save(path)


def _write_workbook(frame: pd.DataFrame, path: Path) -> None:
    frame.to_excel(path, index=False)
    _format_workbook(path)


def run_sheet_filter(
    *,
    source_path: Path,
    output_dir: Path,
    client: ComplaintLookup,
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    logger = log or (lambda _message: None)
    output_dir.mkdir(parents=True, exist_ok=False)
    frame, header_row = read_crm_dataframe(source_path)
    if COMPLAINT_COLUMN not in frame.columns:
        raise ValueError(f"Required column '{COMPLAINT_COLUMN}' is unavailable.")

    logger(
        f"Loaded {len(frame)} rows from {source_path.name}; header detected on row {header_row + 1}."
    )
    cache: dict[str, ComplaintLookupResult] = {}
    row_categories: list[str] = []
    audit_rows: list[dict[str, Any]] = []
    counts = {category: 0 for category in CATEGORIES}
    cache_hits = 0

    total = len(frame)
    for position, (_, row) in enumerate(frame.iterrows(), start=1):
        complaint_no = normalize_complaint_number(row.get(COMPLAINT_COLUMN))
        if not complaint_no:
            result = ComplaintLookupResult(
                category="manual_review",
                reason="Complaint number is blank or invalid.",
            )
        elif complaint_no in cache:
            result = cache[complaint_no]
            cache_hits += 1
        else:
            result = client.lookup_complaint(complaint_no)
            cache[complaint_no] = result

        category = result.category if result.category in CATEGORIES else "manual_review"
        row_categories.append(category)
        counts[category] += 1
        audit = row.to_dict()
        audit.update(
            {
                "Normalized Complaint No": complaint_no,
                "Platform Classification": CATEGORY_LABELS[category],
                "Classification Reason": result.reason,
                "Matched Paperless Document IDs": ", ".join(
                    str(value) for value in result.matched_document_ids if value not in (None, "")
                ),
                "Matched Paperless Statuses": ", ".join(result.matched_statuses),
                "Lookup Error": result.error,
            }
        )
        audit_rows.append(audit)
        level = "MANUAL REVIEW" if category == "manual_review" else CATEGORY_LABELS[category].upper()
        if position <= 10 or position == total or position % 25 == 0 or category == "manual_review":
            logger(f"[{position}/{total}] {complaint_no or '<blank>'} -> {level}")

    artifact_paths: list[Path] = []
    stem = _safe_stem(source_path)
    for category in CATEGORIES:
        indices = [index for index, value in enumerate(row_categories) if value == category]
        if not indices:
            continue
        output_path = output_dir / f"{category}_{stem}.xlsx"
        _write_workbook(frame.iloc[indices].copy(), output_path)
        artifact_paths.append(output_path)
        logger(f"Saved {len(indices)} row(s) to {output_path.name}.")

    audit_path = output_dir / f"classification_audit_{stem}.xlsx"
    _write_workbook(pd.DataFrame(audit_rows), audit_path)
    artifact_paths.append(audit_path)

    summary = {
        "schema_version": "crm_sheet_filter_run_v1",
        "source_name": source_path.name,
        "generated_at": datetime.now(UTC).isoformat(),
        "header_row": header_row + 1,
        "total_rows": total,
        "unique_lookups": len(cache),
        "cache_hits": cache_hits,
        "counts": counts,
        "artifacts": [path.name for path in artifact_paths],
    }
    summary_path = output_dir / "run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    artifact_paths.append(summary_path)

    bundle_path = output_dir / f"crm_filter_results_{stem}.zip"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in artifact_paths:
            archive.write(path, arcname=path.name)
    artifact_paths.append(bundle_path)

    summary["artifacts"] = [path.name for path in artifact_paths]
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger("CRM sheet filtering completed successfully.")
    return {**summary, "artifact_paths": [str(path) for path in artifact_paths]}
