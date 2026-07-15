from __future__ import annotations

import csv
import json
import re
import zipfile
from collections import Counter
from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from crm_filters.intake import COMPLAINT_COLUMN, normalize_complaint_number, read_crm_dataframe


def _safe_stem(value: str, fallback: str = "complaint") -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
    return cleaned or fallback


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Decimal):
        return format(value, "f")
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "null", "nat"} else text


def _escape_text(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\n", "<br/>")
    )


def _write_complaint_pdf(
    *,
    output_path: Path,
    complaint_number: str,
    workbook_name: str,
    row_index: int,
    row_data: list[tuple[str, str]],
) -> None:
    styles = getSampleStyleSheet()
    normal = ParagraphStyle(
        "CrmRowValue",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=11,
        alignment=TA_LEFT,
        wordWrap="CJK",
    )
    label = ParagraphStyle(
        "CrmRowLabel",
        parent=normal,
        fontName="Helvetica-Bold",
        textColor=colors.HexColor("#1f2937"),
    )
    title = ParagraphStyle(
        "CrmComplaintTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=20,
        spaceAfter=8,
        textColor=colors.HexColor("#111827"),
    )
    meta = ParagraphStyle(
        "CrmMeta",
        parent=normal,
        fontSize=8,
        leading=10,
        textColor=colors.HexColor("#4b5563"),
        spaceAfter=12,
    )
    long_label = ParagraphStyle(
        "CrmLongLabel",
        parent=label,
        fontSize=10,
        spaceBefore=10,
        spaceAfter=4,
    )

    document = SimpleDocTemplate(
        str(output_path),
        pagesize=A4,
        rightMargin=16 * mm,
        leftMargin=16 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title=f"Complaint {complaint_number}",
        author="Deomee Automation Platform",
    )
    story: list[Any] = [
        Paragraph(f"Complaint {_escape_text(complaint_number)}", title),
        Paragraph(
            _escape_text(f"Source: {workbook_name} | Data row: {row_index}"),
            meta,
        ),
    ]

    table_rows: list[list[Paragraph]] = []
    long_items: list[tuple[str, str]] = []
    for field, value in row_data:
        if value and (len(value) > 1500 or value.count("\n") > 25):
            long_items.append((field, value))
        else:
            table_rows.append(
                [
                    Paragraph(_escape_text(field), label),
                    Paragraph(_escape_text(value) if value else "-", normal),
                ]
            )

    if table_rows:
        table = Table(table_rows, colWidths=[48 * mm, 130 * mm], repeatRows=0)
        table.setStyle(
            TableStyle(
                [
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3f4f6")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#d1d5db")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        story.append(table)

    if long_items:
        story.append(Spacer(1, 6 * mm))
        for field, value in long_items:
            story.append(Paragraph(_escape_text(field), long_label))
            story.append(Paragraph(_escape_text(value), normal))
            story.append(Spacer(1, 4 * mm))

    document.build(story)


def convert_sheet_rows_to_pdfs(
    *,
    source_path: Path,
    output_dir: Path,
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    logger = log or (lambda _message: None)
    output_dir.mkdir(parents=True, exist_ok=False)
    pdf_dir = output_dir / "complaint-pdfs"
    pdf_dir.mkdir()

    frame, header_row = read_crm_dataframe(source_path)
    if COMPLAINT_COLUMN not in frame.columns:
        raise ValueError(f"Required column '{COMPLAINT_COLUMN}' is unavailable.")

    total = len(frame)
    logger(
        f"Loaded {total} row(s) from {source_path.name}; header detected on row {header_row + 1}."
    )
    complaint_occurrences: Counter[str] = Counter()
    manifest_rows: list[dict[str, Any]] = []
    created_paths: list[Path] = []
    skipped_blank = 0
    failed = 0

    for position, (_, row) in enumerate(frame.iterrows(), start=1):
        complaint_number = normalize_complaint_number(row.get(COMPLAINT_COLUMN))
        if not complaint_number:
            skipped_blank += 1
            manifest_rows.append(
                {
                    "row": position,
                    "complaint_number": "",
                    "status": "skipped",
                    "file": "",
                    "message": "Complaint number is blank or invalid.",
                }
            )
            logger(f"[{position}/{total}] skipped row without a complaint number.")
            continue

        complaint_occurrences[complaint_number] += 1
        occurrence = complaint_occurrences[complaint_number]
        filename = f"{_safe_stem(complaint_number)}.pdf"
        if occurrence > 1:
            filename = f"{_safe_stem(complaint_number)}__row-{position}.pdf"
        output_path = pdf_dir / filename
        row_data = [(str(column), _clean_text(row.get(column))) for column in frame.columns]

        try:
            _write_complaint_pdf(
                output_path=output_path,
                complaint_number=complaint_number,
                workbook_name=source_path.name,
                row_index=position,
                row_data=row_data,
            )
        except Exception as exc:
            failed += 1
            manifest_rows.append(
                {
                    "row": position,
                    "complaint_number": complaint_number,
                    "status": "failed",
                    "file": "",
                    "message": str(exc),
                }
            )
            logger(f"[{position}/{total}] {complaint_number} failed: {exc}")
            continue

        created_paths.append(output_path)
        manifest_rows.append(
            {
                "row": position,
                "complaint_number": complaint_number,
                "status": "created",
                "file": filename,
                "message": "",
            }
        )
        if position <= 10 or position == total or position % 10 == 0:
            logger(f"[{position}/{total}] created {filename}.")

    manifest_path = output_dir / "conversion_manifest.csv"
    with manifest_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["row", "complaint_number", "status", "file", "message"],
        )
        writer.writeheader()
        writer.writerows(manifest_rows)

    counts = {
        "created": len(created_paths),
        "skipped_blank": skipped_blank,
        "failed": failed,
        "total_rows": total,
    }
    summary = {
        "schema_version": "crm_sheet_to_pdf_run_v1",
        "source_name": source_path.name,
        "generated_at": datetime.now(UTC).isoformat(),
        "header_row": header_row + 1,
        "counts": counts,
    }
    summary_path = output_dir / "run_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    bundle_path = output_dir / f"crm_complaint_pdfs_{_safe_stem(source_path.stem, 'crm_sheet')}.zip"
    with zipfile.ZipFile(bundle_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in created_paths:
            archive.write(path, arcname=f"complaint-pdfs/{path.name}")
        archive.write(manifest_path, arcname=manifest_path.name)
        archive.write(summary_path, arcname=summary_path.name)

    artifact_paths = [bundle_path, manifest_path, summary_path]
    logger(
        f"Conversion completed: {len(created_paths)} PDF(s) created, "
        f"{skipped_blank} blank row(s) skipped, {failed} failure(s)."
    )
    return {
        **summary,
        "artifact_paths": [str(path) for path in artifact_paths],
        "pdf_count": len(created_paths),
    }
