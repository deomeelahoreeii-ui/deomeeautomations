from __future__ import annotations

import csv
import json
import re
import shutil
import zipfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Font

from crm_filters.paperless import PaperlessComplaintCandidate
from crm_filters.pdf_extract import ExtractedComplaint, extract_crm_pdf


CATEGORIES = (
    "fresh",
    "uploaded_pending",
    "uploaded_not_relevant",
    "submitted",
    "duplicate_in_batch",
    "manual_review",
    "ocr_failed",
)
CATEGORY_LABELS = {
    "fresh": "Fresh",
    "uploaded_pending": "Uploaded / Pending",
    "uploaded_not_relevant": "Uploaded / Not Relevant",
    "submitted": "Submitted",
    "duplicate_in_batch": "Duplicate in Batch",
    "manual_review": "Manual Review",
    "ocr_failed": "OCR Failed",
}


@dataclass(frozen=True)
class MatchScore:
    document_id: int | None
    title: str
    status: str
    score: float
    reason: str
    number_score: float
    applicant_score: float
    remarks_score: float
    local_remarks_length: int


def fuzzy_ratio(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    left_tokens = " ".join(sorted(set(left.split())))
    right_tokens = " ".join(sorted(set(right.split())))
    return SequenceMatcher(None, left_tokens, right_tokens).ratio()


def score_candidate(local: ExtractedComplaint, candidate: PaperlessComplaintCandidate) -> MatchScore:
    number_score = (
        1.0
        if local.complaint_number
        and local.complaint_number == candidate.complaint_number.strip()
        else 0.0
    )
    applicant_score = fuzzy_ratio(local.applicant_clean, candidate.applicant_clean)
    remarks_score = fuzzy_ratio(local.remarks_clean, candidate.remarks_clean)
    score = (number_score * 0.55) + (applicant_score * 0.20) + (remarks_score * 0.25)
    reasons: list[str] = []
    if number_score:
        reasons.append("same complaint number")
    if applicant_score >= 0.86:
        reasons.append(f"applicant match {applicant_score:.2f}")
    if remarks_score >= 0.84:
        reasons.append(f"remarks match {remarks_score:.2f}")
    return MatchScore(
        document_id=candidate.document_id,
        title=candidate.title,
        status=candidate.status,
        score=score,
        reason=", ".join(reasons) or "weak similarity",
        number_score=number_score,
        applicant_score=applicant_score,
        remarks_score=remarks_score,
        local_remarks_length=len(local.remarks_clean),
    )


def best_match(
    local: ExtractedComplaint,
    candidates: list[PaperlessComplaintCandidate],
) -> MatchScore:
    candidate_pool = candidates
    if local.complaint_number:
        exact = [
            candidate
            for candidate in candidates
            if candidate.complaint_number.strip() == local.complaint_number
        ]
        if exact:
            candidate_pool = exact
    scores = [score_candidate(local, candidate) for candidate in candidate_pool]
    if not scores:
        return MatchScore(
            None,
            "",
            "",
            0.0,
            "no Paperless candidates",
            0.0,
            0.0,
            0.0,
            0,
        )
    return max(scores, key=lambda item: item.score)


def status_decision(status: str) -> str:
    value = status.strip().casefold()
    if value == "submitted":
        return "submitted"
    if value == "not relevant":
        return "uploaded_not_relevant"
    return "uploaded_pending"


def decision_for(local: ExtractedComplaint, match: MatchScore) -> str:
    if local.error and not local.raw_text and not local.ocr_text:
        return "ocr_failed"
    if local.complaint_number and match.number_score == 1.0:
        return status_decision(match.status)
    if match.remarks_score >= 0.90 and match.local_remarks_length >= 120:
        return status_decision(match.status)
    if match.applicant_score >= 0.82 and match.remarks_score >= 0.82:
        return status_decision(match.status)
    if match.remarks_score >= 0.84 and match.local_remarks_length >= 120:
        return "manual_review"
    if match.score >= 0.65:
        return "manual_review"
    if local.needs_review and not local.complaint_number:
        return "manual_review"
    return "fresh"


def number_state(local: ExtractedComplaint) -> str:
    return "number-found" if local.complaint_number else "number-missing"


def _safe_path_name(name: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9._ -]+", "_", Path(name).stem).strip(" ._")
    stem = re.sub(r"\s+", " ", stem)[:160] or "complaint"
    return f"{stem}.pdf"


def _copy_unique(source: Path, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    candidate = target_dir / _safe_path_name(source.name)
    counter = 2
    while candidate.exists():
        candidate = target_dir / f"{candidate.stem} ({counter}){candidate.suffix}"
        counter += 1
    shutil.copy2(source, candidate)
    return candidate


def _format_report_workbook(path: Path) -> None:
    workbook = load_workbook(path)
    sheet = workbook.active
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for cell in sheet[1]:
        cell.font = Font(bold=True)
    for cells in sheet.columns:
        values = [str(cell.value or "") for cell in cells[:250]]
        width = min(max(max((len(value) for value in values), default=8) + 2, 10), 70)
        sheet.column_dimensions[cells[0].column_letter].width = width
    workbook.save(path)


def _write_reports(output_dir: Path, rows: list[dict[str, Any]]) -> list[Path]:
    columns = [
        "file",
        "decision",
        "score",
        "paperless_document_id",
        "paperless_title",
        "paperless_status",
        "complaint_number",
        "number_state",
        "extraction_method",
        "confidence",
        "number_score",
        "applicant_score",
        "remarks_score",
        "reason",
        "needs_review",
        "error",
    ]
    csv_path = output_dir / "crm_pdf_duplicate_filter_report.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    xlsx_path = output_dir / "crm_pdf_duplicate_filter_report.xlsx"
    pd.DataFrame(rows, columns=columns).to_excel(xlsx_path, index=False)
    _format_report_workbook(xlsx_path)
    return [csv_path, xlsx_path]


def _zip_paths(zip_path: Path, entries: list[tuple[Path, str]]) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path, arcname in entries:
            archive.write(path, arcname=arcname)


def run_pdf_filter(
    *,
    pdf_paths: list[Path],
    output_dir: Path,
    candidates: list[PaperlessComplaintCandidate],
    log: Callable[[str], None] | None = None,
    extractor: Callable[[Path], ExtractedComplaint] = extract_crm_pdf,
) -> dict[str, Any]:
    logger = log or (lambda _message: None)
    output_dir.mkdir(parents=True, exist_ok=False)
    categorized_root = output_dir / "categorized"
    counts = {category: 0 for category in CATEGORIES}
    rows: list[dict[str, Any]] = []
    copied: dict[str, list[Path]] = {category: [] for category in CATEGORIES}
    seen_hashes: dict[str, str] = {}
    matched_document_ids: dict[int, str] = {}

    total = len(pdf_paths)
    logger(f"Loaded {total} CRM PDF(s); Paperless index contains {len(candidates)} candidate(s).")
    for position, pdf_path in enumerate(pdf_paths, start=1):
        try:
            local = extractor(pdf_path)
        except Exception as exc:
            local = ExtractedComplaint(
                source_path=str(pdf_path),
                source_sha256="",
                needs_review=True,
                error=f"Extraction failed: {exc}",
            )

        if local.source_sha256 and local.source_sha256 in seen_hashes:
            match = MatchScore(
                None,
                "",
                "",
                1.0,
                f"exact file duplicate of {seen_hashes[local.source_sha256]}",
                0.0,
                0.0,
                0.0,
                len(local.remarks_clean),
            )
            decision = "duplicate_in_batch"
        else:
            if local.source_sha256:
                seen_hashes[local.source_sha256] = pdf_path.name
            match = best_match(local, candidates)
            decision = decision_for(local, match)
            if (
                decision in {"uploaded_pending", "uploaded_not_relevant", "submitted"}
                and match.document_id is not None
                and match.document_id in matched_document_ids
            ):
                decision = "duplicate_in_batch"
                match = MatchScore(
                    match.document_id,
                    match.title,
                    match.status,
                    match.score,
                    f"same Paperless document as {matched_document_ids[match.document_id]}",
                    match.number_score,
                    match.applicant_score,
                    match.remarks_score,
                    match.local_remarks_length,
                )
            elif (
                decision in {"uploaded_pending", "uploaded_not_relevant", "submitted"}
                and match.document_id is not None
            ):
                matched_document_ids[match.document_id] = pdf_path.name

        if decision not in CATEGORIES:
            decision = "manual_review"
        target_dir = categorized_root / decision / number_state(local)
        copied_path = _copy_unique(pdf_path, target_dir)
        copied[decision].append(copied_path)
        counts[decision] += 1
        rows.append(
            {
                "file": pdf_path.name,
                "decision": decision,
                "score": round(match.score, 3),
                "paperless_document_id": match.document_id or "",
                "paperless_title": match.title,
                "paperless_status": match.status,
                "complaint_number": local.complaint_number,
                "number_state": number_state(local),
                "extraction_method": local.extraction_method,
                "confidence": round(local.confidence, 3),
                "number_score": round(match.number_score, 3),
                "applicant_score": round(match.applicant_score, 3),
                "remarks_score": round(match.remarks_score, 3),
                "reason": match.reason,
                "needs_review": "yes" if local.needs_review else "no",
                "error": local.error,
            }
        )
        logger(
            f"[{position}/{total}] {pdf_path.name} -> {CATEGORY_LABELS[decision]} "
            f"score={match.score:.3f}; {match.reason}"
        )

    artifact_paths = _write_reports(output_dir, rows)
    for category in CATEGORIES:
        if not copied[category]:
            continue
        category_zip = output_dir / f"{category}_pdfs.zip"
        entries = [
            (path, f"{number_state_dir}/{path.name}")
            for path in copied[category]
            for number_state_dir in [path.parent.name]
        ]
        _zip_paths(category_zip, entries)
        artifact_paths.append(category_zip)

    summary_path = output_dir / "run_summary.json"
    bundle_path = output_dir / "crm_pdf_filter_results.zip"
    planned_artifacts = [path.name for path in artifact_paths] + [
        summary_path.name,
        bundle_path.name,
    ]
    summary = {
        "schema_version": "crm_pdf_filter_run_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "total_pdfs": total,
        "paperless_candidates": len(candidates),
        "counts": counts,
        "artifacts": planned_artifacts,
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    artifact_paths.append(summary_path)

    bundle_entries: list[tuple[Path, str]] = []
    for category in CATEGORIES:
        for path in copied[category]:
            bundle_entries.append(
                (
                    path,
                    f"categorized/{category}/{path.parent.name}/{path.name}",
                )
            )
    for path in artifact_paths:
        bundle_entries.append((path, path.name))
    _zip_paths(bundle_path, bundle_entries)
    artifact_paths.append(bundle_path)
    logger("CRM PDF duplicate filtering completed successfully.")
    return {**summary, "artifact_paths": [str(path) for path in artifact_paths]}
