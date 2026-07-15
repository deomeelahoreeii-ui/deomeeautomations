from __future__ import annotations

import argparse
import csv
import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from crm.extract import extract_crm_pdf, reviewed_complaint_number_override, sha256_file
from crm.paperless_index import refresh_paperless_index
from crm.store import (
    DEFAULT_CRM_CACHE_DB,
    ExtractedComplaint,
    connect,
    get_local_by_sha256,
    paperless_candidates,
    record_decision,
    save_local_complaint,
)

LOGGER = logging.getLogger("crm_duplicate_filter")
SUPPORTED_PDF_SUFFIXES = {".pdf"}


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
    try:
        from rapidfuzz import fuzz
    except ImportError:
        import difflib

        return difflib.SequenceMatcher(None, left, right).ratio()
    return float(fuzz.token_set_ratio(left, right)) / 100.0


def score_candidate(local: ExtractedComplaint, candidate: Any) -> MatchScore:
    candidate_number = str(candidate["complaint_number"] or "").strip()
    number_score = (
        1.0
        if local.complaint_number and local.complaint_number == candidate_number
        else 0.0
    )
    applicant_score = fuzzy_ratio(
        local.applicant_clean, str(candidate["applicant_clean"] or "")
    )
    remarks_score = fuzzy_ratio(local.remarks_clean, str(candidate["remarks_clean"] or ""))
    score = (number_score * 0.55) + (applicant_score * 0.20) + (remarks_score * 0.25)
    reasons = []
    if number_score:
        reasons.append("same complaint number")
    if applicant_score >= 0.86:
        reasons.append(f"applicant match {applicant_score:.2f}")
    if remarks_score >= 0.84:
        reasons.append(f"remarks match {remarks_score:.2f}")
    try:
        fields = json.loads(str(candidate["custom_fields_json"] or "{}"))
    except json.JSONDecodeError:
        fields = {}
    return MatchScore(
        document_id=int(candidate["document_id"]),
        title=str(candidate["title"] or ""),
        status=str(fields.get("Status") or "").strip(),
        score=score,
        reason=", ".join(reasons) or "weak similarity",
        number_score=number_score,
        applicant_score=applicant_score,
        remarks_score=remarks_score,
        local_remarks_length=len(local.remarks_clean),
    )


def best_match(local: ExtractedComplaint, candidates: list[Any]) -> MatchScore:
    scores = [score_candidate(local, candidate) for candidate in candidates]
    if not scores:
        return MatchScore(None, "", "", 0.0, "no Paperless candidates", 0.0, 0.0, 0.0, 0)
    return sorted(scores, key=lambda item: item.score, reverse=True)[0]


def status_decision(status: str) -> str:
    value = status.strip().lower()
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
    return "fresh"


def number_state(local: ExtractedComplaint) -> str:
    return "number-found" if local.complaint_number else "number-missing"


def output_folder(base: Path, decision: str, local: ExtractedComplaint) -> Path:
    mapping = {
        "uploaded_pending": base / "uploaded" / "pending",
        "uploaded_not_relevant": base / "uploaded" / "not-relevant",
        "submitted": base / "submitted",
        "duplicate_in_batch": base / "duplicates" / "in-batch",
        "manual_review": base / "manual-review",
        "ocr_failed": base / "ocr-failed",
        "fresh": base / "fresh",
    }
    return mapping[decision] / number_state(local)


def extract_with_cache(conn, pdf_path: Path) -> ExtractedComplaint:
    digest = sha256_file(pdf_path)
    cached = get_local_by_sha256(conn, digest)
    if cached and cached.complaint_number:
        return cached
    if (
        cached
        and cached.error
        and not cached.raw_text
        and not cached.ocr_text
        and not reviewed_complaint_number_override(pdf_path)
    ):
        return cached
    item = extract_crm_pdf(pdf_path)
    save_local_complaint(conn, item)
    return item


def copy_pdf(pdf_path: Path, target_dir: Path) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / pdf_path.name
    shutil.copy2(pdf_path, target)
    return target


def write_report(report_path: Path, rows: list[dict[str, Any]]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "file",
        "decision",
        "score",
        "paperless_document_id",
        "paperless_title",
        "paperless_status",
        "complaint_number",
        "number_state",
        "reason",
        "needs_review",
        "error",
    ]
    with report_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def clear_previous_pdf_outputs(output_dir: Path) -> None:
    for relative in (
        Path("fresh"),
        Path("manual-review"),
        Path("ocr-failed"),
        Path("submitted"),
        Path("duplicates") / "in-batch",
        Path("uploaded") / "pending",
        Path("uploaded") / "not-relevant",
    ):
        folder = output_dir / relative
        if not folder.exists():
            continue
        for pdf_path in folder.rglob("*.pdf"):
            pdf_path.unlink()
    report_path = output_dir / "crm-duplicate-filter-report.csv"
    if report_path.exists():
        report_path.unlink()


def run_duplicate_filter(
    project_root: Path,
    input_dir: Path,
    output_dir: Path,
    db_path: Path,
    *,
    refresh_index: bool = True,
    paperless_limit: int | None = None,
) -> int:
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        input_dir.relative_to(output_dir)
    except ValueError:
        pass
    else:
        raise ValueError(
            "Refusing to filter from inside the output folder because old output cleanup "
            f"would delete the input PDFs first. input_dir={input_dir} output_dir={output_dir}"
        )
    clear_previous_pdf_outputs(output_dir)

    if refresh_index:
        indexed = refresh_paperless_index(project_root, db_path, limit=paperless_limit)
        LOGGER.info("Paperless CRM index refreshed: %d document(s).", indexed)

    rows: list[dict[str, Any]] = []
    with connect(db_path) as conn:
        candidates = paperless_candidates(conn)
        matched_document_ids: dict[int, str] = {}
        pdfs = [
            path
            for path in sorted(input_dir.iterdir())
            if path.is_file() and path.suffix.lower() in SUPPORTED_PDF_SUFFIXES
        ]
        for index, pdf_path in enumerate(pdfs, start=1):
            try:
                local = extract_with_cache(conn, pdf_path)
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
            except Exception as exc:
                local = ExtractedComplaint(
                    source_path=str(pdf_path),
                    source_sha256=sha256_file(pdf_path),
                    needs_review=True,
                    error=str(exc),
                )
                save_local_complaint(conn, local)
                match = MatchScore(None, "", "", 0.0, "extraction failed", 0.0, 0.0, 0.0, 0)
                decision = "ocr_failed"

            copy_pdf(pdf_path, output_folder(output_dir, decision, local))
            record_decision(
                conn,
                source_sha256=local.source_sha256,
                source_path=str(pdf_path),
                paperless_document_id=match.document_id,
                decision=decision,
                score=match.score,
                reason=match.reason,
                details={
                    "number_score": match.number_score,
                    "applicant_score": match.applicant_score,
                    "remarks_score": match.remarks_score,
                    "paperless_title": match.title,
                },
            )
            rows.append(
                {
                    "file": pdf_path.name,
                    "decision": decision,
                    "score": f"{match.score:.3f}",
                    "paperless_document_id": match.document_id or "",
                    "paperless_title": match.title,
                    "paperless_status": match.status,
                    "complaint_number": local.complaint_number,
                    "number_state": number_state(local),
                    "reason": match.reason,
                    "needs_review": "yes" if local.needs_review else "no",
                    "error": local.error,
                }
            )
            LOGGER.info(
                "[%d/%d] %s -> %s score=%.3f %s",
                index,
                len(pdfs),
                pdf_path.name,
                decision,
                match.score,
                match.reason,
            )

    write_report(output_dir / "crm-duplicate-filter-report.csv", rows)
    LOGGER.info("Duplicate filter report written: %s", output_dir)
    return 0


def resolve_project_path(project_root: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="OCR and filter CRM PDFs against already uploaded Paperless complaints."
    )
    parser.add_argument("--input-dir", default="crm-main-complaints")
    parser.add_argument("--output-dir", default="phase1-crm/unprocessed-crm/filtered")
    parser.add_argument("--db", default=DEFAULT_CRM_CACHE_DB)
    parser.add_argument("--skip-paperless-refresh", action="store_true")
    parser.add_argument("--paperless-limit", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )
    args = parse_args()
    project_root = Path(__file__).resolve().parents[1]
    return run_duplicate_filter(
        project_root,
        resolve_project_path(project_root, args.input_dir),
        resolve_project_path(project_root, args.output_dir),
        resolve_project_path(project_root, args.db),
        refresh_index=not args.skip_paperless_refresh,
        paperless_limit=args.paperless_limit,
    )


if __name__ == "__main__":
    raise SystemExit(main())
