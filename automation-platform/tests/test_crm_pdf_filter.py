from __future__ import annotations

import json
import zipfile
from pathlib import Path

from crm_filters.paperless import PaperlessComplaintCandidate
from crm_filters.pdf_extract import ExtractedComplaint
from crm_filters.pdf_filter import run_pdf_filter


def candidate(number: str, status: str, document_id: int) -> PaperlessComplaintCandidate:
    return PaperlessComplaintCandidate(
        document_id=document_id,
        title=f"Complaint {number}",
        status=status,
        complaint_number=number,
        applicant_text="Applicant",
        applicant_clean="applicant",
        remarks_text="Complaint details",
        remarks_clean="complaint details",
    )


def test_pdf_filter_separates_all_safety_categories_and_builds_artifacts(tmp_path: Path) -> None:
    names = [
        "fresh.pdf",
        "pending.pdf",
        "submitted.pdf",
        "not-relevant.pdf",
        "duplicate.pdf",
        "manual.pdf",
        "ocr-failed.pdf",
    ]
    paths = []
    for name in names:
        path = tmp_path / name
        path.write_bytes(b"%PDF-1.4\n" + name.encode() + b"\n%%EOF\n")
        paths.append(path)

    extracted = {
        "fresh.pdf": ExtractedComplaint(str(paths[0]), "fresh-hash", "104-1000001", raw_text="text", extraction_method="pdftotext+field", confidence=0.8),
        "pending.pdf": ExtractedComplaint(str(paths[1]), "pending-hash", "104-1000002", raw_text="text", extraction_method="pdftotext+field", confidence=0.8),
        "submitted.pdf": ExtractedComplaint(str(paths[2]), "submitted-hash", "104-1000003", raw_text="text", extraction_method="pdftotext+field", confidence=0.8),
        "not-relevant.pdf": ExtractedComplaint(str(paths[3]), "not-hash", "104-1000004", raw_text="text", extraction_method="pdftotext+field", confidence=0.8),
        "duplicate.pdf": ExtractedComplaint(str(paths[4]), "pending-hash", "104-1000002", raw_text="text", extraction_method="pdftotext+field", confidence=0.8),
        "manual.pdf": ExtractedComplaint(str(paths[5]), "manual-hash", needs_review=True, raw_text="uncertain text", extraction_method="pdftotext", confidence=0.1),
        "ocr-failed.pdf": ExtractedComplaint(str(paths[6]), "ocr-hash", needs_review=True, extraction_method="tesseract", error="OCR failed"),
    }

    summary = run_pdf_filter(
        pdf_paths=paths,
        output_dir=tmp_path / "output",
        candidates=[
            candidate("104-1000002", "Pending", 2),
            candidate("104-1000003", "Submitted", 3),
            candidate("104-1000004", "Not Relevant", 4),
        ],
        extractor=lambda path: extracted[path.name],
    )

    assert summary["counts"] == {
        "fresh": 1,
        "uploaded_pending": 1,
        "uploaded_not_relevant": 1,
        "submitted": 1,
        "duplicate_in_batch": 1,
        "manual_review": 1,
        "ocr_failed": 1,
    }
    artifacts = {Path(path).name for path in summary["artifact_paths"]}
    assert "crm_pdf_duplicate_filter_report.xlsx" in artifacts
    assert "run_summary.json" in artifacts
    assert "crm_pdf_filter_results.zip" in artifacts

    manifest = json.loads((tmp_path / "output" / "run_summary.json").read_text())
    assert manifest["artifacts"] == [Path(path).name for path in summary["artifact_paths"]]
    with zipfile.ZipFile(tmp_path / "output" / "crm_pdf_filter_results.zip") as archive:
        members = set(archive.namelist())
    assert "run_summary.json" in members
    assert any(name.startswith("categorized/duplicate_in_batch/") for name in members)
