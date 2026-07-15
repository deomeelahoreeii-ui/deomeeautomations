from __future__ import annotations

import json
import zipfile

import pandas as pd

from crm_filters.paperless import ComplaintLookupResult
from crm_filters.sheet_filter import run_sheet_filter


class FakePaperless:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def lookup_complaint(self, complaint_no: str) -> ComplaintLookupResult:
        self.calls.append(complaint_no)
        mapping = {
            "104-1": ComplaintLookupResult("fresh", "No match"),
            "104-2": ComplaintLookupResult("uploaded_pending", "Pending", [2], ["Pending"]),
            "104-3": ComplaintLookupResult("submitted", "Submitted", [3], ["Submitted"]),
            "104-4": ComplaintLookupResult(
                "manual_review",
                "Paperless lookup failed; the row was not treated as fresh.",
                error="timeout",
            ),
        }
        return mapping[complaint_no]


def test_native_sheet_filter_writes_categories_audit_summary_and_bundle(tmp_path) -> None:
    source = tmp_path / "input.xlsx"
    pd.DataFrame(
        {
            "Complaint No": ["104-1", "104-2", "104-2", "104-3", "104-4", None],
            "Citizen": ["A", "B", "B2", "C", "D", "E"],
        }
    ).to_excel(source, index=False)
    output = tmp_path / "run"
    client = FakePaperless()

    summary = run_sheet_filter(source_path=source, output_dir=output, client=client)

    assert summary["counts"] == {
        "fresh": 1,
        "uploaded_pending": 2,
        "uploaded_not_relevant": 0,
        "submitted": 1,
        "manual_review": 2,
    }
    assert client.calls == ["104-1", "104-2", "104-3", "104-4"]
    assert summary["cache_hits"] == 1
    assert (output / "fresh_input.xlsx").is_file()
    assert (output / "uploaded_pending_input.xlsx").is_file()
    assert not (output / "uploaded_not_relevant_input.xlsx").exists()
    assert (output / "manual_review_input.xlsx").is_file()

    audit = pd.read_excel(output / "classification_audit_input.xlsx")
    assert "Classification Reason" in audit.columns
    assert "Lookup Error" in audit.columns
    assert audit.loc[4, "Lookup Error"] == "timeout"

    manifest = json.loads((output / "run_summary.json").read_text())
    assert manifest["total_rows"] == 6
    bundle = output / "crm_filter_results_input.zip"
    with zipfile.ZipFile(bundle) as archive:
        assert "classification_audit_input.xlsx" in archive.namelist()
        assert "run_summary.json" in archive.namelist()
