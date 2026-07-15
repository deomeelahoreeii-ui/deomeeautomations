from __future__ import annotations

import csv
import json
import zipfile

import pandas as pd

from crm_filters.sheet_to_pdf import convert_sheet_rows_to_pdfs


def test_sheet_rows_are_converted_to_pdf_bundle_with_manifest(tmp_path) -> None:
    source = tmp_path / "crm.xlsx"
    pd.DataFrame(
        {
            "Complaint No": ["104-1234567", "104-1234567", None, "104-7654321"],
            "Citizen": ["A", "B", "Missing", "C"],
            "Complaint Details": ["First", "Second", "Blank", "Third"],
        }
    ).to_excel(source, index=False)

    output = tmp_path / "run"
    logs: list[str] = []
    result = convert_sheet_rows_to_pdfs(
        source_path=source,
        output_dir=output,
        log=logs.append,
    )

    assert result["counts"] == {
        "created": 3,
        "skipped_blank": 1,
        "failed": 0,
        "total_rows": 4,
    }
    pdfs = sorted((output / "complaint-pdfs").glob("*.pdf"))
    assert len(pdfs) == 3
    assert pdfs[0].read_bytes().startswith(b"%PDF")
    assert any("__row-2.pdf" in path.name for path in pdfs)

    with (output / "conversion_manifest.csv").open(encoding="utf-8-sig", newline="") as handle:
        manifest = list(csv.DictReader(handle))
    assert [row["status"] for row in manifest] == ["created", "created", "skipped", "created"]

    summary = json.loads((output / "run_summary.json").read_text())
    assert summary["counts"]["created"] == 3

    bundle = output / "crm_complaint_pdfs_crm.zip"
    with zipfile.ZipFile(bundle) as archive:
        names = archive.namelist()
        assert "conversion_manifest.csv" in names
        assert "run_summary.json" in names
        assert len([name for name in names if name.startswith("complaint-pdfs/")]) == 3
    assert any("Conversion completed" in message for message in logs)
