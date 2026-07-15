from __future__ import annotations

from openpyxl import Workbook

from crm_filters.intake import COMPLAINT_COLUMN, read_crm_dataframe, validate_crm_sheet


def test_crm_intake_detects_preamble_header_and_duplicate_values(tmp_path) -> None:
    path = tmp_path / "crm.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["CRM export generated today"])
    sheet.append([])
    sheet.append([" complaint   no ", "Citizen Name"])
    sheet.append(["104-1001", "A"])
    sheet.append(["104-1001", "B"])
    sheet.append([None, "C"])
    workbook.save(path)

    status, schema, metadata, errors, warnings = validate_crm_sheet(path)
    frame, header_row = read_crm_dataframe(path)

    assert status == "valid"
    assert schema == "crm_sheet_v1"
    assert errors == []
    assert header_row == 2
    assert COMPLAINT_COLUMN in frame.columns
    assert metadata["header_row"] == 3
    assert metadata["row_count"] == 3
    assert metadata["unique_complaint_count"] == 1
    assert metadata["blank_complaint_count"] == 1
    assert metadata["duplicate_row_count"] == 1
    assert any("Manual Review" in warning for warning in warnings)
    assert any("repeated" in warning for warning in warnings)


def test_crm_intake_rejects_sheet_without_complaint_column(tmp_path) -> None:
    path = tmp_path / "invalid.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Reference", "Citizen"])
    sheet.append(["A-1", "Person"])
    workbook.save(path)

    status, schema, metadata, errors, _warnings = validate_crm_sheet(path)

    assert status == "invalid"
    assert schema == "crm_sheet_v1"
    assert metadata["complaint_column"] is None
    assert any("Complaint No" in error for error in errors)
