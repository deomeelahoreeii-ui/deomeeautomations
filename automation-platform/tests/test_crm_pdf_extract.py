from __future__ import annotations

from pathlib import Path

from crm_filters import pdf_extract


def test_complaint_number_detection_handles_exact_and_spaced_numbers() -> None:
    values = pdf_extract.complaint_numbers_in(
        "Complaint No: 104 - 6609317\nReference 000-12345"
    )

    assert values == ["000-12345", "104-6609317"]


def test_extract_crm_pdf_uses_text_fields_without_ocr(tmp_path: Path, monkeypatch) -> None:
    pdf = tmp_path / "complaint.pdf"
    pdf.write_bytes(b"%PDF-1.4\ncontent\n%%EOF\n")
    text = """Complaint No: 104-6609317
Person Name: Fatima Khan
Mobile No: 03001234567
Person Address: Lahore
Complaint District: Lahore
Tehsil: Model Town
Complaint Remarks: The school demanded an unauthorized charge and refused to issue a receipt for the parent.
"""
    monkeypatch.setattr(pdf_extract, "extract_pdf_text", lambda _path: text)
    monkeypatch.setattr(
        pdf_extract,
        "tesseract_ocr_first_page",
        lambda _path: (_ for _ in ()).throw(AssertionError("OCR should not run")),
    )

    result = pdf_extract.extract_crm_pdf(pdf)

    assert result.complaint_number == "104-6609317"
    assert "fatima" in result.applicant_clean
    assert "unauthorized" in result.remarks_clean
    assert result.extraction_method == "pdftotext+field"
    assert not result.needs_review


def test_extract_crm_pdf_uses_ocr_for_image_only_pdf(tmp_path: Path, monkeypatch) -> None:
    pdf = tmp_path / "image.pdf"
    pdf.write_bytes(b"%PDF-1.4\ncontent\n%%EOF\n")
    monkeypatch.setattr(pdf_extract, "extract_pdf_text", lambda _path: "")
    monkeypatch.setattr(
        pdf_extract,
        "tesseract_ocr_first_page",
        lambda _path: "Complaint Number: 104-7654321\nPerson Name: Ali\nComplaint Remarks: A sufficiently clear OCR complaint description for comparison.",
    )

    result = pdf_extract.extract_crm_pdf(pdf)

    assert result.complaint_number == "104-7654321"
    assert result.extraction_method == "tesseract+field"
    assert result.ocr_text


def test_extract_crm_pdf_selectively_ocrs_blank_later_pages(tmp_path: Path, monkeypatch) -> None:
    pdf = tmp_path / "mixed.pdf"
    pdf.write_bytes(b"%PDF-1.4\ncontent\n%%EOF\n")
    first = "Complaint No: 104-6609317\nPerson Name: Fatima\nComplaint Remarks: " + ("detail " * 30)
    monkeypatch.setattr(pdf_extract, "extract_pdf_text", lambda _path: first + "\f\f")
    called: list[int] = []
    monkeypatch.setattr(
        pdf_extract,
        "tesseract_ocr_page",
        lambda _path, *, page_number: called.append(page_number) or "Attachment text",
    )
    result = pdf_extract.extract_crm_pdf(pdf)
    assert result.page_count == 2
    assert result.ocr_pages == (2,)
    assert called == [2]


def test_extract_crm_pdf_ocrs_sparse_selectable_overlay(tmp_path: Path, monkeypatch) -> None:
    pdf = tmp_path / "portal-scan.pdf"
    pdf.write_bytes(b"%PDF-1.4\ncontent\n%%EOF\n")
    monkeypatch.setattr(
        pdf_extract,
        "extract_pdf_text",
        lambda _path: "CHIEF MINISTER COMPLAINT CELL\nComplaint ID: 104-6032890\nGenerated on 6/29/2026\f",
    )
    monkeypatch.setattr(
        pdf_extract,
        "tesseract_ocr_first_page",
        lambda _path: "Name GHULAM YASEEN CNIC 3630209500245\nComplaint Details\nSalary deduction requires investigation.",
    )

    result = pdf_extract.extract_crm_pdf(pdf)

    assert result.ocr_pages == (1,)
    assert "GHULAM YASEEN" in result.ocr_text
    assert result.extraction_method == "pdftotext+selective-tesseract+field"


def test_extract_crm_pdf_preserves_remarks_continuation_across_ocr_pages(
    tmp_path: Path, monkeypatch
) -> None:
    pdf = tmp_path / "two-page-complaint.pdf"
    pdf.write_bytes(b"%PDF-1.4\ncontent\n%%EOF\n")
    selectable = (
        "CHIEF MINISTER COMPLAINT CELL\nComplaint ID: 104-6759557\n"
        "Generated on 7/7/2026 | Page 1\f"
        "CHIEF MINISTER COMPLAINT CELL\nComplaint ID: 104-6759557\n"
        "Generated on 7/7/2026 | Page 2\f"
    )
    monkeypatch.setattr(pdf_extract, "extract_pdf_text", lambda _path: selectable)
    monkeypatch.setattr(
        pdf_extract,
        "tesseract_ocr_first_page",
        lambda _path: (
            "Complaint Details\nThe school demanded unlawful fees under the guise of\n"
            "Generated on 7/7/2026 | Page 1\nComplaint label OCR: noise"
        ),
    )
    monkeypatch.setattr(
        pdf_extract,
        "tesseract_ocr_page",
        lambda _path, *, page_number: (
            "CHIEF MINISTER COMPLAINT CELL\nGovernment of the Punjab\n"
            "holiday charges and threatened expulsion.\n"
            "Generated on 7/7/2026 | Page 2\n"
            "Complaint label OCR: noise\ncontinued crop noise"
        ),
    )

    result = pdf_extract.extract_crm_pdf(pdf)

    assert result.ocr_pages == (1, 2)
    assert result.remarks_text == (
        "The school demanded unlawful fees under the guise of holiday charges "
        "and threatened expulsion."
    )
    assert "Generated on" not in result.remarks_text
    assert "Complaint label OCR" not in result.remarks_text
