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
