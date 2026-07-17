from __future__ import annotations

from openpyxl import Workbook
import zipfile

from whatsapp_gateway.inbound.processing_classifier import (
    classify_extracted_document,
    extract_document,
    normalize_complaint_numbers,
)


def test_crm_complaint_number_variants_normalize_to_canonical_value() -> None:
    text = " ".join(
        [
            "104-6609317",
            "104 6609317",
            "104_6609317",
            "104–6609317",
            "Complaint No. 104 / 6609317",
            "1046609317",
        ]
    )
    assert normalize_complaint_numbers(text) == ["104-6609317"]


def test_confirmed_crm_form_is_prioritized() -> None:
    result = classify_extracted_document(
        filename="Info Lahore Complaint_104-6609317.pdf",
        mime_type="application/pdf",
        extracted_text=(
            "CHIEF MINISTER COMPLAINT CELL\n"
            "Complaint No: 104-6609317\n"
            "Applicant Details\nComplaint Remarks"
        ),
        extraction_method="pdftotext",
    )
    assert result.category == "crm_complaint"
    assert result.complaint_number == "104-6609317"
    assert result.confidence >= 0.8
    assert result.needs_review is False


def test_reply_with_complaint_number_is_not_mislabeled_as_main_complaint() -> None:
    result = classify_extracted_document(
        filename="Action Taken Report 104-6609317.pdf",
        mime_type="application/pdf",
        extracted_text="Action Taken Report against Complaint No. 104-6609317",
        extraction_method="pdftotext",
    )
    assert result.category == "crm_reply_or_report"
    assert result.complaint_number == "104-6609317"


def test_multiple_complaint_numbers_require_review() -> None:
    result = classify_extracted_document(
        filename="combined.pdf",
        mime_type="application/pdf",
        extracted_text="Complaint 104-6609317 and Complaint 104-6609318",
        extraction_method="pdftotext",
    )
    assert result.category == "possible_crm_complaint"
    assert result.complaint_number is None
    assert result.needs_review is True
    assert result.all_complaint_numbers == ["104-6609317", "104-6609318"]


def test_spreadsheet_text_is_inspected_for_crm_numbers(tmp_path) -> None:
    path = tmp_path / "complaints.xlsx"
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["Complaint No", "Status"])
    sheet.append(["104-6609317", "Pending"])
    workbook.save(path)

    extraction = extract_document(path)
    classification = classify_extracted_document(
        filename=path.name,
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        extracted_text=extraction.text,
        extraction_method=extraction.method,
    )
    assert extraction.method == "spreadsheet:xlsx"
    assert classification.complaint_number == "104-6609317"
    assert classification.category == "crm_supporting_document"


def test_ods_text_is_inspected_for_crm_numbers(tmp_path) -> None:
    path = tmp_path / "complaints.ods"
    content = """<?xml version='1.0' encoding='UTF-8'?>
<office:document-content
 xmlns:office='urn:oasis:names:tc:opendocument:xmlns:office:1.0'
 xmlns:table='urn:oasis:names:tc:opendocument:xmlns:table:1.0'
 xmlns:text='urn:oasis:names:tc:opendocument:xmlns:text:1.0'>
 <office:body><office:spreadsheet><table:table table:name='Sheet1'>
  <table:table-row><table:table-cell><text:p>Complaint No</text:p></table:table-cell></table:table-row>
  <table:table-row><table:table-cell><text:p>104-6609317</text:p></table:table-cell></table:table-row>
 </table:table></office:spreadsheet></office:body>
</office:document-content>"""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("content.xml", content)
    extraction = extract_document(path)
    classification = classify_extracted_document(
        filename=path.name,
        mime_type="application/vnd.oasis.opendocument.spreadsheet",
        extracted_text=extraction.text,
        extraction_method=extraction.method,
    )
    assert extraction.method == "spreadsheet:ods"
    assert classification.complaint_number == "104-6609317"
    assert classification.category == "crm_supporting_document"
