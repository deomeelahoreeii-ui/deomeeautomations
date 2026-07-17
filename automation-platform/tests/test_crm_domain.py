from __future__ import annotations

import uuid

from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, Session, create_engine, select

import automation_core.models  # noqa: F401
import master_data.models  # noqa: F401
import whatsapp_gateway.models  # noqa: F401

from crm_domain.fields import (
    extract_field_observations,
    extract_mapping_observations,
    paperless_custom_field_values,
)
from crm_domain.identifiers import complaint_numbers_in, normalize_complaint_number
from crm_domain.models import (
    ComplaintCase,
    ComplaintDocumentCaseLink,
    ComplaintFieldObservation,
    ComplaintMatch,
)
from crm_domain.registry import (
    CapturedComplaintDocument,
    promote_case_observations,
    record_captured_document,
    record_paperless_match,
)


CRM_TEXT = """Complaint No        104-6609317
Person Name: Muhammad Wasim
Cnic No | 3840332672875
Mobile No  3038366436
Person Address: Shezada.mehrban@gmail.com
Complaint District: LAHORE
Category: Others
Sub Category: Others
Complaint Remarks: The school matter requires investigation.
"""


def test_crm_number_has_one_canonical_native_identity() -> None:
    assert normalize_complaint_number("Complaint No: 104-6609317") == "104-6609317"
    assert normalize_complaint_number("Complaint No 104 6609317") == "104-6609317"
    assert normalize_complaint_number("104_6609317") == "104-6609317"
    assert normalize_complaint_number("104-6609317 and 104-6609318") is None
    assert normalize_complaint_number("CNIC 3840332672875") is None


def test_all_distinct_crm_numbers_are_retained_for_review() -> None:
    assert complaint_numbers_in("104-6609317, 104 6609318, 104-6609317") == [
        "104-6609317",
        "104-6609318",
    ]


def test_labeled_crm_fields_become_provenanced_observations() -> None:
    observations = extract_field_observations(CRM_TEXT)
    values = {item.field_name: item.normalized_value for item in observations}
    assert values == {
        "complaint_number": "104-6609317",
        "complainant_name": "Muhammad Wasim",
        "complainant_cnic": "3840332672875",
        "complainant_mobile": "3038366436",
        "complainant_address": "Shezada.mehrban@gmail.com",
        "district": "LAHORE",
        "category": "Others",
        "sub_category": "Others",
        "remarks": "The school matter requires investigation.",
    }
    assert all(item.source_locator.startswith("line:") for item in observations)


def test_ocr_table_rows_promote_multiple_fields_and_multiline_remarks() -> None:
    text = """Name GHULAM YASEEN CNIC 3630209500245
Contact 03216833965 District LODHRAN
Address GAO RIND JADA Tehsil KEROR PAKKA
Department School Education Category Others
Sub Category Others District LODHRAN
Complaint Details
The complainant disputes a salary deduction.
The matter requires investigation.
Generated on 6/29/2026
"""

    values = {
        item.field_name: item.normalized_value
        for item in extract_field_observations(text)
    }

    assert values["complainant_name"] == "GHULAM YASEEN"
    assert values["complainant_cnic"] == "3630209500245"
    assert values["complainant_mobile"] == "03216833965"
    assert values["district"] == "LODHRAN"
    assert values["tehsil"] == "KEROR PAKKA"
    assert values["department"] == "School Education"
    assert values["category"] == "Others"
    assert values["sub_category"] == "Others"
    assert values["remarks"] == (
        "The complainant disputes a salary deduction. "
        "The matter requires investigation."
    )


def test_ocr_remarks_continue_across_pages_without_parsing_prose_as_fields() -> None:
    text = """Complaint Details
The first page explains the unlawful fee demand under the guise of
Generated on 7/7/2026 | Page 1
Complaint label OCR: unreadable crop
unreadable crop continuation
[OCR page 2]
CHIEF MINISTER COMPLAINT CELL
Government of the Punjab
holiday fees and threats of expulsion.
The District Education Authority Lahore should investigate.
Generated on 7/7/2026 | Page 2
Complaint label OCR: second unreadable crop
second crop continuation
"""

    observations = extract_field_observations(text)
    values = {item.field_name: item.normalized_value for item in observations}

    assert values["remarks"] == (
        "The first page explains the unlawful fee demand under the guise of "
        "holiday fees and threats of expulsion. "
        "The District Education Authority Lahore should investigate."
    )
    assert "district" not in values


def test_paperless_main_complaint_contract_uses_stable_field_names() -> None:
    fields = paperless_custom_field_values(
        {
            "complaint_number": "104-6609317",
            "source": "CRM Portal",
            "status": "Pending",
            "complainant_name": "Muhammad Wasim",
            "complainant_cnic": "3840332672875",
        },
        document_role="Main Complaint",
    )
    assert fields == {
        "Complaint Number": "104-6609317",
        "Source": "CRM Portal",
        "Status": "Pending",
        "Complainant Name": "Muhammad Wasim",
        "Complainant CNIC": "3840332672875",
        "Document Role": "Main Complaint",
    }


def test_paperless_attachment_links_parent_and_cannot_have_case_status() -> None:
    fields = paperless_custom_field_values(
        {"complaint_number": "104-6609317", "status": "Pending"},
        document_role="Complaint Details",
        parent_document_id=4821,
    )
    assert fields["Complaint Number"] == "104-6609317"
    assert fields["Document Role"] == "Complaint Details"
    assert fields["Parent Case"] == 4821
    assert "Status" not in fields


def test_spreadsheet_mapping_preserves_portal_specific_observations() -> None:
    observations = extract_mapping_observations(
        {
            "Complaint No": "104-6609317",
            "Complaint_Status": "Overdue",
            "Escalation Level": "Exceeded",
            "Created Date": "2026-05-20",
            "Caller District": "SARGODHA",
        },
        source_locator="sheet:Sheet1:row:42",
    )
    assert {item.field_name: item.normalized_value for item in observations} == {
        "complaint_number": "104-6609317",
        "portal_status": "Overdue",
        "escalation_level": "Exceeded",
        "portal_created_date": "2026-05-20",
        "caller_district": "SARGODHA",
    }


def test_fresh_paperless_result_still_requires_explicit_human_approval() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        case = ComplaintCase(complaint_number="104-6609317")
        session.add(case)
        session.flush()
        match = record_paperless_match(
            session,
            complaint_case=case,
            processing_item_id=uuid.uuid4(),
            category="fresh",
            reason="No exact Paperless complaint exists.",
            document_ids=[],
            statuses=[],
        )
        session.commit()
        session.refresh(case)
        persisted = session.exec(select(ComplaintMatch).where(ComplaintMatch.id == match.id)).one()
        assert case.state == "review_required"
        assert persisted.proposed_decision == "fresh"
        assert persisted.final_decision is None


def test_human_approval_promotes_observed_fields_without_overwriting_reviewed_values() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        case = ComplaintCase(
            complaint_number="104-6609317",
            complainant_name="Operator corrected name",
        )
        session.add(case)
        session.flush()
        extraction_id = uuid.uuid4()
        session.add_all(
            [
                ComplaintFieldObservation(
                    complaint_case_id=case.id,
                    extraction_id=extraction_id,
                    field_name="complainant_name",
                    raw_value="OCR NAME",
                    normalized_value="OCR NAME",
                    confidence=0.85,
                ),
                ComplaintFieldObservation(
                    complaint_case_id=case.id,
                    extraction_id=extraction_id,
                    field_name="district",
                    raw_value="LODHRAN",
                    normalized_value="LODHRAN",
                    confidence=0.85,
                ),
            ]
        )
        session.flush()

        promote_case_observations(session, case)

        assert case.complainant_name == "Operator corrected name"
        assert case.district == "LODHRAN"


def test_human_approval_can_complete_an_exact_truncated_remarks_prefix() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        prefix = "The school demanded unlawful fees under the guise of"
        complete = prefix + " holiday charges and threatened expulsion."
        case = ComplaintCase(complaint_number="104-6609317", remarks=prefix)
        session.add(case)
        session.flush()
        session.add(
            ComplaintFieldObservation(
                complaint_case_id=case.id,
                extraction_id=uuid.uuid4(),
                field_name="remarks",
                raw_value=complete,
                normalized_value=complete,
                confidence=0.95,
            )
        )
        session.flush()

        promote_case_observations(session, case)

        assert case.remarks == complete


def test_pdf_extractor_structured_remarks_are_retained_as_provenance() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        prefix = "Complaint Details\nThe first page ends under the guise of"
        complete = (
            "The first page ends under the guise of holiday fees and the second "
            "page requests an investigation."
        )
        _document, case, extraction = record_captured_document(
            session,
            CapturedComplaintDocument(
                processing_item_id=uuid.uuid4(),
                attachment_id=None,
                message_id=None,
                source_sha256="a" * 64,
                original_filename="104-6609317.pdf",
                mime_type="application/pdf",
                category="crm_complaint",
                complaint_number="104-6609317",
                classification_confidence=0.95,
                classification_evidence=["complaint_number_in_ocr_text"],
                extraction_method="pdftotext+selective-tesseract",
                extracted_text=prefix,
                extraction_metadata={"remarks_text": complete},
            ),
        )
        session.flush()

        structured = session.exec(
            select(ComplaintFieldObservation).where(
                ComplaintFieldObservation.complaint_case_id == case.id,
                ComplaintFieldObservation.extraction_id == extraction.id,
                ComplaintFieldObservation.source_locator
                == "metadata:pdf-extractor:remarks_text",
            )
        ).one()

        assert structured.normalized_value == complete
        assert structured.confidence == 0.95


def test_identical_binary_capture_is_a_duplicate_audit_link_not_active_evidence() -> None:
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        def capture(processing_item_id: uuid.UUID) -> CapturedComplaintDocument:
            return CapturedComplaintDocument(
                processing_item_id=processing_item_id,
                attachment_id=None,
                message_id=None,
                source_sha256="b" * 64,
                original_filename="104-6795237.pdf",
                mime_type="application/pdf",
                category="crm_complaint",
                complaint_number="104-6795237",
                classification_confidence=1.0,
                classification_evidence=["complaint_number_in_ocr_text"],
                extraction_method="pdftotext+selective-tesseract",
                extracted_text="Complaint Details\nSame complaint.",
                extraction_metadata={"remarks_text": "Same complaint."},
            )

        canonical, case, _extraction = record_captured_document(
            session, capture(uuid.uuid4())
        )
        duplicate, duplicate_case, _duplicate_extraction = record_captured_document(
            session, capture(uuid.uuid4())
        )
        session.flush()
        duplicate_link = session.exec(
            select(ComplaintDocumentCaseLink).where(
                ComplaintDocumentCaseLink.complaint_document_id == duplicate.id
            )
        ).one()
        match = session.exec(
            select(ComplaintMatch).where(
                ComplaintMatch.processing_item_id
                == duplicate.source_processing_item_id,
                ComplaintMatch.proposed_decision == "exact_file_duplicate",
            )
        ).one()

        assert duplicate_case.id == case.id
        assert canonical.review_state == "proposed"
        assert duplicate.review_state == "duplicate"
        assert duplicate_link.review_state == "duplicate"
        assert match.signals_json["matched_document_id"] == str(canonical.id)

        # Approval can revisit captures in any order. Processing the later copy
        # first must never cause the earlier canonical file to become a duplicate.
        record_captured_document(
            session, capture(duplicate.source_processing_item_id)
        )
        revisited_canonical, _, _ = record_captured_document(
            session, capture(canonical.source_processing_item_id)
        )
        session.flush()
        assert revisited_canonical.review_state == "proposed"
        assert duplicate.review_state == "duplicate"
