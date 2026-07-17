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
from crm_domain.models import ComplaintCase, ComplaintFieldObservation, ComplaintMatch
from crm_domain.registry import promote_case_observations, record_paperless_match


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
