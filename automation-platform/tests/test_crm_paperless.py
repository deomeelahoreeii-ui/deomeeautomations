from __future__ import annotations

from crm_filters.paperless import (
    PaperlessMetadata,
    categorize_matching_documents,
    title_has_exact_complaint,
)


def metadata() -> PaperlessMetadata:
    return PaperlessMetadata(
        complaint_type_id=7,
        complaint_number_field_id=11,
        source_field_id=12,
        document_role_field_id=13,
        status_field_id=14,
        status_option_ids_by_label={"submitted": 100, "not relevant": 101},
        status_option_labels_by_id={"100": "Submitted", "101": "Not Relevant", "102": "Pending"},
        option_labels_by_field_id={
            "12": {"20": "CRM Portal"},
            "13": {"30": "Main Complaint"},
            "14": {"100": "Submitted", "101": "Not Relevant", "102": "Pending"},
        },
    )


def document(document_id: int, complaint: str, status: int) -> dict:
    return {
        "id": document_id,
        "title": f"Complaint {complaint}",
        "document_type": 7,
        "custom_fields": [
            {"field": 11, "value": complaint},
            {"field": 12, "value": 20},
            {"field": 13, "value": 30},
            {"field": 14, "value": status},
        ],
    }


def test_paperless_classifies_submitted_and_pending_matches() -> None:
    submitted = categorize_matching_documents([document(1, "104-1001", 100)], "104-1001", metadata())
    pending = categorize_matching_documents([document(2, "104-1002", 102)], "104-1002", metadata())

    assert submitted.category == "submitted"
    assert submitted.matched_statuses == ["Submitted"]
    assert pending.category == "uploaded_pending"


def test_paperless_conflicting_terminal_statuses_require_manual_review() -> None:
    result = categorize_matching_documents(
        [document(1, "104-1001", 100), document(2, "104-1001", 101)],
        "104-1001",
        metadata(),
    )

    assert result.category == "manual_review"
    assert "Conflicting" in result.reason


def test_title_matching_does_not_match_longer_number() -> None:
    assert title_has_exact_complaint("Complaint 104-1234 received", "104-1234")
    assert not title_has_exact_complaint("Complaint 104-12345 received", "104-1234")


def test_publication_maps_paperless_select_labels_to_option_ids() -> None:
    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client.metadata = metadata()
    client.metadata.field_ids_by_name = {
        "complaint number": 11,
        "source": 12,
        "document role": 13,
        "status": 14,
    }
    assert client.custom_field_payload(
        {
            "Complaint Number": "104-6609317",
            "Source": "CRM Portal",
            "Document Role": "Main Complaint",
            "Status": "Pending",
        }
    ) == [
        {"field": 11, "value": "104-6609317"},
        {"field": 12, "value": "20"},
        {"field": 13, "value": "30"},
        {"field": 14, "value": "102"},
    ]


def test_publication_omits_unknown_select_values_and_maps_department_alias() -> None:
    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client.metadata = metadata()
    client.metadata.field_ids_by_name.update({"department": 17, "tehsil": 16})
    client.metadata.field_data_types_by_id.update({"17": "select", "16": "select"})
    client.metadata.option_labels_by_field_id.update(
        {
            "17": {"school-option": "School Education Department"},
            "16": {"lahore-option": "Model Town"},
        }
    )

    assert client.custom_field_payload(
        {"Department": "School Education", "Tehsil": "KEROR PAKKA"}
    ) == [{"field": 17, "value": "school-option"}]


def test_publication_maps_parent_case_to_document_link_list() -> None:
    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client.metadata = metadata()
    client.metadata.field_ids_by_name["parent case"] = 9
    client.metadata.field_data_types_by_id["9"] = "documentlink"

    assert client.custom_field_payload({"Parent Case": 757}) == [
        {"field": 9, "value": [757]}
    ]


def test_publication_applies_the_legacy_crm_correspondent() -> None:
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client.metadata = metadata()
    client.metadata.correspondent_id = 1
    client.metadata.field_ids_by_name = {"status": 14}
    response = MagicMock()
    client._request = MagicMock(return_value=response)

    client.update_document_metadata(
        42,
        title="CRM - 104-6609317 - Main Complaint - v1",
        document_type_id=7,
        correspondent_id=client.metadata.correspondent_id,
        custom_fields={"Status": "Pending"},
    )

    payload = client._request.call_args.kwargs["json"]
    assert payload["correspondent"] == 1
    assert payload["custom_fields"] == [{"field": 14, "value": "102"}]


def test_internal_paperless_tls_failure_uses_configured_fallback() -> None:
    import requests
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    response = MagicMock()
    client = PaperlessClient(
        base_url="https://paperless.lab.internal",
        token="token",
        verify_ssl=True,
        allow_insecure_fallback=True,
    )
    client.session.request = MagicMock(
        side_effect=[requests.exceptions.SSLError("unknown issuer"), response]
    )

    actual = client._request("GET", "https://paperless.lab.internal/api/documents/")

    assert actual is response
    assert client.insecure_fallback_used is True
    first_call = client.session.request.call_args_list[0]
    second_call = client.session.request.call_args_list[1]
    assert first_call.kwargs["verify"] is True
    assert second_call.kwargs["verify"] is False


def test_external_paperless_tls_failure_is_not_bypassed() -> None:
    import pytest
    import requests
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(
        base_url="https://paperless.example.com",
        token="token",
        verify_ssl=True,
        allow_insecure_fallback=True,
    )
    client.session.request = MagicMock(side_effect=requests.exceptions.SSLError("unknown issuer"))

    with pytest.raises(requests.exceptions.SSLError):
        client._request("GET", "https://paperless.example.com/api/documents/")

    assert client.session.request.call_count == 1
    assert client.insecure_fallback_used is False


def test_sheet_lookup_index_fetches_documents_once_and_matches_locally() -> None:
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client.metadata = metadata()
    docs = [
        document(1, "104-1234567", 100),
        document(2, "104-7654321", 102),
    ]
    client._get_all = MagicMock(return_value=docs)

    logs: list[str] = []
    index = client.prepare_complaint_lookup_index(log=logs.append)

    assert index["104-1234567"].category == "submitted"
    assert client.lookup_complaint("104-7654321").category == "uploaded_pending"
    assert client.lookup_complaint("104-0000000").category == "fresh"
    assert client._get_all.call_count == 1
    assert any("index ready" in message.lower() for message in logs)


def test_targeted_sheet_index_skips_irrelevant_document_detail_requests() -> None:
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client.metadata = metadata()
    target_summary = {
        "id": 1,
        "title": "Complaint 104-1234567",
        "document_type": 7,
        "custom_fields": [],
    }
    irrelevant_summary = {
        "id": 2,
        "title": "Complaint 104-9999999",
        "document_type": 7,
        "custom_fields": [],
    }
    client._get_all = MagicMock(return_value=[target_summary, irrelevant_summary])
    client._get_document = MagicMock(return_value=document(1, "104-1234567", 100))

    index = client.prepare_complaint_lookup_index(
        complaint_numbers=["104-1234567"],
    )

    assert index["104-1234567"].category == "submitted"
    client._get_document.assert_called_once_with(1)


def test_targeted_index_marks_absent_numbers_fresh_without_per_number_searches() -> None:
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client.metadata = metadata()
    client._get_all = MagicMock(return_value=[document(1, "104-1234567", 100)])

    client.prepare_complaint_lookup_index(
        complaint_numbers=["104-0000000"],
    )

    assert client.lookup_complaint("104-0000000").category == "fresh"
    assert client._get_all.call_count == 1


def test_collection_reader_accepts_unpaginated_paperless_task_lists() -> None:
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    response = MagicMock()
    response.json.return_value = [
        {"task_id": "task-1", "status": "SUCCESS", "related_document": 42}
    ]
    client._request = MagicMock(return_value=response)

    assert client._get_all("/api/tasks/") == [
        {"task_id": "task-1", "status": "SUCCESS", "related_document": 42}
    ]
    assert client._request.call_count == 1


def test_publication_retry_finds_an_exact_previously_uploaded_title() -> None:
    from unittest.mock import MagicMock

    from crm_filters.paperless import PaperlessClient

    client = PaperlessClient(base_url="https://paperless.lab.internal", token="token")
    client._get_all = MagicMock(
        return_value=[
            {"id": 41, "title": "CRM - 104-1234567 - Main Complaint - v10 old"},
            {"id": 42, "title": "CRM - 104-1234567 - Main Complaint - v10"},
        ]
    )

    assert (
        client.find_document_by_exact_title(
            "CRM - 104-1234567 - Main Complaint - v10"
        )
        == 42
    )
