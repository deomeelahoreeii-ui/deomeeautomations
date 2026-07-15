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
