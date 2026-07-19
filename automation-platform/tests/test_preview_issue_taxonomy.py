from __future__ import annotations

from types import SimpleNamespace

from whatsapp_gateway.previews.issue_taxonomy import (
    filter_validation_issues,
    partition_issues,
)
from whatsapp_gateway.previews.serialization import _delivery_dict
from whatsapp_gateway.rendering.antidengue.evidence import (
    evidence_metadata,
    validate_evidence_coverage,
)


def _issues() -> list[dict[str, str]]:
    return [
        {"code": "technical_note", "severity": "info", "message": "Diagnostic"},
        {"code": "review_mobile", "severity": "warning", "message": "Review"},
        {"code": "missing_evidence", "severity": "blocked", "message": "Resolve"},
    ]


def test_validation_views_default_to_actionable_issues() -> None:
    rows = _issues()
    assert [item["severity"] for item in filter_validation_issues(rows)] == [
        "warning", "blocked"
    ]
    assert [item["severity"] for item in filter_validation_issues(rows, "info")] == [
        "info"
    ]
    actionable, diagnostics = partition_issues(rows)
    assert [item["severity"] for item in actionable] == ["warning", "blocked"]
    assert [item["severity"] for item in diagnostics] == ["info"]


def test_delivery_serialization_separates_validation_from_diagnostics() -> None:
    delivery = SimpleNamespace(
        id="delivery-id",
        preview_id="preview-id",
        sequence=1,
        source_route_key="route",
        target_type="contact",
        target_name="Recipient",
        target_jid="923001234567@s.whatsapp.net",
        wing_name="DEO MEE",
        route_kind="markaz",
        route_scope="MARKAZ ONE",
        message="Message",
        attachment_ids=[],
        routing_snapshot={"presentation_metadata": {"message_mode": "summary"}},
        issues=_issues(),
        status="blocked",
        idempotency_key="key",
        created_at=None,
    )

    serialized = _delivery_dict(None, delivery)
    assert [item["severity"] for item in serialized["issues"]] == [
        "warning", "blocked"
    ]
    assert [item["severity"] for item in serialized["diagnostics"]] == ["info"]


def test_evidence_contract_blocks_each_missing_requirement_independently() -> None:
    complete = evidence_metadata(
        message_mode="summary",
        message_summary_complete=True,
        complete_evidence_required=True,
        complete_evidence_available=True,
        complete_evidence_source="parent",
    )
    assert validate_evidence_coverage(complete) == []

    missing_summary = {**complete, "message_summary_complete": False}
    assert [item["code"] for item in validate_evidence_coverage(missing_summary)] == [
        "missing_required_message_summary"
    ]

    missing_evidence = {**complete, "complete_evidence_available": False}
    assert [item["code"] for item in validate_evidence_coverage(missing_evidence)] == [
        "missing_complete_supporting_evidence"
    ]

    missing_both = {
        **complete,
        "message_summary_complete": False,
        "complete_evidence_available": False,
    }
    assert [item["code"] for item in validate_evidence_coverage(missing_both)] == [
        "missing_required_message_summary",
        "missing_complete_supporting_evidence",
    ]
