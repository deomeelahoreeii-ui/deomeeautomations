from __future__ import annotations

from typing import Any

from whatsapp_gateway.rendering.antidengue.issues import _issue


def evidence_metadata(
    *,
    message_mode: str,
    message_summary_complete: bool,
    complete_evidence_required: bool,
    complete_evidence_available: bool,
    complete_evidence_source: str,
    summarized_school_count: int = 0,
    summarized_activity_count: int = 0,
) -> dict[str, Any]:
    return {
        "message_mode": message_mode,
        "message_summary_complete": message_summary_complete,
        "complete_evidence_required": complete_evidence_required,
        "complete_evidence_available": complete_evidence_available,
        "complete_evidence_source": complete_evidence_source,
        "summarized_school_count": summarized_school_count,
        "summarized_activity_count": summarized_activity_count,
    }


def validate_evidence_coverage(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if not metadata.get("message_summary_complete"):
        issues.append(
            _issue(
                "missing_required_message_summary",
                "blocked",
                "The WhatsApp payload is missing its required scope summary.",
                category="evidence",
            )
        )
    if metadata.get("complete_evidence_required") and not metadata.get(
        "complete_evidence_available"
    ):
        issues.append(
            _issue(
                "missing_complete_supporting_evidence",
                "blocked",
                "The message is summarized, but no complete supporting evidence is available in the final payload.",
                category="evidence",
                summarized_school_count=int(
                    metadata.get("summarized_school_count") or 0
                ),
                summarized_activity_count=int(
                    metadata.get("summarized_activity_count") or 0
                ),
            )
        )
    return issues


__all__ = ["evidence_metadata", "validate_evidence_coverage"]
