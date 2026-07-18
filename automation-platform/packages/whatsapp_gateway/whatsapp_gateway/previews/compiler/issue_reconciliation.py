from __future__ import annotations

from whatsapp_gateway.models import WhatsAppDispatchPreviewDelivery

ZERO_RESULT_ACKNOWLEDGEMENT = "zero_result_acknowledgement"


def _count_phrase(count: int, singular: str, plural: str) -> str:
    return f"{count} {singular if count == 1 else plural}"


def reconcile_zero_result_issues(
    batch_issues: list[dict],
    deliveries: list[WhatsAppDispatchPreviewDelivery],
) -> list[dict]:
    """Replace an empty-report warning when acknowledgement routes cover it."""
    acknowledgements = [
        delivery
        for delivery in deliveries
        if (delivery.routing_snapshot or {}).get("delivery_kind")
        == ZERO_RESULT_ACKNOWLEDGEMENT
    ]
    has_empty_report = any(
        item.get("code") == "nothing_to_dispatch" for item in batch_issues
    )
    if not has_empty_report:
        return batch_issues

    # A multi-report plan can legitimately have no rows for one report while
    # another report has a complete, sendable delivery. The empty category is
    # useful review information, but it must not make the whole plan unclean or
    # prevent Send Now from dispatching the independent report.
    other_ready_deliveries = [
        delivery
        for delivery in deliveries
        if delivery.status in {"ready", "warning"}
        and (delivery.routing_snapshot or {}).get("delivery_kind")
        != ZERO_RESULT_ACKNOWLEDGEMENT
    ]
    if not acknowledgements and other_ready_deliveries:
        return [
            *(item for item in batch_issues if item.get("code") != "nothing_to_dispatch"),
            {
                "code": "empty_report_category_resolved",
                "severity": "info",
                "message": (
                    "No dormant MEE schools were found; the other selected "
                    "report category has a delivery ready to send."
                ),
                "ready_delivery_count": len(other_ready_deliveries),
            },
        ]

    if not acknowledgements:
        return batch_issues

    ready_count = sum(item.status in {"ready", "warning"} for item in acknowledgements)
    skipped_count = sum(item.status == "skipped" for item in acknowledgements)
    ready_phrase = _count_phrase(
        ready_count, "zero-result acknowledgement", "zero-result acknowledgements"
    )
    skipped_phrase = _count_phrase(
        skipped_count, "was already sent", "were already sent"
    )
    if ready_count and skipped_count:
        message = (
            f"No dormant MEE schools were found; {ready_phrase} "
            f"{'is' if ready_count == 1 else 'are'} ready to send and {skipped_phrase} today."
        )
    elif ready_count:
        message = (
            f"No dormant MEE schools were found; {ready_phrase} "
            f"{'is' if ready_count == 1 else 'are'} ready to send."
        )
    else:
        message = (
            "No dormant MEE schools were found; all acknowledgements "
            f"({skipped_count}) were already sent today."
        )

    return [
        *(item for item in batch_issues if item.get("code") != "nothing_to_dispatch"),
        {
            "code": "zero_result_acknowledgements_resolved",
            "severity": "info",
            "message": message,
            "ready_count": ready_count,
            "already_sent_count": skipped_count,
        },
    ]


__all__ = ["reconcile_zero_result_issues"]
