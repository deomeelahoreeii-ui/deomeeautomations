from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from whatsapp_gateway.models import (
    WhatsAppDispatchPreview,
    WhatsAppDispatchPreviewArtifact,
    WhatsAppDispatchPreviewDelivery,
)


@dataclass(frozen=True)
class PreviewStateSummary:
    ready_delivery_count: int
    warning_delivery_count: int
    blocked_delivery_count: int
    skipped_delivery_count: int
    warning_issue_count: int
    blocked_issue_count: int
    batch_warning_count: int
    batch_blocked_count: int
    artifact_blocked_count: int
    delivery_count: int
    eligible_delivery_count: int
    status: str

    @property
    def excluded_delivery_count(self) -> int:
        return self.blocked_delivery_count + self.skipped_delivery_count

    @property
    def partial_approval_available(self) -> bool:
        return (
            self.status == "ready"
            and self.eligible_delivery_count > 0
            and self.blocked_delivery_count > 0
        )


def summarize_preview_state(
    deliveries: Iterable[WhatsAppDispatchPreviewDelivery],
    batch_issues: Iterable[dict[str, Any]],
    artifacts: Iterable[WhatsAppDispatchPreviewArtifact] = (),
) -> PreviewStateSummary:
    delivery_rows = list(deliveries)
    batch_rows = list(batch_issues)
    artifact_rows = list(artifacts)

    ready_delivery_count = sum(item.status == "ready" for item in delivery_rows)
    warning_delivery_count = sum(item.status == "warning" for item in delivery_rows)
    blocked_delivery_count = sum(item.status == "blocked" for item in delivery_rows)
    skipped_delivery_count = sum(item.status == "skipped" for item in delivery_rows)

    delivery_warning_issues = sum(
        issue.get("severity") == "warning"
        for delivery in delivery_rows
        for issue in list(delivery.issues or [])
    )
    delivery_blocked_issues = sum(
        issue.get("severity") == "blocked"
        for delivery in delivery_rows
        for issue in list(delivery.issues or [])
    )
    artifact_warning_issues = sum(
        issue.get("severity") == "warning"
        for artifact in artifact_rows
        for issue in list(artifact.issues or [])
    )
    artifact_blocked_issues = sum(
        issue.get("severity") == "blocked"
        for artifact in artifact_rows
        for issue in list(artifact.issues or [])
    )
    batch_warning_count = sum(issue.get("severity") == "warning" for issue in batch_rows)
    batch_blocked_count = sum(issue.get("severity") == "blocked" for issue in batch_rows)

    active_attachment_ids = {
        artifact_id
        for delivery in delivery_rows
        if delivery.status in {"ready", "warning"}
        for artifact_id in list(delivery.attachment_ids or [])
    }
    artifact_blocked_count = sum(
        artifact.status == "blocked" and str(artifact.id) in active_attachment_ids
        for artifact in artifact_rows
    )

    eligible_delivery_count = ready_delivery_count + warning_delivery_count
    if batch_blocked_count or artifact_blocked_count:
        status = "blocked"
    elif eligible_delivery_count:
        # Delivery-scoped blockers are exclusions, not a batch-wide veto.
        status = "ready"
    elif blocked_delivery_count:
        status = "blocked"
    else:
        # Empty/all-skipped previews preserve the existing no-op approval path.
        status = "ready"

    return PreviewStateSummary(
        ready_delivery_count=ready_delivery_count,
        warning_delivery_count=warning_delivery_count,
        blocked_delivery_count=blocked_delivery_count,
        skipped_delivery_count=skipped_delivery_count,
        warning_issue_count=(
            delivery_warning_issues + artifact_warning_issues + batch_warning_count
        ),
        blocked_issue_count=(
            delivery_blocked_issues + artifact_blocked_issues + batch_blocked_count
        ),
        batch_warning_count=batch_warning_count,
        batch_blocked_count=batch_blocked_count,
        artifact_blocked_count=artifact_blocked_count,
        delivery_count=len(delivery_rows),
        eligible_delivery_count=eligible_delivery_count,
        status=status,
    )


def apply_preview_state(
    preview: WhatsAppDispatchPreview,
    summary: PreviewStateSummary,
) -> None:
    """Persist delivery-state counts; issue counts remain derived API metadata."""

    preview.ready_count = summary.ready_delivery_count
    preview.warning_count = summary.warning_delivery_count
    preview.blocked_count = summary.blocked_delivery_count
    preview.skipped_count = summary.skipped_delivery_count
    preview.delivery_count = summary.delivery_count
    preview.status = summary.status


__all__ = [
    "PreviewStateSummary",
    "apply_preview_state",
    "summarize_preview_state",
]
