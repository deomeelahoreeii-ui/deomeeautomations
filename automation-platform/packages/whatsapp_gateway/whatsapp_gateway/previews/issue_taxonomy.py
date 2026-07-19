from __future__ import annotations

from typing import Any, Iterable

ACTIONABLE_SEVERITIES = frozenset({"blocked", "warning"})


def is_actionable_issue(item: dict[str, Any]) -> bool:
    return str(item.get("severity") or "").lower() in ACTIONABLE_SEVERITIES


def partition_issues(
    rows: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    actionable: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []
    for item in rows:
        (actionable if is_actionable_issue(item) else diagnostics).append(item)
    return actionable, diagnostics


def filter_validation_issues(
    rows: Iterable[dict[str, Any]], severity: str = ""
) -> list[dict[str, Any]]:
    normalized = severity.strip().lower()
    if normalized:
        return [
            item
            for item in rows
            if str(item.get("severity") or "").lower() == normalized
        ]
    return [item for item in rows if is_actionable_issue(item)]


__all__ = [
    "ACTIONABLE_SEVERITIES",
    "filter_validation_issues",
    "is_actionable_issue",
    "partition_issues",
]
