from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import os
import re
from typing import Iterable, Sequence

import pandas as pd

POLICY_VERSION = "one_for_one_fifo.v1"

_STATUS_OPEN = "Open issue"
_STATUS_CORRECTED = "Corrected issue"
_STATUS_CORRECTIVE = "Corrective activity"
_STATUS_UNUSED = "Valid unused activity"
_STATUS_IGNORED = "Ignored invalid measurement"


@dataclass(frozen=True)
class CorrectiveCompliancePolicy:
    """Frozen, deterministic one-for-one corrective-compliance policy."""

    enabled: bool = True
    correction_ratio: int = 1
    matching_mode: str = "fifo"
    correction_must_be_later: bool = True
    require_same_reporting_day: bool = True
    version: str = POLICY_VERSION

    def as_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "correction_ratio": self.correction_ratio,
            "matching_mode": self.matching_mode,
            "correction_must_be_later": self.correction_must_be_later,
            "require_same_reporting_day": self.require_same_reporting_day,
            "version": self.version,
        }


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def corrective_compliance_policy() -> CorrectiveCompliancePolicy:
    """Return the policy used for the current frozen run.

    These switches are deliberately narrow. A run summary/workbook records the
    resolved policy version, so the reconciliation remains auditable.
    """

    return CorrectiveCompliancePolicy(
        enabled=_env_bool("ANTIDENGUE_CORRECTIVE_COMPLIANCE_ENABLED", True),
        require_same_reporting_day=_env_bool(
            "ANTIDENGUE_CORRECTIVE_COMPLIANCE_SAME_DAY", True
        ),
    )


def _normalized_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).strip()).casefold()


def _first_present(frame: pd.DataFrame, names: Sequence[str]) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def activity_id_series(
    frame: pd.DataFrame,
    candidates: Sequence[str] = ("ID", "Activity ID", "Activity Id", "Id"),
) -> pd.Series:
    column = _first_present(frame, candidates)
    if column is None:
        return pd.Series(
            [f"row-{position + 1}" for position in range(len(frame))],
            index=frame.index,
            dtype="object",
        )
    result = frame[column].fillna("").astype(str).str.strip()
    fallback = pd.Series(
        [f"row-{position + 1}" for position in range(len(frame))],
        index=frame.index,
        dtype="object",
    )
    return result.where(result.ne(""), fallback)


def parse_activity_timestamps(
    frame: pd.DataFrame,
    candidates: Sequence[str] = (
        "Activity Date/Time",
        "Activity Date Time",
        "Activity Datetime",
        "Submitted At",
        "Submission Date/Time",
        "Date/Time",
        "Created At",
        "Created Date/Time",
    ),
) -> tuple[pd.Series, str | None]:
    column = _first_present(frame, candidates)
    if column is None:
        return pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]"), None

    cleaned = (
        frame[column]
        .fillna("")
        .astype(str)
        .str.replace(r"^\s*on\s+", "", regex=True, case=False)
        .str.replace(r"\s+at\s+", " ", regex=True, case=False)
        .str.strip()
    )
    try:
        parsed = pd.to_datetime(cleaned, errors="coerce", format="mixed")
    except TypeError:  # pandas < 2 compatibility
        parsed = pd.to_datetime(cleaned, errors="coerce")
    return parsed, column


def _resolved_match_columns(
    frame: pd.DataFrame,
    groups: Sequence[Sequence[str] | str],
) -> list[str]:
    resolved: list[str] = []
    for group in groups:
        aliases = (group,) if isinstance(group, str) else tuple(group)
        column = _first_present(frame, aliases)
        if column is not None and column not in resolved:
            resolved.append(column)
    return resolved


def _match_keys(
    frame: pd.DataFrame,
    school_emis: pd.Series,
    match_column_groups: Sequence[Sequence[str] | str],
) -> tuple[pd.Series, list[str]]:
    columns = _resolved_match_columns(frame, match_column_groups)
    keys: list[str] = []
    for position, index in enumerate(frame.index):
        emis = _normalized_text(school_emis.loc[index])
        # Unmapped rows are intentionally isolated. They must remain visible
        # for review but can never borrow credit from another unmapped row.
        if not emis:
            keys.append(f"unmapped:{position}")
            continue
        parts = [emis]
        parts.extend(_normalized_text(frame.at[index, column]) for column in columns)
        keys.append(" | ".join(parts))
    return pd.Series(keys, index=frame.index, dtype="object"), columns


def _strictly_later(
    current_index: object,
    issue_index: object,
    *,
    parsed_times: pd.Series,
    row_order: pd.Series,
) -> bool:
    current_time = parsed_times.loc[current_index]
    issue_time = parsed_times.loc[issue_index]
    if pd.notna(current_time) and pd.notna(issue_time):
        return bool(current_time > issue_time)
    if pd.isna(current_time) and pd.isna(issue_time):
        return int(row_order.loc[current_index]) > int(row_order.loc[issue_index])
    # Mixed timestamp evidence is not strong enough to prove a later activity.
    return False


def reconcile_activities(
    frame: pd.DataFrame,
    *,
    issue_mask: pd.Series,
    valid_correction_mask: pd.Series,
    school_emis: pd.Series,
    stream: str,
    match_column_groups: Sequence[Sequence[str] | str] = (),
    timestamp_candidates: Sequence[str] = (
        "Activity Date/Time",
        "Activity Date Time",
        "Activity Datetime",
        "Submitted At",
        "Submission Date/Time",
        "Date/Time",
        "Created At",
        "Created Date/Time",
    ),
    policy: CorrectiveCompliancePolicy | None = None,
) -> dict[str, object]:
    """Reconcile wrong and later-correct activities one-for-one using FIFO.

    A valid activity can close exactly one earlier open issue only when it has
    the same school, matching task key and reporting day. Earlier valid work
    never prepays a later mistake. Original rows are never deleted.
    """

    resolved_policy = policy or corrective_compliance_policy()
    work = frame.copy()
    issue = issue_mask.reindex(work.index, fill_value=False).fillna(False).astype(bool)
    valid = (
        valid_correction_mask.reindex(work.index, fill_value=False)
        .fillna(False)
        .astype(bool)
        & ~issue
    )
    emis = school_emis.reindex(work.index, fill_value="").fillna("").astype(str).str.strip()
    row_order = pd.Series(range(len(work)), index=work.index, dtype="int64")
    activity_ids = activity_id_series(work)
    parsed_times, time_column = parse_activity_timestamps(work, timestamp_candidates)
    match_keys, match_columns = _match_keys(work, emis, match_column_groups)

    if resolved_policy.require_same_reporting_day:
        window = parsed_times.dt.strftime("%Y-%m-%d")
        if time_column is None:
            window = pd.Series("input-batch", index=work.index, dtype="object")
        else:
            window = window.fillna("timestamp-unproved")
    else:
        window = pd.Series("reporting-window", index=work.index, dtype="object")

    statuses = pd.Series(_STATUS_IGNORED, index=work.index, dtype="object")
    statuses.loc[issue] = _STATUS_OPEN
    statuses.loc[valid] = _STATUS_UNUSED
    correction_activity_id = pd.Series("", index=work.index, dtype="object")
    correction_activity_time = pd.Series("", index=work.index, dtype="object")
    corrects_activity_id = pd.Series("", index=work.index, dtype="object")

    if resolved_policy.enabled:
        sortable = pd.DataFrame(
            {
                "match_key": match_keys,
                "window": window,
                "parsed_time": parsed_times,
                "row_order": row_order,
            },
            index=work.index,
        )
        # Timestamp-proved rows are ordered by actual time. Rows without any
        # timestamp use deterministic source order inside their own batch.
        sortable["sort_time_missing"] = sortable["parsed_time"].isna()
        ordered_indices = sortable.sort_values(
            ["match_key", "window", "sort_time_missing", "parsed_time", "row_order"],
            kind="stable",
            na_position="last",
        ).index

        grouped: dict[tuple[str, str], list[object]] = {}
        for index in ordered_indices:
            grouped.setdefault((match_keys.loc[index], str(window.loc[index])), []).append(index)

        for indices in grouped.values():
            open_issues: deque[object] = deque()
            for index in indices:
                if issue.loc[index]:
                    open_issues.append(index)
                    continue
                if not valid.loc[index] or not open_issues:
                    continue

                matched_issue: object | None = None
                for candidate in list(open_issues):
                    if not resolved_policy.correction_must_be_later or _strictly_later(
                        index,
                        candidate,
                        parsed_times=parsed_times,
                        row_order=row_order,
                    ):
                        matched_issue = candidate
                        break
                if matched_issue is None:
                    continue

                open_issues.remove(matched_issue)
                statuses.loc[matched_issue] = _STATUS_CORRECTED
                statuses.loc[index] = _STATUS_CORRECTIVE
                correction_activity_id.loc[matched_issue] = activity_ids.loc[index]
                raw_time = work.at[index, time_column] if time_column else ""
                correction_activity_time.loc[matched_issue] = (
                    "" if pd.isna(raw_time) else str(raw_time)
                )
                corrects_activity_id.loc[index] = activity_ids.loc[matched_issue]

    audit = work.copy()
    audit.insert(0, "Compliance Stream", stream)
    audit.insert(1, "Corrective Policy Version", resolved_policy.version)
    audit.insert(2, "Compliance Status", statuses)
    audit.insert(3, "Correction Activity ID", correction_activity_id)
    audit.insert(4, "Correction Activity Date/Time", correction_activity_time)
    audit.insert(5, "Corrects Activity ID", corrects_activity_id)
    audit.insert(6, "Reconciliation Match Key", match_keys)
    audit.insert(7, "Reconciliation Window", window)
    audit.insert(8, "Reconciliation Time Source", time_column or "source row order")

    open_issues_frame = audit.loc[statuses.eq(_STATUS_OPEN)].copy()
    corrected_issues_frame = audit.loc[statuses.eq(_STATUS_CORRECTED)].copy()
    corrective_activities_frame = audit.loc[statuses.eq(_STATUS_CORRECTIVE)].copy()
    valid_unused_frame = audit.loc[statuses.eq(_STATUS_UNUSED)].copy()

    return {
        "policy": resolved_policy.as_dict(),
        "match_columns": match_columns,
        "timestamp_column": time_column,
        "historical_issue_count": int(issue.sum()),
        "corrected_issue_count": int(statuses.eq(_STATUS_CORRECTED).sum()),
        "open_issue_count": int(statuses.eq(_STATUS_OPEN).sum()),
        "corrective_activity_count": int(statuses.eq(_STATUS_CORRECTIVE).sum()),
        "valid_unused_count": int(statuses.eq(_STATUS_UNUSED).sum()),
        "ignored_invalid_count": int(statuses.eq(_STATUS_IGNORED).sum()),
        "open_issues": open_issues_frame,
        "corrected_issues": corrected_issues_frame,
        "corrective_activities": corrective_activities_frame,
        "valid_unused": valid_unused_frame,
        "audit": audit,
    }


def prepare_review_sheet(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "Review Classification" in result.columns:
        result["Review Classification"] = "Review required"
    else:
        insert_at = min(9, len(result.columns))
        result.insert(insert_at, "Review Classification", "Review required")
    return result


def workbook_frames(result: dict[str, object]) -> Iterable[tuple[str, pd.DataFrame]]:
    yield "Review Required", prepare_review_sheet(result["open_issues"])
    yield "Corrected Issues", result["corrected_issues"]
    yield "Valid Unused Activities", result["valid_unused"]
    yield "All Activity Audit", result["audit"]


__all__ = [
    "POLICY_VERSION",
    "CorrectiveCompliancePolicy",
    "activity_id_series",
    "corrective_compliance_policy",
    "parse_activity_timestamps",
    "prepare_review_sheet",
    "reconcile_activities",
    "workbook_frames",
]
