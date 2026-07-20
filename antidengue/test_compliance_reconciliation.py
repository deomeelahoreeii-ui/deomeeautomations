from __future__ import annotations

from pathlib import Path

import pandas as pd

from compliance_reconciliation import reconcile_activities


def _frame(rows: list[tuple[int, str, str, str, int]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=["ID", "School EMIS", "Tag", "Task", "Seconds"],
    )


def _reconcile(frame: pd.DataFrame, wrong_ids: set[int], valid_ids: set[int]):
    return reconcile_activities(
        frame,
        issue_mask=frame["ID"].isin(wrong_ids),
        valid_correction_mask=frame["ID"].isin(valid_ids),
        school_emis=frame["School EMIS"],
        stream="test_stream",
        match_column_groups=("Tag", "Task"),
        timestamp_candidates=(),
    )


def test_one_later_valid_activity_closes_one_wrong_activity() -> None:
    frame = _frame([
        (1, "35210001", "simple", "school", 120),
        (2, "35210001", "simple", "school", 600),
    ])

    result = _reconcile(frame, {1}, {2})

    assert result["historical_issue_count"] == 1
    assert result["corrected_issue_count"] == 1
    assert result["open_issue_count"] == 0
    assert result["corrected_issues"].iloc[0]["Correction Activity ID"] == "2"
    assert result["corrective_activities"].iloc[0]["Corrects Activity ID"] == "1"


def test_ten_wrong_and_seven_later_valid_leave_three_open_fifo() -> None:
    rows = [
        (item, "35210001", "simple", "school", 120)
        for item in range(1, 11)
    ] + [
        (item, "35210001", "simple", "school", 600)
        for item in range(11, 18)
    ]
    frame = _frame(rows)

    result = _reconcile(frame, set(range(1, 11)), set(range(11, 18)))

    assert result["historical_issue_count"] == 10
    assert result["corrected_issue_count"] == 7
    assert result["open_issue_count"] == 3
    assert result["open_issues"]["ID"].tolist() == [8, 9, 10]
    assert result["corrected_issues"]["ID"].tolist() == list(range(1, 8))


def test_earlier_valid_activity_does_not_prepay_later_wrong_activity() -> None:
    frame = _frame([
        (1, "35210001", "simple", "school", 600),
        (2, "35210001", "simple", "school", 120),
    ])

    result = _reconcile(frame, {2}, {1})

    assert result["corrected_issue_count"] == 0
    assert result["open_issue_count"] == 1
    assert result["open_issues"]["ID"].tolist() == [2]


def test_different_school_or_task_never_borrows_correction_credit() -> None:
    frame = _frame([
        (1, "35210001", "simple", "school-a", 120),
        (2, "35210002", "simple", "school-a", 600),
        (3, "35210001", "simple", "school-b", 600),
    ])

    result = _reconcile(frame, {1}, {2, 3})

    assert result["open_issue_count"] == 1
    assert result["corrected_issue_count"] == 0
    assert result["valid_unused_count"] == 2


def test_same_timestamp_is_not_proven_later() -> None:
    frame = pd.DataFrame([
        {"ID": 1, "School EMIS": "35210001", "Tag": "x", "Task": "y", "When": "2026-07-20 09:00"},
        {"ID": 2, "School EMIS": "35210001", "Tag": "x", "Task": "y", "When": "2026-07-20 09:00"},
    ])

    result = reconcile_activities(
        frame,
        issue_mask=frame["ID"].eq(1),
        valid_correction_mask=frame["ID"].eq(2),
        school_emis=frame["School EMIS"],
        stream="test_stream",
        match_column_groups=("Tag", "Task"),
        timestamp_candidates=("When",),
    )

    assert result["open_issue_count"] == 1
    assert result["corrected_issue_count"] == 0


def test_different_reporting_days_do_not_cross_correct() -> None:
    frame = pd.DataFrame([
        {"ID": 1, "School EMIS": "35210001", "Tag": "x", "Task": "y", "When": "2026-07-19 09:00"},
        {"ID": 2, "School EMIS": "35210001", "Tag": "x", "Task": "y", "When": "2026-07-20 09:00"},
    ])

    result = reconcile_activities(
        frame,
        issue_mask=frame["ID"].eq(1),
        valid_correction_mask=frame["ID"].eq(2),
        school_emis=frame["School EMIS"],
        stream="test_stream",
        match_column_groups=("Tag", "Task"),
        timestamp_candidates=("When",),
    )

    assert result["open_issue_count"] == 1
    assert result["corrected_issue_count"] == 0
