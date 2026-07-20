from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from compliance_reconciliation import reconcile_activities, workbook_frames

SCHEMA_VERSION = "simple_activity_timing.v1"
TIME_DIFFERENCE_COLUMN = "Time Difference(Sec)"
TIME_DIFFERENCE_ALIASES = (TIME_DIFFERENCE_COLUMN, "Picture Time Difference(Sec)")


def _time_difference_column(frame: pd.DataFrame) -> str:
    for name in TIME_DIFFERENCE_ALIASES:
        if name in frame.columns:
            return name
    raise ValueError(
        "Simple Activity List is missing a supported timing column: "
        + ", ".join(TIME_DIFFERENCE_ALIASES)
    )


def _read_table(path: Path) -> pd.DataFrame:
    tables = pd.read_html(path)
    if len(tables) != 1:
        raise ValueError(f"Expected one Simple Activity List table, found {len(tables)}")
    frame = tables[0]
    _time_difference_column(frame)
    return frame


def _snapshot() -> dict[str, Any]:
    path = os.getenv("ANTIDENGUE_RUNTIME_SNAPSHOT", "").strip()
    if not path:
        return {}
    try:
        snapshot = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read Simple Activity rules from runtime snapshot: {path}") from exc
    if snapshot.get("schema_version") != 1 or snapshot.get("source") != "automation-platform-postgresql":
        raise RuntimeError("Simple Activity rules require a trusted runtime snapshot")
    return snapshot


def _rules() -> list[dict[str, Any]]:
    return [
        item for item in (_snapshot().get("simple_activity_rules") or [])
        if isinstance(item, dict)
    ]


def _classification(
    frame: pd.DataFrame,
) -> tuple[pd.Series, pd.Series, list[dict[str, Any]], pd.Series]:
    seconds = pd.to_numeric(frame[_time_difference_column(frame)], errors="coerce")
    numeric = seconds.ge(0)
    issue = pd.Series(False, index=frame.index)
    results: list[dict[str, Any]] = []
    rules = _rules()
    if not rules:
        threshold = float(os.getenv("SIMPLE_ACTIVITY_MINIMUM_SECONDS", "300"))
        issue = (numeric & seconds.lt(threshold)).fillna(False)
        valid = (numeric & seconds.ge(threshold)).fillna(False)
        return issue, valid, [], seconds

    valid = numeric.copy()
    for rule in rules:
        threshold = float(rule.get("minimum_seconds") or 300)
        is_lte = rule.get("operator") == "lte"
        matched = seconds.le(threshold) if is_lte else seconds.lt(threshold)
        matched = (numeric & matched).fillna(False)
        issue |= matched
        valid &= seconds.gt(threshold) if is_lte else seconds.ge(threshold)
        results.append({
            "rule_id": rule.get("id"),
            "rule_key": rule.get("rule_key"),
            "rule_version": rule.get("version"),
            "rule_name": rule.get("name"),
            "classification": "review_required",
            "matching_activity_count": int(matched.sum()),
        })
    return issue.fillna(False), valid.fillna(False), results, seconds


def _candidate_mask(frame: pd.DataFrame) -> tuple[pd.Series, list[dict[str, Any]], pd.Series]:
    issue, _, results, seconds = _classification(frame)
    return issue, results, seconds


def _school_index() -> dict[str, dict[str, Any]]:
    return {
        str(item.get("School EMIS") or "").strip(): item
        for item in (_snapshot().get("master_schools") or [])
        if isinstance(item, dict) and str(item.get("School EMIS") or "").strip()
    }


def _submitted_column(frame: pd.DataFrame) -> str | None:
    for name in ("Submitted by", "Submitted By", "User Name", "Username"):
        if name in frame.columns:
            return name
    return None


def _emis_series(frame: pd.DataFrame) -> pd.Series:
    for name in ("School EMIS", "EMIS Code", "EMIS", "School Code"):
        if name in frame.columns:
            return (
                frame[name]
                .fillna("")
                .astype(str)
                .str.extract(r"(\d{5,})", expand=False)
                .fillna("")
            )
    submitted = _submitted_column(frame)
    if submitted:
        return (
            frame[submitted]
            .fillna("")
            .astype(str)
            .str.extract(r"^(\d{5,})", expand=False)
            .fillna("")
        )
    return pd.Series("", index=frame.index, dtype="object")


def _with_authoritative_routing(frame: pd.DataFrame, emis_values: pd.Series) -> pd.DataFrame:
    routed = frame.copy()
    source_time_column = _time_difference_column(routed)
    if source_time_column != TIME_DIFFERENCE_COLUMN:
        routed.insert(0, TIME_DIFFERENCE_COLUMN, routed[source_time_column])
    for authoritative in (
        "School EMIS",
        "School Name",
        "Tehsil",
        "Markaz",
        "Wing ID",
        "Tehsil ID",
        "Markaz ID",
        "Review Classification",
    ):
        if authoritative in routed.columns:
            routed = routed.drop(columns=[authoritative])
    routed.insert(0, "School EMIS", emis_values.reindex(routed.index).fillna("").astype(str))
    index = _school_index()

    def school_value(emis: object, key: str) -> str:
        return str(index.get(str(emis).strip(), {}).get(key) or "")

    routed.insert(1, "School Name", routed["School EMIS"].map(lambda value: school_value(value, "School Name")))
    routed.insert(2, "Tehsil", routed["School EMIS"].map(lambda value: school_value(value, "Tehsil")))
    routed.insert(3, "Markaz", routed["School EMIS"].map(lambda value: school_value(value, "Markaz")))
    routed.insert(4, "Wing ID", routed["School EMIS"].map(lambda value: school_value(value, "_wing_id")))
    routed.insert(5, "Tehsil ID", routed["School EMIS"].map(lambda value: school_value(value, "_tehsil_id")))
    routed.insert(6, "Markaz ID", routed["School EMIS"].map(lambda value: school_value(value, "_markaz_id")))
    return routed


def _reconcile(path: Path) -> tuple[dict[str, object], list[dict[str, Any]], pd.Series]:
    frame = _read_table(path)
    issue, valid, rule_results, seconds = _classification(frame)
    emis = _emis_series(frame)
    routed = _with_authoritative_routing(frame, emis)
    result = reconcile_activities(
        routed,
        issue_mask=issue,
        valid_correction_mask=valid,
        school_emis=routed["School EMIS"],
        stream="simple_activity_timing",
        match_column_groups=(
            ("Tag", "Activity Tag"),
            (
                "Activity Type",
                "Activity Name",
                "Name/Address",
                "Category",
                "Task",
                "Name",
                "Address",
            ),
        ),
    )
    return result, rule_results, seconds


def _format_workbook_sheet(sheet) -> None:
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions


def write_simple_activity_review_report(source_path: Path, destination: Path) -> dict[str, Any]:
    result, rule_results, _ = _reconcile(source_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp.xlsx")
    with pd.ExcelWriter(temporary, engine="openpyxl") as writer:
        for sheet_name, sheet_frame in workbook_frames(result):
            sheet_frame.to_excel(writer, sheet_name=sheet_name, index=False)
            _format_workbook_sheet(writer.book[sheet_name])
    temporary.replace(destination)

    candidates = result["open_issues"]
    mapped = candidates["School Name"].fillna("").astype(str).str.strip().ne("")
    return {
        "path": str(destination),
        "name": destination.name,
        "selection_mode": "published_rules" if rule_results else "default_minimum",
        "candidate_count": int(result["open_issue_count"]),
        "historical_candidate_count": int(result["historical_issue_count"]),
        "corrected_candidate_count": int(result["corrected_issue_count"]),
        "valid_unused_count": int(result["valid_unused_count"]),
        "unique_school_count": int(
            candidates.loc[candidates["School EMIS"].ne(""), "School EMIS"].nunique()
        ),
        "unmapped_school_count": int((~mapped).sum()),
        "rule_results": rule_results,
        "corrective_compliance": result["policy"],
        "reconciliation_match_columns": result["match_columns"],
    }


def analyze_simple_activity_report(path: Path) -> dict[str, Any]:
    result, rule_results, seconds = _reconcile(path)
    invalid = seconds.isna() | seconds.lt(0)
    audit = result["audit"]
    return {
        "schema_version": SCHEMA_VERSION,
        "source_format": "html_table_xls",
        "valid": True,
        "row_count": int(len(audit)),
        "column_count": int(len(_read_table(path).columns)),
        "columns": [str(value) for value in _read_table(path).columns],
        "valid_time_difference_count": int((~invalid).sum()),
        "invalid_time_difference_count": int(invalid.sum()),
        "review_candidate_count": int(result["open_issue_count"]),
        "historical_review_candidate_count": int(result["historical_issue_count"]),
        "corrected_review_candidate_count": int(result["corrected_issue_count"]),
        "rule_results": rule_results,
        "corrective_compliance": result["policy"],
    }


__all__ = [
    "TIME_DIFFERENCE_COLUMN",
    "analyze_simple_activity_report",
    "write_simple_activity_review_report",
]
