from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from compliance_reconciliation import reconcile_activities, workbook_frames

SCHEMA_VERSION = "hotspot_distance.v1"
DISTANCE_COLUMN = "Distance Difference (In meters)"
REQUIRED_COLUMNS = (
    "ID",
    "District",
    "Town",
    "Sub Department",
    "Tag",
    "Hotspot Name",
    "Hotspot(Lat,Long)",
    "Activity(Lat,Long)",
    DISTANCE_COLUMN,
    "Submitted by",
    "Activity Date/Time",
)


def configured_review_threshold() -> float | None:
    raw = os.getenv("HOTSPOT_DISTANCE_REVIEW_THRESHOLD_METERS", "").strip()
    if not raw:
        return None
    threshold = float(raw)
    if threshold < 0:
        raise ValueError("HOTSPOT_DISTANCE_REVIEW_THRESHOLD_METERS cannot be negative")
    return threshold


def _snapshot() -> dict[str, Any]:
    raw_path = os.getenv("ANTIDENGUE_RUNTIME_SNAPSHOT", "").strip()
    if not raw_path:
        return {}
    try:
        snapshot = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read activity rules from runtime snapshot: {raw_path}") from exc
    if snapshot.get("schema_version") != 1 or snapshot.get("source") != "automation-platform-postgresql":
        raise RuntimeError("Activity rules require a trusted runtime snapshot")
    return snapshot


def _runtime_rules() -> list[dict[str, Any]]:
    rules = _snapshot().get("activity_rules") or []
    if not isinstance(rules, list):
        raise RuntimeError("Runtime snapshot activity_rules must be a list")
    return [item for item in rules if isinstance(item, dict)]


def _time_minutes(value: str) -> int:
    parsed = dt.time.fromisoformat(value)
    return parsed.hour * 60 + parsed.minute


def _time_between(values: pd.Series, start: int, end: int) -> pd.Series:
    if start < end:
        return values.ge(start) & values.lt(end)
    return values.ge(start) | values.lt(end)


def _parsed_activity_times(frame: pd.DataFrame) -> pd.Series:
    values = (
        frame["Activity Date/Time"]
        .fillna("")
        .astype(str)
        .str.replace(r"^\s*on\s+", "", regex=True, case=False)
        .str.replace(r"\s+at\s+", " ", regex=True, case=False)
        .str.strip()
    )
    try:
        return pd.to_datetime(values, errors="coerce", format="mixed")
    except TypeError:
        return pd.to_datetime(values, errors="coerce")


def _evaluate_rules(frame: pd.DataFrame, distances: pd.Series) -> tuple[list[dict], pd.Series]:
    timestamps = _parsed_activity_times(frame)
    combined = pd.Series(False, index=frame.index)
    results = []
    for rule in _runtime_rules():
        conditions = []
        if rule.get("distance_enabled"):
            threshold = float(rule.get("distance_threshold_meters") or 0)
            conditions.append(
                distances.gt(threshold)
                if rule.get("distance_operator") == "gt"
                else distances.ge(threshold)
            )
        if rule.get("time_enabled"):
            minute_values = timestamps.dt.hour.mul(60).add(timestamps.dt.minute)
            between = _time_between(
                minute_values,
                _time_minutes(str(rule.get("time_start") or "00:00")),
                _time_minutes(str(rule.get("time_end") or "07:00")),
            )
            time_condition = (
                ~between if rule.get("time_operator") == "outside" else between
            ) & timestamps.notna()
            conditions.append(time_condition)
        if not conditions:
            continue
        matched = conditions[0]
        for condition in conditions[1:]:
            matched = (
                matched & condition
                if rule.get("match_mode") != "any"
                else matched | condition
            )
        matched = matched.fillna(False)
        combined |= matched
        matching = frame[matched]
        submitters = matching["Submitted by"].fillna("").astype(str).str.strip()
        results.append({
            "rule_id": rule.get("id"),
            "rule_key": rule.get("rule_key"),
            "rule_version": rule.get("version"),
            "rule_name": rule.get("name"),
            "classification": rule.get("classification", "review_required"),
            "matching_activity_count": int(matched.sum()),
            "unique_submitter_count": int(submitters[submitters.ne("")].nunique()),
            "activity_id_sample": [str(value) for value in matching["ID"].head(20)],
        })
    return results, combined


def _valid_correction_mask(
    frame: pd.DataFrame,
    distances: pd.Series,
    *,
    rule_results: list[dict],
    issue_mask: pd.Series,
    threshold: float | None,
) -> pd.Series:
    numeric = distances.notna() & distances.ge(0)
    if not rule_results:
        if threshold is None:
            return pd.Series(False, index=frame.index)
        return (numeric & distances.le(threshold)).fillna(False)

    # A correction must pass every published hotspot rule, including any
    # combined distance/time condition. Passing distance alone must never clear
    # an issue that is still review-required for another active condition.
    return (numeric & ~issue_mask).fillna(False)


def _read_classified_frame(
    path: Path,
) -> tuple[pd.DataFrame, pd.Series, pd.Series, pd.Series, list[dict], str, float | None]:
    tables = pd.read_html(path)
    if len(tables) != 1:
        raise ValueError(f"Expected one hotspot-distance table, found {len(tables)}")
    frame = tables[0]
    missing = sorted(set(REQUIRED_COLUMNS).difference(map(str, frame.columns)))
    if missing:
        raise ValueError(
            "Hotspot-distance report is missing expected columns: " + ", ".join(missing)
        )

    distances = pd.to_numeric(frame[DISTANCE_COLUMN], errors="coerce")
    rule_results, rule_candidate_mask = _evaluate_rules(frame, distances)
    threshold = configured_review_threshold()
    if rule_results:
        issue_mask = rule_candidate_mask.fillna(False)
        selection_mode = "published_rules"
    elif threshold is None:
        issue_mask = pd.Series(False, index=frame.index)
        selection_mode = "not_configured"
    else:
        issue_mask = distances.gt(threshold).fillna(False)
        selection_mode = "legacy_threshold"
    valid_correction_mask = _valid_correction_mask(
        frame,
        distances,
        rule_results=rule_results,
        issue_mask=issue_mask,
        threshold=threshold,
    )
    return (
        frame,
        issue_mask,
        valid_correction_mask,
        distances,
        rule_results,
        selection_mode,
        threshold,
    )


def _runtime_school_index() -> dict[str, dict]:
    return {
        str(item.get("School EMIS") or "").strip(): item
        for item in _snapshot().get("master_schools") or []
        if isinstance(item, dict) and str(item.get("School EMIS") or "").strip()
    }


def _emis_series(frame: pd.DataFrame) -> pd.Series:
    return (
        frame["Submitted by"]
        .fillna("")
        .astype(str)
        .str.extract(r"^(\d{5,})", expand=False)
        .fillna("")
    )


def _with_authoritative_routing(frame: pd.DataFrame, emis_values: pd.Series) -> pd.DataFrame:
    routed = frame.copy()
    for name in (
        "School EMIS",
        "School Name",
        "Tehsil",
        "Markaz",
        "Wing ID",
        "Tehsil ID",
        "Markaz ID",
        "Review Classification",
    ):
        if name in routed.columns:
            routed = routed.drop(columns=[name])
    routed.insert(0, "School EMIS", emis_values.reindex(routed.index).fillna("").astype(str))
    school_index = _runtime_school_index()

    def school_value(emis: object, key: str) -> str:
        return str(school_index.get(str(emis).strip(), {}).get(key) or "")

    routed.insert(1, "School Name", routed["School EMIS"].map(lambda value: school_value(value, "School Name")))
    routed.insert(2, "Tehsil", routed["School EMIS"].map(lambda value: school_value(value, "Tehsil")))
    routed.insert(3, "Markaz", routed["School EMIS"].map(lambda value: school_value(value, "Markaz")))
    routed.insert(4, "Wing ID", routed["School EMIS"].map(lambda value: school_value(value, "_wing_id")))
    routed.insert(5, "Tehsil ID", routed["School EMIS"].map(lambda value: school_value(value, "_tehsil_id")))
    routed.insert(6, "Markaz ID", routed["School EMIS"].map(lambda value: school_value(value, "_markaz_id")))
    return routed


def _reconcile(path: Path) -> tuple[dict[str, object], list[dict], str, float | None, pd.Series]:
    (
        frame,
        issue_mask,
        valid_correction_mask,
        distances,
        rule_results,
        selection_mode,
        threshold,
    ) = _read_classified_frame(path)
    emis = _emis_series(frame)
    routed = _with_authoritative_routing(frame, emis)
    result = reconcile_activities(
        routed,
        issue_mask=issue_mask,
        valid_correction_mask=valid_correction_mask,
        school_emis=routed["School EMIS"],
        stream="hotspot_distance",
        match_column_groups=("Tag", "Hotspot Name"),
        timestamp_candidates=("Activity Date/Time",),
    )
    return result, rule_results, selection_mode, threshold, distances


def _format_workbook_sheet(sheet) -> None:
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions


def write_hotspot_review_report(source_path: Path, destination: Path) -> dict[str, object]:
    """Write open, corrected and audit activity sets without deleting history."""

    result, rule_results, selection_mode, _, _ = _reconcile(source_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp.xlsx")
    with pd.ExcelWriter(temporary, engine="openpyxl") as writer:
        for sheet_name, sheet_frame in workbook_frames(result):
            sheet_frame.to_excel(writer, sheet_name=sheet_name, index=False)
            _format_workbook_sheet(writer.book[sheet_name])
    temporary.replace(destination)

    candidates = result["open_issues"]
    known = candidates["School Name"].fillna("").astype(str).str.strip().ne("")
    return {
        "path": str(destination),
        "name": destination.name,
        "selection_mode": selection_mode,
        "candidate_count": int(result["open_issue_count"]),
        "historical_candidate_count": int(result["historical_issue_count"]),
        "corrected_candidate_count": int(result["corrected_issue_count"]),
        "valid_unused_count": int(result["valid_unused_count"]),
        "unique_school_count": int(
            candidates.loc[candidates["School EMIS"].ne(""), "School EMIS"].nunique()
        ),
        "unmapped_school_count": int((~known).sum()),
        "rule_results": rule_results,
        "corrective_compliance": result["policy"],
        "reconciliation_match_columns": result["match_columns"],
    }


def analyze_hotspot_distance_report(path: Path) -> dict[str, object]:
    result, rule_results, selection_mode, threshold, distances = _reconcile(path)
    audit = result["audit"]
    valid_distances = distances.dropna()
    submitters = audit["Submitted by"].fillna("").astype(str).str.strip()
    candidates = result["open_issues"]
    candidate_sample = [
        {
            "activity_id": str(row["ID"]),
            "hotspot_name": str(row["Hotspot Name"]),
            "distance_meters": float(row[DISTANCE_COLUMN]),
            "submitted_by": str(row["Submitted by"]),
            "activity_datetime": str(row["Activity Date/Time"]),
        }
        for _, row in candidates.sort_values(DISTANCE_COLUMN, ascending=False).head(20).iterrows()
    ]

    return {
        "schema_version": SCHEMA_VERSION,
        "source_format": "html_table_xls",
        "valid": True,
        "row_count": int(len(audit)),
        "column_count": int(len(audit.columns) - 9),
        "columns": [
            str(value)
            for value in audit.columns
            if value
            not in {
                "Compliance Stream",
                "Corrective Policy Version",
                "Compliance Status",
                "Correction Activity ID",
                "Correction Activity Date/Time",
                "Corrects Activity ID",
                "Reconciliation Match Key",
                "Reconciliation Window",
                "Reconciliation Time Source",
            }
        ],
        "valid_distance_count": int(len(valid_distances)),
        "missing_distance_count": int(distances.isna().sum()),
        "unique_submitter_count": int(submitters[submitters.ne("")].nunique()),
        "distance_meters": {
            "minimum": float(valid_distances.min()) if not valid_distances.empty else None,
            "median": float(valid_distances.median()) if not valid_distances.empty else None,
            "p90": float(valid_distances.quantile(0.90)) if not valid_distances.empty else None,
            "maximum": float(valid_distances.max()) if not valid_distances.empty else None,
        },
        "review_threshold_meters": threshold,
        "review_candidate_count": (
            int(result["open_issue_count"])
            if selection_mode != "not_configured"
            else None
        ),
        "historical_review_candidate_count": int(result["historical_issue_count"]),
        "corrected_review_candidate_count": int(result["corrected_issue_count"]),
        "review_candidate_sample": candidate_sample,
        "classification": (
            "review_required"
            if rule_results
            else "distance_review_candidates"
            if threshold is not None
            else "threshold_not_configured"
        ),
        "rule_results": rule_results,
        "corrective_compliance": result["policy"],
        "selection_mode": selection_mode,
    }
