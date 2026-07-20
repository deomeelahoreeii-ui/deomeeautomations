from __future__ import annotations

import os
import json
import datetime as dt
from pathlib import Path

import pandas as pd

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


def _runtime_rules() -> list[dict]:
    raw_path = os.getenv("ANTIDENGUE_RUNTIME_SNAPSHOT", "").strip()
    if not raw_path:
        return []
    try:
        snapshot = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read activity rules from runtime snapshot: {raw_path}") from exc
    if snapshot.get("schema_version") != 1 or snapshot.get("source") != "automation-platform-postgresql":
        raise RuntimeError("Activity rules require a trusted runtime snapshot")
    rules = snapshot.get("activity_rules") or []
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


def _evaluate_rules(frame: pd.DataFrame, distances: pd.Series) -> tuple[list[dict], pd.Series]:
    timestamps = pd.to_datetime(
        frame["Activity Date/Time"].astype(str).str.replace("on ", "", regex=False),
        format="%m/%d/%Y at %I:%M%p",
        errors="coerce",
    )
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


def _candidate_frame(path: Path) -> tuple[pd.DataFrame, list[dict], str]:
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
    if rule_results:
        return frame[rule_candidate_mask].copy(), rule_results, "published_rules"
    threshold = configured_review_threshold()
    if threshold is None:
        return frame.iloc[0:0].copy(), [], "not_configured"
    return frame[distances.gt(threshold)].copy(), [], "legacy_threshold"


def _runtime_school_index() -> dict[str, dict]:
    raw_path = os.getenv("ANTIDENGUE_RUNTIME_SNAPSHOT", "").strip()
    if not raw_path:
        return {}
    try:
        snapshot = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read school routing from runtime snapshot: {raw_path}") from exc
    if snapshot.get("schema_version") != 1 or snapshot.get("source") != "automation-platform-postgresql":
        raise RuntimeError("Hotspot routing requires a trusted runtime snapshot")
    return {
        str(item.get("School EMIS") or "").strip(): item
        for item in snapshot.get("master_schools") or []
        if isinstance(item, dict) and str(item.get("School EMIS") or "").strip()
    }


def write_hotspot_review_report(source_path: Path, destination: Path) -> dict[str, object]:
    """Write the complete frozen set of activities selected by published rules.

    The generated workbook carries authoritative hierarchy identifiers from the
    platform runtime snapshot. The WhatsApp compiler can therefore reuse an
    existing audience safely without trusting portal-provided town labels.
    """
    candidates, rule_results, selection_mode = _candidate_frame(source_path)
    school_index = _runtime_school_index()
    submitted = candidates["Submitted by"].fillna("").astype(str)
    candidates.insert(0, "School EMIS", submitted.str.extract(r"^(\d+)", expand=False).fillna(""))

    def school_value(emis: object, key: str) -> str:
        return str(school_index.get(str(emis).strip(), {}).get(key) or "")

    candidates.insert(1, "School Name", candidates["School EMIS"].map(lambda value: school_value(value, "School Name")))
    candidates.insert(2, "Tehsil", candidates["School EMIS"].map(lambda value: school_value(value, "Tehsil")))
    candidates.insert(3, "Markaz", candidates["School EMIS"].map(lambda value: school_value(value, "Markaz")))
    candidates.insert(4, "Wing ID", candidates["School EMIS"].map(lambda value: school_value(value, "_wing_id")))
    candidates.insert(5, "Tehsil ID", candidates["School EMIS"].map(lambda value: school_value(value, "_tehsil_id")))
    candidates.insert(6, "Markaz ID", candidates["School EMIS"].map(lambda value: school_value(value, "_markaz_id")))
    candidates.insert(7, "Review Classification", "Review required")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp.xlsx")
    with pd.ExcelWriter(temporary, engine="openpyxl") as writer:
        candidates.to_excel(writer, sheet_name="Review Required", index=False)
        worksheet = writer.book["Review Required"]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions
    temporary.replace(destination)
    known = candidates["School Name"].fillna("").astype(str).str.strip().ne("")
    return {
        "path": str(destination),
        "name": destination.name,
        "selection_mode": selection_mode,
        "candidate_count": int(len(candidates)),
        "unique_school_count": int(candidates.loc[candidates["School EMIS"].ne(""), "School EMIS"].nunique()),
        "unmapped_school_count": int((~known).sum()),
        "rule_results": rule_results,
    }


def analyze_hotspot_distance_report(path: Path) -> dict[str, object]:
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
    valid_distances = distances.dropna()
    submitters = frame["Submitted by"].fillna("").astype(str).str.strip()
    threshold = configured_review_threshold()
    rule_results, rule_candidate_mask = _evaluate_rules(frame, distances)
    candidate_mask = (
        rule_candidate_mask
        if rule_results
        else (distances.gt(threshold) if threshold is not None else None)
    )
    candidates = frame[candidate_mask] if candidate_mask is not None else frame.iloc[0:0]
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
        "row_count": int(len(frame)),
        "column_count": int(len(frame.columns)),
        "columns": [str(value) for value in frame.columns],
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
            int(len(candidates)) if rule_results or threshold is not None else None
        ),
        "review_candidate_sample": candidate_sample,
        "classification": (
            "review_required"
            if rule_results
            else "distance_review_candidates"
            if threshold is not None
            else "threshold_not_configured"
        ),
        "rule_results": rule_results,
    }


__all__ = [
    "analyze_hotspot_distance_report",
    "configured_review_threshold",
    "write_hotspot_review_report",
]
