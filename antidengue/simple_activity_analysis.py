from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

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


def _candidate_mask(frame: pd.DataFrame) -> tuple[pd.Series, list[dict[str, Any]], pd.Series]:
    seconds = pd.to_numeric(frame[_time_difference_column(frame)], errors="coerce")
    valid = seconds.ge(0)
    combined = pd.Series(False, index=frame.index)
    results: list[dict[str, Any]] = []
    rules = _rules()
    if not rules:
        threshold = float(os.getenv("SIMPLE_ACTIVITY_MINIMUM_SECONDS", "300"))
        combined = valid & seconds.lt(threshold)
        return combined.fillna(False), [], seconds
    for rule in rules:
        threshold = float(rule.get("minimum_seconds") or 300)
        matched = seconds.le(threshold) if rule.get("operator") == "lte" else seconds.lt(threshold)
        matched = (valid & matched).fillna(False)
        combined |= matched
        results.append({
            "rule_id": rule.get("id"),
            "rule_key": rule.get("rule_key"),
            "rule_version": rule.get("version"),
            "rule_name": rule.get("name"),
            "classification": "review_required",
            "matching_activity_count": int(matched.sum()),
        })
    return combined, results, seconds


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
            return frame[name].fillna("").astype(str).str.extract(r"(\d{5,})", expand=False).fillna("")
    submitted = _submitted_column(frame)
    if submitted:
        return frame[submitted].fillna("").astype(str).str.extract(r"^(\d{5,})", expand=False).fillna("")
    return pd.Series("", index=frame.index)


def write_simple_activity_review_report(source_path: Path, destination: Path) -> dict[str, Any]:
    frame = _read_table(source_path)
    mask, rule_results, _ = _candidate_mask(frame)
    candidates = frame[mask].copy()
    source_time_column = _time_difference_column(candidates)
    if source_time_column != TIME_DIFFERENCE_COLUMN:
        candidates.insert(0, TIME_DIFFERENCE_COLUMN, candidates[source_time_column])
    emis_values = _emis_series(candidates)
    for authoritative in ("School EMIS", "School Name", "Tehsil", "Markaz", "Wing ID", "Tehsil ID", "Markaz ID", "Review Classification"):
        if authoritative in candidates.columns:
            candidates = candidates.drop(columns=[authoritative])
    candidates.insert(0, "School EMIS", emis_values)
    index = _school_index()

    def school_value(emis: object, key: str) -> str:
        return str(index.get(str(emis).strip(), {}).get(key) or "")

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
        sheet = writer.book["Review Required"]
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
    temporary.replace(destination)
    mapped = candidates["School Name"].fillna("").astype(str).str.strip().ne("")
    return {
        "path": str(destination), "name": destination.name,
        "selection_mode": "published_rules" if rule_results else "default_minimum",
        "candidate_count": int(len(candidates)),
        "unique_school_count": int(candidates.loc[candidates["School EMIS"].ne(""), "School EMIS"].nunique()),
        "unmapped_school_count": int((~mapped).sum()), "rule_results": rule_results,
    }


def analyze_simple_activity_report(path: Path) -> dict[str, Any]:
    frame = _read_table(path)
    mask, results, seconds = _candidate_mask(frame)
    invalid = seconds.isna() | seconds.lt(0)
    return {
        "schema_version": SCHEMA_VERSION, "source_format": "html_table_xls", "valid": True,
        "row_count": int(len(frame)), "column_count": int(len(frame.columns)),
        "columns": [str(value) for value in frame.columns],
        "valid_time_difference_count": int((~invalid).sum()),
        "invalid_time_difference_count": int(invalid.sum()),
        "review_candidate_count": int(mask.sum()), "rule_results": results,
    }


__all__ = ["TIME_DIFFERENCE_COLUMN", "analyze_simple_activity_report", "write_simple_activity_review_report"]
