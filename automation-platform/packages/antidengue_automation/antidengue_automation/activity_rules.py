from __future__ import annotations

import datetime as dt
import hashlib
import re
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from sqlmodel import Session, col, select

from antidengue_automation.activity_rule_schemas import ActivityRuleInput
from antidengue_automation.models import AntiDengueActivityRule
from automation_core.config import get_settings
from automation_core.models import Artifact
from automation_core.storage_catalog import ensure_artifact_local

DISTANCE_COLUMN = "Distance Difference (In meters)"
TIME_COLUMN = "Activity Date/Time"
REQUIRED_COLUMNS = (
    "ID",
    "District",
    "Town",
    "Hotspot Name",
    DISTANCE_COLUMN,
    "Submitted by",
    TIME_COLUMN,
)
TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")


def validate_rule_values(values: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(values)
    normalized["name"] = str(normalized.get("name") or "").strip()
    if not normalized["name"]:
        raise ValueError("Rule name is required")
    if not normalized.get("distance_enabled") and not normalized.get("time_enabled"):
        raise ValueError("Enable at least one distance or submission-time condition")
    for field in ("time_start", "time_end"):
        value = str(normalized.get(field) or "")
        if not TIME_RE.fullmatch(value):
            raise ValueError(f"{field.replace('_', ' ').title()} must use 24-hour HH:MM")
        normalized[field] = value
    if normalized.get("time_enabled") and normalized["time_start"] == normalized["time_end"]:
        raise ValueError("Submission-time start and end cannot be the same")
    return normalized


def rule_dict(item: AntiDengueActivityRule) -> dict[str, Any]:
    return {
        "id": str(item.id),
        "rule_key": str(item.rule_key),
        "version": item.version,
        "name": item.name,
        "status": item.status,
        "enabled": item.enabled,
        "classification": item.classification,
        "match_mode": item.match_mode,
        "distance_enabled": item.distance_enabled,
        "distance_operator": item.distance_operator,
        "distance_threshold_meters": item.distance_threshold_meters,
        "time_enabled": item.time_enabled,
        "time_operator": item.time_operator,
        "time_start": item.time_start,
        "time_end": item.time_end,
        "timezone": item.timezone,
        "supersedes_id": str(item.supersedes_id) if item.supersedes_id else None,
        "created_by": item.created_by,
        "created_at": item.created_at,
        "updated_at": item.updated_at,
        "published_at": item.published_at,
    }


def _latest_hotspot_report(session: Session) -> tuple[Path, dict[str, Any]]:
    artifact = session.scalar(
        select(Artifact)
        .where(
            Artifact.module_key == "antidengue",
            Artifact.kind == "raw",
            col(Artifact.name).contains("hotspot_distance_report"),
        )
        .order_by(col(Artifact.created_at).desc())
        .limit(1)
    )
    if artifact is not None:
        path = ensure_artifact_local(session, artifact)
        digest = artifact.sha256 or hashlib.sha256(path.read_bytes()).hexdigest()
        return path, {
            "artifact_id": artifact.id,
            "name": artifact.name,
            "sha256": digest,
            "created_at": artifact.created_at,
            "storage_status": artifact.storage_status,
        }

    root = get_settings().antidengue_root / "drop-raw-files"
    candidates = sorted(
        root.glob("hotspot_distance_report_*.xls"),
        key=lambda value: value.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise ValueError("No hotspot-distance report is available for rule preview")
    path = candidates[0]
    return path, {
        "artifact_id": None,
        "name": path.name,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "created_at": dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC),
        "storage_status": "local",
    }


def _minutes(value: str) -> int:
    hour, minute = (int(part) for part in value.split(":"))
    return hour * 60 + minute


def _between_time(minutes: pd.Series, start: int, end: int) -> pd.Series:
    if start < end:
        return minutes.ge(start) & minutes.lt(end)
    return minutes.ge(start) | minutes.lt(end)


def preview_rule(
    session: Session,
    data: ActivityRuleInput,
    *,
    sample_size: int = 20,
) -> dict[str, Any]:
    values = validate_rule_values(data.model_dump())
    path, source = _latest_hotspot_report(session)
    tables = pd.read_html(path)
    if len(tables) != 1:
        raise ValueError(f"Expected one hotspot-distance table, found {len(tables)}")
    frame = tables[0]
    missing = sorted(set(REQUIRED_COLUMNS).difference(map(str, frame.columns)))
    if missing:
        raise ValueError("Latest report is missing columns: " + ", ".join(missing))

    distances = pd.to_numeric(frame[DISTANCE_COLUMN], errors="coerce")
    timestamps = pd.to_datetime(
        frame[TIME_COLUMN].astype(str).str.replace("on ", "", regex=False),
        format="%m/%d/%Y at %I:%M%p",
        errors="coerce",
    )
    conditions: list[pd.Series] = []
    distance_match = None
    if values["distance_enabled"]:
        threshold = float(values["distance_threshold_meters"])
        distance_match = (
            distances.gt(threshold)
            if values["distance_operator"] == "gt"
            else distances.ge(threshold)
        )
        conditions.append(distance_match)
    time_match = None
    time_condition = None
    if values["time_enabled"]:
        minute_values = timestamps.dt.hour.mul(60).add(timestamps.dt.minute)
        time_match = _between_time(
            minute_values,
            _minutes(values["time_start"]),
            _minutes(values["time_end"]),
        )
        time_condition = (
            ~time_match if values["time_operator"] == "outside" else time_match
        ) & timestamps.notna()
        conditions.append(time_condition)

    matched = conditions[0]
    for condition in conditions[1:]:
        matched = matched & condition if values["match_mode"] == "all" else matched | condition
    matched = matched.fillna(False)
    matching_rows = frame[matched].copy()
    matching_rows["_distance"] = distances[matched]
    matching_rows["_timestamp"] = timestamps[matched]
    matching_rows = matching_rows.sort_values("_distance", ascending=False, na_position="last")

    samples = []
    for _, row in matching_rows.head(sample_size).iterrows():
        reasons = []
        distance = row["_distance"]
        timestamp = row["_timestamp"]
        if (
            values["distance_enabled"]
            and pd.notna(distance)
            and distance_match is not None
            and bool(distance_match.loc[row.name])
        ):
            symbol = ">" if values["distance_operator"] == "gt" else "≥"
            reasons.append(f"Distance {float(distance):g} m {symbol} {float(values['distance_threshold_meters']):g} m")
        if (
            values["time_enabled"]
            and pd.notna(timestamp)
            and time_condition is not None
            and bool(time_condition.loc[row.name])
        ):
            relation = "outside" if values["time_operator"] == "outside" else "within"
            reasons.append(
                f"Submitted {timestamp.strftime('%H:%M')} {relation} "
                f"{values['time_start']}–{values['time_end']}"
            )
        samples.append({
            "activity_id": str(row["ID"]),
            "district": str(row["District"]),
            "town": str(row["Town"]),
            "hotspot_name": str(row["Hotspot Name"]),
            "distance_meters": float(distance) if pd.notna(distance) else None,
            "submitted_by": str(row["Submitted by"]),
            "activity_datetime": str(row[TIME_COLUMN]),
            "explanation": "; ".join(reasons),
        })

    submitters = matching_rows["Submitted by"].fillna("").astype(str).str.strip()
    hotspots = matching_rows["Hotspot Name"].fillna("").astype(str).str.strip()
    return {
        "source": source,
        "rule": values,
        "total_activities": int(len(frame)),
        "matching_activities": int(matched.sum()),
        "unique_submitters": int(submitters[submitters.ne("")].nunique()),
        "unique_hotspots": int(hotspots[hotspots.ne("")].nunique()),
        "invalid_distance_count": int(distances.isna().sum()),
        "invalid_time_count": int(timestamps.isna().sum()),
        "sample": samples,
    }


def rule_input_from_model(item: AntiDengueActivityRule) -> ActivityRuleInput:
    return ActivityRuleInput(**{
        key: value
        for key, value in rule_dict(item).items()
        if key in ActivityRuleInput.model_fields
    })


__all__ = [
    "preview_rule",
    "rule_dict",
    "rule_input_from_model",
    "validate_rule_values",
]
