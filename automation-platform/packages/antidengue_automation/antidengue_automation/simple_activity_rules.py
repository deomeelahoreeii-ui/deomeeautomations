from __future__ import annotations

import datetime as dt
import hashlib
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlmodel import Session, col, select

from antidengue_automation.models import AntiDengueSimpleActivityRule
from automation_core.config import get_settings
from automation_core.models import Artifact
from automation_core.storage_catalog import ensure_artifact_local

TIME_DIFFERENCE_COLUMN = "Time Difference(Sec)"
TIME_DIFFERENCE_ALIASES = (TIME_DIFFERENCE_COLUMN, "Picture Time Difference(Sec)")


def _time_column(frame: pd.DataFrame) -> str:
    for name in TIME_DIFFERENCE_ALIASES:
        if name in frame.columns: return name
    raise ValueError("Latest Simple Activity List is missing a supported timing column: " + ", ".join(TIME_DIFFERENCE_ALIASES))


class SimpleActivityRuleInput(BaseModel):
    name: str = Field(min_length=1, max_length=180)
    operator: str = Field(default="lt", pattern="^(lt|lte)$")
    minimum_seconds: int = Field(default=300, ge=1, le=86400)
    timezone: str = "Asia/Karachi"
    created_by: str = Field(default="web-operator", max_length=120)


def rule_dict(item: AntiDengueSimpleActivityRule) -> dict[str, Any]:
    return {
        "id": str(item.id), "rule_key": str(item.rule_key), "version": item.version,
        "name": item.name, "status": item.status, "enabled": item.enabled,
        "operator": item.operator, "minimum_seconds": item.minimum_seconds,
        "minimum_minutes": item.minimum_seconds / 60, "timezone": item.timezone,
        "supersedes_id": str(item.supersedes_id) if item.supersedes_id else None,
        "created_by": item.created_by, "created_at": item.created_at,
        "updated_at": item.updated_at, "published_at": item.published_at,
    }


def latest_report(session: Session) -> tuple[Path, dict[str, Any]]:
    artifact = session.scalar(select(Artifact).where(
        Artifact.module_key == "antidengue", Artifact.kind == "raw",
        col(Artifact.name).contains("simple_activity_list"),
    ).order_by(col(Artifact.created_at).desc()).limit(1))
    if artifact:
        path = ensure_artifact_local(session, artifact)
        return path, {"artifact_id": artifact.id, "name": artifact.name, "sha256": artifact.sha256 or hashlib.sha256(path.read_bytes()).hexdigest(), "created_at": artifact.created_at}
    root = get_settings().antidengue_root / "drop-raw-files"
    candidates = sorted(root.glob("simple_activity_list_*.xls"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not candidates:
        raise ValueError("No Simple Activity List report is available for rule preview")
    path = candidates[0]
    return path, {"artifact_id": None, "name": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "created_at": dt.datetime.fromtimestamp(path.stat().st_mtime, tz=dt.UTC)}


def preview_rule(session: Session, data: SimpleActivityRuleInput, sample_size: int = 20) -> dict[str, Any]:
    path, source = latest_report(session)
    tables = pd.read_html(path)
    if len(tables) != 1:
        raise ValueError("Expected one Simple Activity List table")
    frame = tables[0]
    seconds = pd.to_numeric(frame[_time_column(frame)], errors="coerce")
    valid = seconds.ge(0)
    matched = valid & (seconds.le(data.minimum_seconds) if data.operator == "lte" else seconds.lt(data.minimum_seconds))
    matching = frame[matched.fillna(False)]
    samples = []
    for index, row in matching.head(sample_size).iterrows():
        samples.append({
            "activity_id": str(row.get("ID", index)),
            "submitted_by": str(row.get("Submitted by", row.get("Submitted By", ""))),
            "time_difference_seconds": float(seconds.loc[index]),
            "time_difference_display": f"{float(seconds.loc[index]) / 60:.1f} minutes",
        })
    return {"source": source, "rule": data.model_dump(), "total_activities": int(len(frame)), "matching_activities": int(matched.sum()), "invalid_time_difference_count": int((seconds.isna() | seconds.lt(0)).sum()), "sample": samples}


__all__ = ["SimpleActivityRuleInput", "preview_rule", "rule_dict"]
