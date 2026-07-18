from __future__ import annotations

from typing import Literal

from sqlmodel import Field, SQLModel


class ActivityRuleInput(SQLModel):
    name: str = Field(min_length=1, max_length=180)
    classification: Literal["review_required"] = "review_required"
    match_mode: Literal["all", "any"] = "all"
    distance_enabled: bool = True
    distance_operator: Literal["gt", "gte"] = "gte"
    distance_threshold_meters: float = Field(default=50, ge=0, le=100000)
    time_enabled: bool = False
    time_operator: Literal["between", "outside"] = "between"
    time_start: str = Field(default="00:00", min_length=5, max_length=5)
    time_end: str = Field(default="07:00", min_length=5, max_length=5)
    timezone: Literal["Asia/Karachi"] = "Asia/Karachi"
    created_by: str = Field(default="web-operator", max_length=120)


class ActivityRuleUpdate(SQLModel):
    name: str | None = Field(default=None, min_length=1, max_length=180)
    match_mode: Literal["all", "any"] | None = None
    distance_enabled: bool | None = None
    distance_operator: Literal["gt", "gte"] | None = None
    distance_threshold_meters: float | None = Field(default=None, ge=0, le=100000)
    time_enabled: bool | None = None
    time_operator: Literal["between", "outside"] | None = None
    time_start: str | None = Field(default=None, min_length=5, max_length=5)
    time_end: str | None = Field(default=None, min_length=5, max_length=5)


class ActivityRulePreviewInput(ActivityRuleInput):
    sample_size: int = Field(default=20, ge=1, le=100)


__all__ = ["ActivityRuleInput", "ActivityRulePreviewInput", "ActivityRuleUpdate"]
