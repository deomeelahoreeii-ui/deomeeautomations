from __future__ import annotations

import uuid
from datetime import date
from typing import Literal

from sqlmodel import Field, SQLModel


class AntiDengueRunRequest(SQLModel):
    """Legacy endpoint contract retained for rollback compatibility.

    The web dashboard and scheduler no longer use dry_run=False. New sends always
    originate from a successful dry run and an immutable approved preview.
    """

    dry_run: bool = Field(default=True)
    login_mode: Literal["auto", "manual", "remote_approve"] = Field(default="auto")


class AntiDengueScheduleCreate(SQLModel):
    name: str = Field(min_length=1, max_length=180)
    enabled: bool = True
    recurrence_type: Literal["once", "daily", "weekly"] = "daily"
    run_date: date | None = None
    weekdays: list[int] = Field(default_factory=list)
    times: list[str] = Field(default_factory=lambda: ["08:30"])
    timezone: str = Field(default="Asia/Karachi", min_length=1, max_length=80)
    submission_deadline_override: str | None = Field(default=None, max_length=20)
    login_mode: Literal["auto", "manual", "remote_approve"] = "auto"
    dispatch_policy: Literal["preview_only", "auto_send_when_clean"] = "preview_only"
    dispatch_profile_id: uuid.UUID | None = None
    dispatch_profile_ids: list[uuid.UUID] = Field(default_factory=list, max_length=50)
    missed_run_grace_minutes: int = Field(default=15, ge=0, le=240)
    overlap_grace_minutes: int = Field(default=15, ge=0, le=240)
    created_by: str = Field(default="web-operator", max_length=120)


class AntiDengueScheduleUpdate(SQLModel):
    name: str | None = Field(default=None, min_length=1, max_length=180)
    enabled: bool | None = None
    recurrence_type: Literal["once", "daily", "weekly"] | None = None
    run_date: date | None = None
    weekdays: list[int] | None = None
    times: list[str] | None = None
    timezone: str | None = Field(default=None, min_length=1, max_length=80)
    submission_deadline_override: str | None = Field(default=None, max_length=20)
    login_mode: Literal["auto", "manual", "remote_approve"] | None = None
    dispatch_policy: Literal["preview_only", "auto_send_when_clean"] | None = None
    dispatch_profile_id: uuid.UUID | None = None
    dispatch_profile_ids: list[uuid.UUID] | None = Field(default=None, min_length=1, max_length=50)
    missed_run_grace_minutes: int | None = Field(default=None, ge=0, le=240)
    overlap_grace_minutes: int | None = Field(default=None, ge=0, le=240)


class AntiDengueExecutionCreate(SQLModel):
    login_mode: Literal["auto", "manual", "remote_approve"] = "auto"
    dispatch_policy: Literal["preview_only", "auto_send_when_clean"] = "preview_only"
    dispatch_profile_id: uuid.UUID | None = None
    dispatch_profile_ids: list[uuid.UUID] = Field(default_factory=list, max_length=50)
    created_by: str = Field(default="web-operator", max_length=120)


class AntiDengueRunNowRequest(SQLModel):
    created_by: str = Field(default="web-operator", max_length=120)


class AntiDengueDeadlinePolicyUpdate(SQLModel):
    submission_deadline: str = Field(min_length=1, max_length=20)
    timezone: str = Field(default="Asia/Karachi", min_length=1, max_length=80)
    updated_by: str = Field(default="web-operator", max_length=120)
