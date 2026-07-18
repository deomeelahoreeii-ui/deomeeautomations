from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import CheckConstraint, Column, DateTime, Float, ForeignKey, JSON, Text, UniqueConstraint, Uuid
from sqlalchemy.types import TypeDecorator
from sqlmodel import Field, SQLModel

from automation_core.time import utcnow


class UTCDateTime(TypeDecorator):
    """Store UTC and always return timezone-aware datetimes on every dialect.

    PostgreSQL preserves timezone information for TIMESTAMPTZ. SQLite does not,
    so its result processor must explicitly restore UTC. Keeping this behavior
    in the SQLAlchemy type prevents scheduler timestamps from being interpreted
    as local wall-clock values during tests, development, or SQLite tooling.
    """

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):  # type: ignore[no-untyped-def]
        return dialect.type_descriptor(DateTime(timezone=True))

    def process_bind_param(self, value: datetime | None, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        if dialect.name == "sqlite":
            return normalized.replace(tzinfo=None)
        return normalized

    def process_result_value(self, value: datetime | None, dialect):  # type: ignore[no-untyped-def]
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


def utc_datetime_column(*, nullable: bool, index: bool = True) -> Column:
    return Column(UTCDateTime(), nullable=nullable, index=index)


class AntiDengueSchedule(SQLModel, table=True):
    """Durable server-owned schedule for the proven dry-run dispatch planner."""

    __tablename__ = "antidengue_schedules"
    __table_args__ = (
        CheckConstraint(
            "recurrence_type IN ('once', 'daily', 'weekly')",
            name="ck_antidengue_schedules_recurrence_type",
        ),
        CheckConstraint(
            "login_mode IN ('auto', 'manual', 'remote_approve')",
            name="ck_antidengue_schedules_login_mode",
        ),
        CheckConstraint(
            "dispatch_policy IN ('preview_only', 'auto_send_when_clean')",
            name="ck_antidengue_schedules_dispatch_policy",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    name: str = Field(index=True, min_length=1, max_length=180)
    enabled: bool = Field(default=True, index=True)
    recurrence_type: str = Field(default="daily", index=True)
    run_date: date | None = Field(default=None, index=True)
    weekdays: list[int] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    times: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    timezone: str = Field(default="Asia/Karachi", index=True, max_length=80)
    login_mode: str = Field(default="auto", index=True)
    dispatch_policy: str = Field(default="preview_only", index=True)
    dispatch_profile_id: uuid.UUID = Field(
        sa_column=Column(Uuid, ForeignKey("whatsapp_dispatch_profiles.id"), nullable=False, index=True)
    )
    dispatch_profile_ids: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    missed_run_grace_minutes: int = Field(default=15, ge=0, le=240)
    overlap_grace_minutes: int = Field(default=15, ge=0, le=240)
    next_run_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))
    last_run_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))
    last_run_status: str | None = Field(default=None, index=True, max_length=80)
    archived_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))
    created_by: str = Field(default="web-operator", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))
    updated_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))


class AntiDengueScheduleExecution(SQLModel, table=True):
    """One auditable manual or scheduled AntiDengue orchestration occurrence."""

    __tablename__ = "antidengue_schedule_executions"
    __table_args__ = (
        UniqueConstraint("execution_key", name="uq_antidengue_schedule_execution_key"),
        CheckConstraint(
            "trigger_type IN ('scheduled', 'manual_preview', 'manual_send', 'schedule_run_now')",
            name="ck_antidengue_schedule_executions_trigger_type",
        ),
        CheckConstraint(
            "dispatch_policy IN ('preview_only', 'auto_send_when_clean')",
            name="ck_antidengue_schedule_executions_dispatch_policy",
        ),
        CheckConstraint(
            "status IN ("
            "'due', 'waiting_overlap', 'dry_run_queued', 'dry_run_running', "
            "'preview_queued', 'preview_compiling', 'preview_ready', 'auto_approving', "
            "'dispatch_queued', 'dispatch_running', 'completed', "
            "'completed_with_delivery_errors', 'blocked', 'failed', 'skipped', 'cancelled'"
            ")",
            name="ck_antidengue_schedule_executions_status",
        ),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    execution_key: str = Field(unique=True, index=True, max_length=220)
    execution_code: str = Field(unique=True, index=True, max_length=80)
    schedule_id: uuid.UUID | None = Field(
        default=None,
        sa_column=Column(
            Uuid,
            ForeignKey("antidengue_schedules.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
    )
    trigger_type: str = Field(default="scheduled", index=True)
    scheduled_for: datetime = Field(sa_column=utc_datetime_column(nullable=False))
    status: str = Field(default="due", index=True)
    dispatch_policy: str = Field(default="preview_only", index=True)
    login_mode: str = Field(default="auto", index=True)
    dispatch_profile_id: uuid.UUID = Field(
        sa_column=Column(Uuid, ForeignKey("whatsapp_dispatch_profiles.id"), nullable=False, index=True)
    )
    dispatch_profile_ids: list[str] = Field(default_factory=list, sa_column=Column(JSON, nullable=False))
    overlap_deadline_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))
    source_job_id: uuid.UUID | None = Field(default=None, foreign_key="jobs.id", index=True)
    preview_job_id: uuid.UUID | None = Field(default=None, foreign_key="jobs.id", index=True)
    preview_id: uuid.UUID | None = Field(
        default=None,
        sa_column=Column(
            Uuid,
            ForeignKey("whatsapp_dispatch_previews.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
    )
    send_job_id: uuid.UUID | None = Field(default=None, foreign_key="jobs.id", index=True)
    source_summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    preview_summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    dispatch_summary: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    error: str | None = Field(default=None, sa_column=Column(Text))
    created_by: str = Field(default="scheduler", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))
    started_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))
    finished_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))
    updated_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))


class AntiDengueScheduleEvent(SQLModel, table=True):
    """Persistent orchestration activity that survives refreshes and restarts."""

    __tablename__ = "antidengue_schedule_events"

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    execution_id: uuid.UUID = Field(
        sa_column=Column(
            Uuid,
            ForeignKey("antidengue_schedule_executions.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        )
    )
    level: str = Field(default="info", index=True, max_length=30)
    event_type: str = Field(index=True, max_length=80)
    message: str = Field(sa_column=Column(Text, nullable=False))
    details: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON, nullable=False))
    created_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))


class AntiDengueActivityRule(SQLModel, table=True):
    """Versioned, auditable classification rule for portal activity evidence."""

    __tablename__ = "antidengue_activity_rules"
    __table_args__ = (
        UniqueConstraint("rule_key", "version", name="uq_antidengue_activity_rule_version"),
        CheckConstraint("status IN ('draft', 'published', 'archived')", name="ck_antidengue_activity_rule_status"),
        CheckConstraint("classification IN ('review_required')", name="ck_antidengue_activity_rule_classification"),
        CheckConstraint("match_mode IN ('all', 'any')", name="ck_antidengue_activity_rule_match_mode"),
        CheckConstraint("distance_operator IN ('gt', 'gte')", name="ck_antidengue_activity_rule_distance_operator"),
        CheckConstraint("time_operator IN ('between', 'outside')", name="ck_antidengue_activity_rule_time_operator"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    rule_key: uuid.UUID = Field(default_factory=uuid.uuid4, index=True)
    version: int = Field(default=1, ge=1)
    name: str = Field(index=True, min_length=1, max_length=180)
    status: str = Field(default="draft", index=True, max_length=30)
    enabled: bool = Field(default=False, index=True)
    classification: str = Field(default="review_required", index=True, max_length=50)
    match_mode: str = Field(default="all", max_length=10)
    distance_enabled: bool = True
    distance_operator: str = Field(default="gte", max_length=10)
    distance_threshold_meters: float = Field(default=50.0, ge=0, sa_column=Column(Float, nullable=False))
    time_enabled: bool = False
    time_operator: str = Field(default="between", max_length=20)
    time_start: str = Field(default="00:00", max_length=5)
    time_end: str = Field(default="07:00", max_length=5)
    timezone: str = Field(default="Asia/Karachi", max_length=80)
    supersedes_id: uuid.UUID | None = Field(
        default=None,
        sa_column=Column(
            Uuid,
            ForeignKey("antidengue_activity_rules.id", ondelete="SET NULL"),
            nullable=True,
            index=True,
        ),
    )
    created_by: str = Field(default="web-operator", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))
    updated_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))
    published_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))


class AntiDengueSimpleActivityRule(SQLModel, table=True):
    """Versioned minimum before/after-photo interval policy."""

    __tablename__ = "antidengue_simple_activity_rules"
    __table_args__ = (
        UniqueConstraint("rule_key", "version", name="uq_antidengue_simple_activity_rule_version"),
        CheckConstraint("status IN ('draft', 'published', 'archived')", name="ck_antidengue_simple_activity_rule_status"),
        CheckConstraint("operator IN ('lt', 'lte')", name="ck_antidengue_simple_activity_rule_operator"),
    )

    id: uuid.UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    rule_key: uuid.UUID = Field(default_factory=uuid.uuid4, index=True)
    version: int = Field(default=1, ge=1)
    name: str = Field(index=True, min_length=1, max_length=180)
    status: str = Field(default="draft", index=True, max_length=30)
    enabled: bool = Field(default=False, index=True)
    operator: str = Field(default="lt", max_length=10)
    minimum_seconds: int = Field(default=300, ge=1)
    timezone: str = Field(default="Asia/Karachi", max_length=80)
    supersedes_id: uuid.UUID | None = Field(
        default=None,
        sa_column=Column(Uuid, ForeignKey("antidengue_simple_activity_rules.id", ondelete="SET NULL"), nullable=True, index=True),
    )
    created_by: str = Field(default="web-operator", index=True, max_length=120)
    created_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))
    updated_at: datetime = Field(default_factory=utcnow, sa_column=utc_datetime_column(nullable=False))
    published_at: datetime | None = Field(default=None, sa_column=utc_datetime_column(nullable=True))


__all__ = [
    "AntiDengueSchedule",
    "AntiDengueScheduleExecution",
    "AntiDengueScheduleEvent",
    "AntiDengueActivityRule",
    "AntiDengueSimpleActivityRule",
]
