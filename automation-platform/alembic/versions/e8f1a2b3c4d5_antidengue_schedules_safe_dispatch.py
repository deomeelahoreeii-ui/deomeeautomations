"""AntiDengue schedules and safe dry-run preview/send orchestration.

Revision ID: e8f1a2b3c4d5
Revises: d7c9e4a1b620
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa

revision = "e8f1a2b3c4d5"
down_revision = "d7c9e4a1b620"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "antidengue_schedules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=180), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("recurrence_type", sa.String(), nullable=False),
        sa.Column("run_date", sa.Date(), nullable=True),
        sa.Column("weekdays", sa.JSON(), nullable=False),
        sa.Column("times", sa.JSON(), nullable=False),
        sa.Column("timezone", sa.String(length=80), nullable=False),
        sa.Column("login_mode", sa.String(), nullable=False),
        sa.Column("dispatch_policy", sa.String(), nullable=False),
        sa.Column("dispatch_profile_id", sa.Uuid(), nullable=False),
        sa.Column("missed_run_grace_minutes", sa.Integer(), nullable=False),
        sa.Column("overlap_grace_minutes", sa.Integer(), nullable=False),
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_status", sa.String(length=80), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "recurrence_type IN ('once', 'daily', 'weekly')",
            name="ck_antidengue_schedules_recurrence_type",
        ),
        sa.CheckConstraint(
            "login_mode IN ('auto', 'manual', 'remote_approve')",
            name="ck_antidengue_schedules_login_mode",
        ),
        sa.CheckConstraint(
            "dispatch_policy IN ('preview_only', 'auto_send_when_clean')",
            name="ck_antidengue_schedules_dispatch_policy",
        ),
        sa.ForeignKeyConstraint(
            ["dispatch_profile_id"], ["whatsapp_dispatch_profiles.id"]
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in [
        "name", "enabled", "recurrence_type", "run_date", "timezone", "login_mode",
        "dispatch_policy", "dispatch_profile_id", "next_run_at", "last_run_at",
        "last_run_status", "archived_at", "created_by", "created_at", "updated_at",
    ]:
        op.create_index(f"ix_antidengue_schedules_{column}", "antidengue_schedules", [column])

    op.create_table(
        "antidengue_schedule_executions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("execution_key", sa.String(length=220), nullable=False),
        sa.Column("execution_code", sa.String(length=80), nullable=False),
        sa.Column("schedule_id", sa.Uuid(), nullable=True),
        sa.Column("trigger_type", sa.String(), nullable=False),
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("dispatch_policy", sa.String(), nullable=False),
        sa.Column("login_mode", sa.String(), nullable=False),
        sa.Column("dispatch_profile_id", sa.Uuid(), nullable=False),
        sa.Column("overlap_deadline_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_job_id", sa.Uuid(), nullable=True),
        sa.Column("preview_job_id", sa.Uuid(), nullable=True),
        sa.Column("preview_id", sa.Uuid(), nullable=True),
        sa.Column("send_job_id", sa.Uuid(), nullable=True),
        sa.Column("source_summary", sa.JSON(), nullable=False),
        sa.Column("preview_summary", sa.JSON(), nullable=False),
        sa.Column("dispatch_summary", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "trigger_type IN ('scheduled', 'manual_preview', 'manual_send', 'schedule_run_now')",
            name="ck_antidengue_schedule_executions_trigger_type",
        ),
        sa.CheckConstraint(
            "dispatch_policy IN ('preview_only', 'auto_send_when_clean')",
            name="ck_antidengue_schedule_executions_dispatch_policy",
        ),
        sa.CheckConstraint(
            "status IN ('due', 'waiting_overlap', 'dry_run_queued', 'dry_run_running', "
            "'preview_queued', 'preview_compiling', 'preview_ready', 'auto_approving', "
            "'dispatch_queued', 'dispatch_running', 'completed', "
            "'completed_with_delivery_errors', 'blocked', 'failed', 'skipped', 'cancelled')",
            name="ck_antidengue_schedule_executions_status",
        ),
        sa.ForeignKeyConstraint(
            ["schedule_id"], ["antidengue_schedules.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["dispatch_profile_id"], ["whatsapp_dispatch_profiles.id"]
        ),
        sa.ForeignKeyConstraint(["source_job_id"], ["jobs.id"]),
        sa.ForeignKeyConstraint(["preview_job_id"], ["jobs.id"]),
        sa.ForeignKeyConstraint(["preview_id"], ["whatsapp_dispatch_previews.id"]),
        sa.ForeignKeyConstraint(["send_job_id"], ["jobs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("execution_key", name="uq_antidengue_schedule_execution_key"),
        sa.UniqueConstraint("execution_code"),
    )
    for column in [
        "execution_key", "execution_code", "schedule_id", "trigger_type", "scheduled_for",
        "status", "dispatch_policy", "login_mode", "dispatch_profile_id",
        "overlap_deadline_at", "source_job_id", "preview_job_id", "preview_id",
        "send_job_id", "created_by", "created_at", "started_at", "finished_at", "updated_at",
    ]:
        op.create_index(
            f"ix_antidengue_schedule_executions_{column}",
            "antidengue_schedule_executions",
            [column],
        )

    op.create_table(
        "antidengue_schedule_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("execution_id", sa.Uuid(), nullable=False),
        sa.Column("level", sa.String(length=30), nullable=False),
        sa.Column("event_type", sa.String(length=80), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["execution_id"], ["antidengue_schedule_executions.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in ["execution_id", "level", "event_type", "created_at"]:
        op.create_index(
            f"ix_antidengue_schedule_events_{column}",
            "antidengue_schedule_events",
            [column],
        )


def downgrade() -> None:
    op.drop_table("antidengue_schedule_events")
    op.drop_table("antidengue_schedule_executions")
    op.drop_table("antidengue_schedules")
