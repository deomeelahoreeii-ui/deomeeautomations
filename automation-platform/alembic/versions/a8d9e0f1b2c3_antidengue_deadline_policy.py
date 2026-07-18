"""Database-backed AntiDengue submission deadline policy.

Revision ID: a8d9e0f1b2c3
Revises: f7c8d9e0a1b2
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a8d9e0f1b2c3"
down_revision: str | None = "f7c8d9e0a1b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "antidengue_deadline_policies",
        sa.Column("policy_key", sa.String(length=80), nullable=False),
        sa.Column("submission_deadline", sa.String(length=5), nullable=False),
        sa.Column("timezone", sa.String(length=80), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("updated_by", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("policy_key"),
    )
    op.execute(sa.text("""
        INSERT INTO antidengue_deadline_policies
            (policy_key, submission_deadline, timezone, version, updated_by, created_at, updated_at)
        VALUES
            ('default', '12:30', 'Asia/Karachi', 1, 'database-migration', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """))
    op.add_column(
        "antidengue_schedules",
        sa.Column("submission_deadline_override", sa.String(length=5), nullable=True),
    )
    op.add_column(
        "antidengue_schedule_executions",
        sa.Column("submission_deadline", sa.String(length=5), nullable=False, server_default="12:30"),
    )
    op.add_column(
        "antidengue_schedule_executions",
        sa.Column("submission_deadline_label", sa.String(length=20), nullable=False, server_default="12:30 PM"),
    )
    op.add_column(
        "antidengue_schedule_executions",
        sa.Column("deadline_timezone", sa.String(length=80), nullable=False, server_default="Asia/Karachi"),
    )
    op.add_column(
        "antidengue_schedule_executions",
        sa.Column("deadline_policy_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "antidengue_schedule_executions",
        sa.Column("deadline_source", sa.String(length=30), nullable=False, server_default="global"),
    )


def downgrade() -> None:
    op.drop_column("antidengue_schedule_executions", "deadline_source")
    op.drop_column("antidengue_schedule_executions", "deadline_policy_version")
    op.drop_column("antidengue_schedule_executions", "deadline_timezone")
    op.drop_column("antidengue_schedule_executions", "submission_deadline_label")
    op.drop_column("antidengue_schedule_executions", "submission_deadline")
    op.drop_column("antidengue_schedules", "submission_deadline_override")
    op.drop_table("antidengue_deadline_policies")
