"""versioned AntiDengue activity classification rules

Revision ID: b3c4d5e6f7a8
Revises: a2b3c4d5e6f7
Create Date: 2026-07-18 14:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b3c4d5e6f7a8"
down_revision: str | None = "a2b3c4d5e6f7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "antidengue_activity_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("rule_key", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("name", sa.String(length=180), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="draft"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("classification", sa.String(length=50), nullable=False, server_default="review_required"),
        sa.Column("match_mode", sa.String(length=10), nullable=False, server_default="all"),
        sa.Column("distance_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("distance_operator", sa.String(length=10), nullable=False, server_default="gte"),
        sa.Column("distance_threshold_meters", sa.Float(), nullable=False, server_default="50"),
        sa.Column("time_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("time_operator", sa.String(length=20), nullable=False, server_default="between"),
        sa.Column("time_start", sa.String(length=5), nullable=False, server_default="00:00"),
        sa.Column("time_end", sa.String(length=5), nullable=False, server_default="07:00"),
        sa.Column("timezone", sa.String(length=80), nullable=False, server_default="Asia/Karachi"),
        sa.Column("supersedes_id", sa.Uuid(), nullable=True),
        sa.Column("created_by", sa.String(length=120), nullable=False, server_default="web-operator"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('draft', 'published', 'archived')", name="ck_antidengue_activity_rule_status"),
        sa.CheckConstraint("classification IN ('review_required')", name="ck_antidengue_activity_rule_classification"),
        sa.CheckConstraint("match_mode IN ('all', 'any')", name="ck_antidengue_activity_rule_match_mode"),
        sa.CheckConstraint("distance_operator IN ('gt', 'gte')", name="ck_antidengue_activity_rule_distance_operator"),
        sa.CheckConstraint("time_operator IN ('between', 'outside')", name="ck_antidengue_activity_rule_time_operator"),
        sa.ForeignKeyConstraint(["supersedes_id"], ["antidengue_activity_rules.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("rule_key", "version", name="uq_antidengue_activity_rule_version"),
    )
    for column in (
        "rule_key", "name", "status", "enabled", "classification", "supersedes_id",
        "created_by", "created_at", "updated_at", "published_at",
    ):
        op.create_index(
            f"ix_antidengue_activity_rules_{column}",
            "antidengue_activity_rules",
            [column],
        )


def downgrade() -> None:
    op.drop_table("antidengue_activity_rules")
