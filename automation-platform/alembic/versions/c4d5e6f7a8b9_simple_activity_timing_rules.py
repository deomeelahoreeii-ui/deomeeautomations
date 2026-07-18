"""Simple Activity timing rules.

Revision ID: c4d5e6f7a8b9
Revises: b3c4d5e6f7a8
"""
from collections.abc import Sequence
from datetime import UTC, datetime
import uuid
import sqlalchemy as sa
from alembic import op

revision: str = "c4d5e6f7a8b9"
down_revision: str | None = "b3c4d5e6f7a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "antidengue_simple_activity_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("rule_key", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("name", sa.String(180), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="draft"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("operator", sa.String(10), nullable=False, server_default="lt"),
        sa.Column("minimum_seconds", sa.Integer(), nullable=False, server_default="300"),
        sa.Column("timezone", sa.String(80), nullable=False, server_default="Asia/Karachi"),
        sa.Column("supersedes_id", sa.Uuid(), nullable=True),
        sa.Column("created_by", sa.String(120), nullable=False, server_default="web-operator"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('draft', 'published', 'archived')", name="ck_antidengue_simple_activity_rule_status"),
        sa.CheckConstraint("operator IN ('lt', 'lte')", name="ck_antidengue_simple_activity_rule_operator"),
        sa.ForeignKeyConstraint(["supersedes_id"], ["antidengue_simple_activity_rules.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("rule_key", "version", name="uq_antidengue_simple_activity_rule_version"),
    )
    for column in ("rule_key", "name", "status", "enabled", "supersedes_id", "created_by", "created_at", "updated_at", "published_at"):
        op.create_index(f"ix_antidengue_simple_activity_rules_{column}", "antidengue_simple_activity_rules", [column])
    rules = sa.table(
        "antidengue_simple_activity_rules",
        sa.column("id", sa.Uuid()), sa.column("rule_key", sa.Uuid()),
        sa.column("version", sa.Integer()), sa.column("name", sa.String()),
        sa.column("status", sa.String()), sa.column("enabled", sa.Boolean()),
        sa.column("operator", sa.String()), sa.column("minimum_seconds", sa.Integer()),
        sa.column("timezone", sa.String()), sa.column("created_by", sa.String()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
        sa.column("published_at", sa.DateTime(timezone=True)),
    )
    now = datetime.now(UTC)
    op.bulk_insert(rules, [{
        "id": uuid.UUID("63cba0fb-1131-4ccf-b3b7-3104c3b01b01"),
        "rule_key": uuid.UUID("63cba0fb-1131-4ccf-b3b7-3104c3b01b00"),
        "version": 1, "name": "Minimum five-minute before/after interval",
        "status": "published", "enabled": True, "operator": "lt",
        "minimum_seconds": 300, "timezone": "Asia/Karachi",
        "created_by": "platform-default", "created_at": now,
        "updated_at": now, "published_at": now,
    }])


def downgrade() -> None:
    op.drop_table("antidengue_simple_activity_rules")
