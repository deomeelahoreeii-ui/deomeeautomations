"""Durable worker runtime capabilities.

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e6f7a8b9c0d1"
down_revision: str | None = "d5e6f7a8b9c0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "automation_worker_runtimes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("worker_name", sa.String(length=180), nullable=False),
        sa.Column("queues", sa.JSON(), nullable=False),
        sa.Column("protocols", sa.JSON(), nullable=False),
        sa.Column("capabilities", sa.JSON(), nullable=False),
        sa.Column("capability_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("build_id", sa.String(length=180), nullable=False),
        sa.Column("database_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("worker_name"),
    )
    for column in (
        "worker_name", "capability_fingerprint", "build_id",
        "database_fingerprint", "last_seen_at",
    ):
        op.create_index(
            f"ix_automation_worker_runtimes_{column}",
            "automation_worker_runtimes", [column],
        )


def downgrade() -> None:
    op.drop_table("automation_worker_runtimes")
