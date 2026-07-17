"""Durable task outbox for AntiDengue orchestration reliability.

Revision ID: f9a2b3c4d5e6
Revises: e8f1a2b3c4d5
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa

revision = "f9a2b3c4d5e6"
down_revision = "e8f1a2b3c4d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("task_name", sa.String(length=180), nullable=False),
        sa.Column("queue", sa.String(length=80), nullable=False),
        sa.Column("args", sa.JSON(), nullable=False),
        sa.Column("kwargs", sa.JSON(), nullable=False),
        sa.Column("task_id", sa.String(length=80), nullable=False),
        sa.Column("idempotency_key", sa.String(length=240), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'publishing', 'published', 'failed', 'cancelled')",
            name="ck_task_outbox_status",
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_task_outbox_idempotency_key"),
        sa.UniqueConstraint("task_id", name="uq_task_outbox_task_id"),
    )
    for column in [
        "job_id", "task_name", "queue", "task_id", "idempotency_key", "status",
        "available_at", "locked_at", "published_at", "created_at", "updated_at",
    ]:
        op.create_index(f"ix_task_outbox_{column}", "task_outbox", [column])


def downgrade() -> None:
    op.drop_table("task_outbox")
