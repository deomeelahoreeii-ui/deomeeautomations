"""Make RustFS authoritative for AntiDengue artifacts and runtime state.

Revision ID: a5c7e9b1d302
Revises: f4a6c8e0b215
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a5c7e9b1d302"
down_revision = "f4a6c8e0b215"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "antidengue_runtime_state",
        sa.Column("state_key", sa.String(length=120), nullable=False),
        sa.Column("value_json", sa.JSON(), nullable=False),
        sa.Column("updated_by_job_id", sa.Uuid(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["updated_by_job_id"],
            ["jobs.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("state_key"),
    )
    op.create_index(
        "ix_antidengue_runtime_state_updated_by_job_id",
        "antidengue_runtime_state",
        ["updated_by_job_id"],
        unique=False,
    )
    op.create_index(
        "ix_antidengue_runtime_state_updated_at",
        "antidengue_runtime_state",
        ["updated_at"],
        unique=False,
    )
    op.add_column(
        "artifacts",
        sa.Column("local_evicted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "artifacts",
        sa.Column("last_hydrated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_artifacts_local_evicted_at",
        "artifacts",
        ["local_evicted_at"],
        unique=False,
    )
    op.create_index(
        "ix_artifacts_last_hydrated_at",
        "artifacts",
        ["last_hydrated_at"],
        unique=False,
    )
    op.add_column(
        "source_files",
        sa.Column("local_evicted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "source_files",
        sa.Column("last_hydrated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_source_files_local_evicted_at",
        "source_files",
        ["local_evicted_at"],
        unique=False,
    )
    op.create_index(
        "ix_source_files_last_hydrated_at",
        "source_files",
        ["last_hydrated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_source_files_last_hydrated_at", table_name="source_files")
    op.drop_index("ix_source_files_local_evicted_at", table_name="source_files")
    op.drop_column("source_files", "last_hydrated_at")
    op.drop_column("source_files", "local_evicted_at")
    op.drop_index("ix_artifacts_last_hydrated_at", table_name="artifacts")
    op.drop_index("ix_artifacts_local_evicted_at", table_name="artifacts")
    op.drop_column("artifacts", "last_hydrated_at")
    op.drop_column("artifacts", "local_evicted_at")
    op.drop_index(
        "ix_antidengue_runtime_state_updated_at",
        table_name="antidengue_runtime_state",
    )
    op.drop_index(
        "ix_antidengue_runtime_state_updated_by_job_id",
        table_name="antidengue_runtime_state",
    )
    op.drop_table("antidengue_runtime_state")
