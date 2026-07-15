"""source file intake

Revision ID: f2b6c8d1e304
Revises: c7a8d2e4f901
Create Date: 2026-07-13 20:18:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f2b6c8d1e304"
down_revision: str | None = "c7a8d2e4f901"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "source_files",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("module_key", sa.String(), nullable=False),
        sa.Column("source_kind", sa.String(), nullable=False),
        sa.Column("original_name", sa.String(), nullable=False),
        sa.Column("stored_path", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(), nullable=True),
        sa.Column("extension", sa.String(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("validation_status", sa.String(), nullable=False),
        sa.Column("schema_version", sa.String(), nullable=True),
        sa.Column("detected_metadata", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("validation_errors", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("validation_warnings", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("duplicate_of_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["duplicate_of_id"], ["source_files.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in ("module_key", "source_kind", "extension", "sha256", "validation_status", "duplicate_of_id", "created_at"):
        op.create_index(f"ix_source_files_{column}", "source_files", [column])

    op.create_table(
        "source_file_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_file_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.ForeignKeyConstraint(["source_file_id"], ["source_files.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_source_file_runs_source_file_id", "source_file_runs", ["source_file_id"])
    op.create_index("ix_source_file_runs_job_id", "source_file_runs", ["job_id"], unique=True)
    op.create_index("ix_source_file_runs_created_at", "source_file_runs", ["created_at"])


def downgrade() -> None:
    op.drop_table("source_file_runs")
    op.drop_table("source_files")
