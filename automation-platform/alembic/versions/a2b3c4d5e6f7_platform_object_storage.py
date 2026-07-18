"""platform object catalog and durable artifact references

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-07-18 12:30:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a2b3c4d5e6f7"
down_revision: str | None = "f1a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "stored_objects",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("backend", sa.String(), nullable=False, server_default="s3"),
        sa.Column("bucket", sa.String(), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("content_type", sa.String(), nullable=True),
        sa.Column("etag", sa.String(), nullable=True),
        sa.Column("version_id", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="ready"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("verified_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint("status IN ('ready', 'error')", name="ck_stored_objects_status"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("backend", "bucket", "object_key", name="uq_stored_objects_location"),
    )
    for column in ("backend", "bucket", "sha256", "status", "created_at", "verified_at"):
        op.create_index(f"ix_stored_objects_{column}", "stored_objects", [column])

    op.add_column("artifacts", sa.Column("module_key", sa.String(), nullable=False, server_default="legacy"))
    op.add_column("artifacts", sa.Column("sha256", sa.String(length=64), nullable=False, server_default=""))
    op.add_column("artifacts", sa.Column("content_type", sa.String(), nullable=True))
    op.add_column("artifacts", sa.Column("stored_object_id", sa.Uuid(), nullable=True))
    op.add_column("artifacts", sa.Column("storage_status", sa.String(), nullable=False, server_default="local"))
    op.add_column("artifacts", sa.Column("storage_error", sa.Text(), nullable=True))
    op.add_column("artifacts", sa.Column("archived_at", sa.DateTime(), nullable=True))
    op.create_foreign_key("fk_artifacts_stored_object", "artifacts", "stored_objects", ["stored_object_id"], ["id"], ondelete="SET NULL")
    for column in ("module_key", "sha256", "stored_object_id", "storage_status", "archived_at"):
        op.create_index(f"ix_artifacts_{column}", "artifacts", [column])
    op.execute("UPDATE artifacts SET module_key = CASE WHEN jobs.type LIKE 'antidengue.%' THEN 'antidengue' WHEN jobs.type LIKE 'crm.%' THEN 'crm' WHEN jobs.type LIKE 'whatsapp.%' THEN 'whatsapp' ELSE 'legacy' END FROM jobs WHERE artifacts.job_id = jobs.id")

    for table in ("source_files",):
        op.add_column(table, sa.Column("stored_object_id", sa.Uuid(), nullable=True))
        op.add_column(table, sa.Column("storage_status", sa.String(), nullable=False, server_default="local"))
        op.add_column(table, sa.Column("storage_error", sa.Text(), nullable=True))
        op.add_column(table, sa.Column("archived_at", sa.DateTime(), nullable=True))
        op.create_foreign_key(f"fk_{table}_stored_object", table, "stored_objects", ["stored_object_id"], ["id"], ondelete="SET NULL")
        for column in ("stored_object_id", "storage_status", "archived_at"):
            op.create_index(f"ix_{table}_{column}", table, [column])

    table = "whatsapp_dispatch_preview_artifacts"
    op.add_column(table, sa.Column("stored_object_id", sa.Uuid(), nullable=True))
    op.add_column(table, sa.Column("storage_status", sa.String(), nullable=False, server_default="local"))
    op.add_column(table, sa.Column("storage_error", sa.Text(), nullable=True))
    op.create_foreign_key("fk_preview_artifacts_stored_object", table, "stored_objects", ["stored_object_id"], ["id"], ondelete="SET NULL")
    op.create_index("ix_whatsapp_dispatch_preview_artifacts_stored_object_id", table, ["stored_object_id"])
    op.create_index("ix_whatsapp_dispatch_preview_artifacts_storage_status", table, ["storage_status"])


def downgrade() -> None:
    table = "whatsapp_dispatch_preview_artifacts"
    op.drop_index("ix_whatsapp_dispatch_preview_artifacts_storage_status", table_name=table)
    op.drop_index("ix_whatsapp_dispatch_preview_artifacts_stored_object_id", table_name=table)
    op.drop_constraint("fk_preview_artifacts_stored_object", table, type_="foreignkey")
    for column in ("storage_error", "storage_status", "stored_object_id"):
        op.drop_column(table, column)
    for table in ("source_files",):
        for column in ("archived_at", "storage_status", "stored_object_id"):
            op.drop_index(f"ix_{table}_{column}", table_name=table)
        op.drop_constraint(f"fk_{table}_stored_object", table, type_="foreignkey")
        for column in ("archived_at", "storage_error", "storage_status", "stored_object_id"):
            op.drop_column(table, column)
    for column in ("archived_at", "storage_status", "stored_object_id", "sha256", "module_key"):
        op.drop_index(f"ix_artifacts_{column}", table_name="artifacts")
    op.drop_constraint("fk_artifacts_stored_object", "artifacts", type_="foreignkey")
    for column in ("archived_at", "storage_error", "storage_status", "stored_object_id", "content_type", "sha256", "module_key"):
        op.drop_column("artifacts", column)
    op.drop_table("stored_objects")
