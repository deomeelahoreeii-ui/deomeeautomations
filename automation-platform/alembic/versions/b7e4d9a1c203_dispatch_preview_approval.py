"""dispatch preview approval and idempotent delivery queue

Revision ID: b7e4d9a1c203
Revises: e6b7c8d9f012
Create Date: 2026-07-14 10:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
import sqlmodel

revision: str = "b7e4d9a1c203"
down_revision: str | None = "e6b7c8d9f012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_dispatch_approvals",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("preview_id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("approved_by", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("warnings_acknowledged", sa.Boolean(), nullable=False),
        sa.Column("preview_content_sha256", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("delivery_count", sa.Integer(), nullable=False),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('queued', 'sending', 'completed', 'failed')",
            name="ck_whatsapp_dispatch_approvals_status",
        ),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.ForeignKeyConstraint(["preview_id"], ["whatsapp_dispatch_previews.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column, unique in (
        ("preview_id", True), ("job_id", True), ("approved_by", False),
        ("preview_content_sha256", False), ("status", False),
        ("approved_at", False), ("completed_at", False),
    ):
        op.create_index(
            op.f(f"ix_whatsapp_dispatch_approvals_{column}"),
            "whatsapp_dispatch_approvals", [column], unique=unique,
        )

    op.add_column("whatsapp_deliveries", sa.Column("approval_id", sa.Uuid(), nullable=True))
    op.add_column("whatsapp_deliveries", sa.Column("preview_delivery_id", sa.Uuid(), nullable=True))
    op.add_column("whatsapp_deliveries", sa.Column("attachments", sa.JSON(), nullable=True))
    op.create_foreign_key(
        "fk_whatsapp_deliveries_approval_id", "whatsapp_deliveries",
        "whatsapp_dispatch_approvals", ["approval_id"], ["id"],
    )
    op.create_foreign_key(
        "fk_whatsapp_deliveries_preview_delivery_id", "whatsapp_deliveries",
        "whatsapp_dispatch_preview_deliveries", ["preview_delivery_id"], ["id"],
    )
    op.create_index(op.f("ix_whatsapp_deliveries_approval_id"), "whatsapp_deliveries", ["approval_id"])
    op.create_index(
        op.f("ix_whatsapp_deliveries_preview_delivery_id"),
        "whatsapp_deliveries", ["preview_delivery_id"], unique=True,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_whatsapp_deliveries_preview_delivery_id"), table_name="whatsapp_deliveries")
    op.drop_index(op.f("ix_whatsapp_deliveries_approval_id"), table_name="whatsapp_deliveries")
    op.drop_constraint("fk_whatsapp_deliveries_preview_delivery_id", "whatsapp_deliveries", type_="foreignkey")
    op.drop_constraint("fk_whatsapp_deliveries_approval_id", "whatsapp_deliveries", type_="foreignkey")
    op.drop_column("whatsapp_deliveries", "attachments")
    op.drop_column("whatsapp_deliveries", "preview_delivery_id")
    op.drop_column("whatsapp_deliveries", "approval_id")
    op.drop_table("whatsapp_dispatch_approvals")
