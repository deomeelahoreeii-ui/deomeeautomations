"""Allow unapproved preview deletion while preserving AntiDengue executions.

Revision ID: b3e5f7a9c102
Revises: a1d4e7f9b203
Create Date: 2026-07-17
"""

from alembic import op


revision = "b3e5f7a9c102"
down_revision = "a1d4e7f9b203"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "antidengue_schedule_executions_preview_id_fkey",
        "antidengue_schedule_executions",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "antidengue_schedule_executions_preview_id_fkey",
        "antidengue_schedule_executions",
        "whatsapp_dispatch_previews",
        ["preview_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "antidengue_schedule_executions_preview_id_fkey",
        "antidengue_schedule_executions",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "antidengue_schedule_executions_preview_id_fkey",
        "antidengue_schedule_executions",
        "whatsapp_dispatch_previews",
        ["preview_id"],
        ["id"],
    )
