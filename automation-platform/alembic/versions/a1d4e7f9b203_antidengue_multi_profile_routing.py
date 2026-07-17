"""AntiDengue multi-profile routing selections.

Revision ID: a1d4e7f9b203
Revises: f9a2b3c4d5e6
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa

revision = "a1d4e7f9b203"
down_revision = "f9a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for table in ("antidengue_schedules", "antidengue_schedule_executions"):
        op.add_column(table, sa.Column("dispatch_profile_ids", sa.JSON(), nullable=True))
        op.execute(
            sa.text(
                f"UPDATE {table} SET dispatch_profile_ids = "
                "jsonb_build_array(dispatch_profile_id::text) "
                "WHERE dispatch_profile_ids IS NULL"
            )
        )
        op.alter_column(table, "dispatch_profile_ids", nullable=False)


def downgrade() -> None:
    op.drop_column("antidengue_schedule_executions", "dispatch_profile_ids")
    op.drop_column("antidengue_schedules", "dispatch_profile_ids")
