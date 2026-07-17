"""Preserve bounded versus all-history intake scope.

Revision ID: a6e8c2d4f701
Revises: c5d7e9f1a302
"""

from alembic import op
import sqlalchemy as sa


revision = "a6e8c2d4f701"
down_revision = "c5d7e9f1a302"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "whatsapp_inbound_history_requests",
        sa.Column("all_history", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "whatsapp_inbound_batches",
        sa.Column("all_history", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("whatsapp_inbound_batches", "all_history")
    op.drop_column("whatsapp_inbound_history_requests", "all_history")
