"""add WhatsApp Web history provider marker

Revision ID: 9b7e4c2d1a60
Revises: f53a9c1d7b20
Create Date: 2026-07-16
"""

from alembic import op
import sqlalchemy as sa

revision = "9b7e4c2d1a60"
down_revision = "f53a9c1d7b20"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("whatsapp_inbound_history_requests") as batch_op:
        batch_op.add_column(
            sa.Column("provider", sa.String(), nullable=False, server_default="baileys")
        )
        batch_op.create_index(
            "ix_whatsapp_inbound_history_requests_provider",
            ["provider"],
            unique=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("whatsapp_inbound_history_requests") as batch_op:
        batch_op.drop_index("ix_whatsapp_inbound_history_requests_provider")
        batch_op.drop_column("provider")
