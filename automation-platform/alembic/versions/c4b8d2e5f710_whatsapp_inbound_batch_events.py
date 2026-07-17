"""WhatsApp inbound batch activity events.

Revision ID: c4b8d2e5f710
Revises: b4a7c1d2e390
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa

revision = "c4b8d2e5f710"
down_revision = "b4a7c1d2e390"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_inbound_batch_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("batch_item_id", sa.Uuid(), nullable=True),
        sa.Column("level", sa.String(), nullable=False),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["batch_id"], ["whatsapp_inbound_batches.id"]),
        sa.ForeignKeyConstraint(["batch_item_id"], ["whatsapp_inbound_batch_items.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in ["batch_id", "batch_item_id", "level", "event_type", "created_at"]:
        op.create_index(
            f"ix_whatsapp_inbound_batch_events_{column}",
            "whatsapp_inbound_batch_events",
            [column],
        )


def downgrade() -> None:
    op.drop_table("whatsapp_inbound_batch_events")
