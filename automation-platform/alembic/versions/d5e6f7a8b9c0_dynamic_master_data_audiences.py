"""Dynamic Master Data audience sources.

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d5e6f7a8b9c0"
down_revision: str | None = "c4d5e6f7a8b9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_audience_sources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("audience_id", sa.Uuid(), nullable=False),
        sa.Column("source_type", sa.String(), nullable=False),
        sa.Column("recipient_role", sa.String(), nullable=False),
        sa.Column("wing_id", sa.Uuid(), nullable=False),
        sa.Column("route_scope_key", sa.String(), nullable=False),
        sa.Column("aggregate_by_recipient", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("source_type IN ('master_data_jurisdictions')", name="ck_whatsapp_audience_sources_type"),
        sa.CheckConstraint("recipient_role IN ('aeo', 'ddeo')", name="ck_whatsapp_audience_sources_role"),
        sa.CheckConstraint("route_scope_key IN ('markaz', 'tehsil')", name="ck_whatsapp_audience_sources_scope"),
        sa.ForeignKeyConstraint(["audience_id"], ["whatsapp_audiences.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["wing_id"], ["wings.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("audience_id", "source_type", "recipient_role", "wing_id", "route_scope_key", name="uq_whatsapp_audience_sources_selector"),
    )
    for column in ("audience_id", "source_type", "recipient_role", "wing_id", "route_scope_key", "enabled"):
        op.create_index(f"ix_whatsapp_audience_sources_{column}", "whatsapp_audience_sources", [column])


def downgrade() -> None:
    op.drop_table("whatsapp_audience_sources")
