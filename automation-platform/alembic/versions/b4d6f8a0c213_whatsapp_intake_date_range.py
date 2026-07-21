"""Add received-only and date-range scope to WhatsApp complaint intake.

Revision ID: b4d6f8a0c213
Revises: f1c3e5a7b902
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "b4d6f8a0c213"
down_revision = "f1c3e5a7b902"
branch_labels = None
depends_on = None


def _add_scope_columns(table: str) -> None:
    op.add_column(table, sa.Column("date_from", sa.DateTime(), nullable=True))
    op.add_column(table, sa.Column("date_to", sa.DateTime(), nullable=True))
    op.add_column(
        table,
        sa.Column("received_only", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.create_index(f"ix_{table}_date_from", table, ["date_from"])
    op.create_index(f"ix_{table}_date_to", table, ["date_to"])


def _drop_scope_columns(table: str) -> None:
    op.drop_index(f"ix_{table}_date_to", table_name=table)
    op.drop_index(f"ix_{table}_date_from", table_name=table)
    op.drop_column(table, "received_only")
    op.drop_column(table, "date_to")
    op.drop_column(table, "date_from")


def upgrade() -> None:
    _add_scope_columns("whatsapp_inbound_history_requests")
    _add_scope_columns("whatsapp_inbound_batches")


def downgrade() -> None:
    _drop_scope_columns("whatsapp_inbound_batches")
    _drop_scope_columns("whatsapp_inbound_history_requests")
