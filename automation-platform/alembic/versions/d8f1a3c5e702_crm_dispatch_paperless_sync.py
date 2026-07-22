"""Add durable CRM dispatch to Paperless status synchronization.

Revision ID: d8f1a3c5e702
Revises: c5e7a9b1d324
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "d8f1a3c5e702"
down_revision = "c5e7a9b1d324"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crm_paperless_status_syncs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_item_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("paperless_document_id", sa.Integer(), nullable=True),
        sa.Column("intended_status", sa.String(length=80), nullable=False),
        sa.Column("state", sa.String(length=24), nullable=False),
        sa.Column("observed_status_before", sa.String(length=80), nullable=True),
        sa.Column("observed_status_after", sa.String(length=80), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_attempted_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "state IN ('pending', 'syncing', 'succeeded', 'failed', 'blocked')",
            name="ck_crm_paperless_status_syncs_state",
        ),
        sa.ForeignKeyConstraint(
            ["dispatch_item_id"], ["crm_dispatch_items.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["complaint_case_id"], ["crm_complaint_cases.id"]
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "dispatch_item_id",
            name="uq_crm_paperless_status_syncs_dispatch_item",
        ),
    )
    for column in (
        "complaint_case_id",
        "paperless_document_id",
        "intended_status",
        "state",
        "observed_status_before",
        "observed_status_after",
        "last_attempted_at",
        "completed_at",
        "created_at",
        "updated_at",
    ):
        op.create_index(
            f"ix_crm_paperless_status_syncs_{column}",
            "crm_paperless_status_syncs",
            [column],
        )


def downgrade() -> None:
    op.drop_table("crm_paperless_status_syncs")
