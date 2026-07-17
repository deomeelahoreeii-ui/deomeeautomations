"""Add durable CRM complaint replies.

Revision ID: e7a1c3d5f902
Revises: d4f6a8c0e205
Create Date: 2026-07-18
"""

from alembic import op
import sqlalchemy as sa


revision = "e7a1c3d5f902"
down_revision = "d4f6a8c0e205"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crm_complaint_replies",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("reply_text", sa.Text(), nullable=False),
        sa.Column("source_filename", sa.String(), nullable=False),
        sa.Column("source_row", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("imported_at", sa.DateTime(), nullable=False),
        sa.Column("generated_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("complaint_case_id", name="uq_crm_reply_case"),
    )
    for column in ("complaint_case_id", "source_filename", "imported_at", "generated_at", "updated_at"):
        op.create_index(f"ix_crm_complaint_replies_{column}", "crm_complaint_replies", [column])


def downgrade() -> None:
    op.drop_table("crm_complaint_replies")
