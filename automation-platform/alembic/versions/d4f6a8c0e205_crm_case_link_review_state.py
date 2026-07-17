"""Store review decisions on each complaint-document relationship.

Revision ID: d4f6a8c0e205
Revises: c2e4f6a8b103
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa


revision = "d4f6a8c0e205"
down_revision = "c2e4f6a8b103"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "crm_complaint_document_case_links",
        sa.Column("review_state", sa.String(), nullable=False, server_default="pending"),
    )
    op.create_index(
        "ix_crm_complaint_document_case_links_review_state",
        "crm_complaint_document_case_links",
        ["review_state"],
    )
    op.execute(
        """
        UPDATE crm_complaint_document_case_links AS link
        SET review_state = document.review_state
        FROM crm_complaint_documents AS document
        WHERE document.id = link.complaint_document_id
        """
    )
    op.alter_column(
        "crm_complaint_document_case_links", "review_state", server_default=None
    )


def downgrade() -> None:
    op.drop_index(
        "ix_crm_complaint_document_case_links_review_state",
        table_name="crm_complaint_document_case_links",
    )
    op.drop_column("crm_complaint_document_case_links", "review_state")
