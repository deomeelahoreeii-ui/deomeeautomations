"""Allow one source document, such as a sheet, to contain many complaint cases.

Revision ID: c2e4f6a8b103
Revises: b8d2f4a6c901
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa


revision = "c2e4f6a8b103"
down_revision = "b8d2f4a6c901"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "crm_complaint_document_case_links",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_document_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("source_locator", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["complaint_document_id"], ["crm_complaint_documents.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "complaint_document_id", "complaint_case_id", "source_locator",
            name="uq_crm_document_case_source",
        ),
    )
    for column in (
        "complaint_document_id", "complaint_case_id", "role", "source_locator", "created_at"
    ):
        op.create_index(
            f"ix_crm_complaint_document_case_links_{column}",
            "crm_complaint_document_case_links",
            [column],
        )

    # Existing singular relationships also participate in the uniform graph.
    op.execute(
        """
        INSERT INTO crm_complaint_document_case_links (
            id, complaint_document_id, complaint_case_id, role, confidence,
            reason, source_locator, created_at
        )
        SELECT gen_random_uuid(), id, complaint_case_id, role,
               relationship_confidence, relationship_reason, 'document', created_at
        FROM crm_complaint_documents
        WHERE complaint_case_id IS NOT NULL
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_table("crm_complaint_document_case_links")
