"""Add durable exact-content duplicate decisions to CRM intake.

Revision ID: c5e7a9b1d324
Revises: b4d6f8a0c213
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "c5e7a9b1d324"
down_revision = "b4d6f8a0c213"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "whatsapp_inbound_processing_runs",
        sa.Column(
            "content_duplicate_items",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )
    op.add_column(
        "whatsapp_inbound_processing_items",
        sa.Column("normalized_content_sha256", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "whatsapp_inbound_processing_items",
        sa.Column("content_match_kind", sa.String(length=32), nullable=True),
    )
    op.add_column(
        "whatsapp_inbound_processing_items",
        sa.Column("canonical_processing_item_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "whatsapp_inbound_processing_items",
        sa.Column(
            "content_match_details_json",
            sa.JSON(),
            nullable=False,
            server_default=sa.text("'{}'::json"),
        ),
    )
    op.create_check_constraint(
        "ck_whatsapp_inbound_processing_items_content_match_kind",
        "whatsapp_inbound_processing_items",
        "content_match_kind IS NULL OR content_match_kind IN ('exact_reused', 'exact_pending', 'exact_conflict', 'normalized_reused', 'normalized_candidate')",
    )
    op.create_foreign_key(
        "fk_whatsapp_processing_items_canonical_item",
        "whatsapp_inbound_processing_items",
        "whatsapp_inbound_processing_items",
        ["canonical_processing_item_id"],
        ["id"],
        ondelete="SET NULL",
    )
    processing_indexes = {
        "normalized_content_sha256": "ix_wa_processing_items_content_sha",
        "content_match_kind": "ix_wa_processing_items_match_kind",
        "canonical_processing_item_id": "ix_wa_processing_items_canonical",
    }
    for column, index_name in processing_indexes.items():
        op.create_index(
            index_name,
            "whatsapp_inbound_processing_items",
            [column],
        )

    op.add_column(
        "crm_complaint_documents",
        sa.Column("duplicate_of_document_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_crm_complaint_documents_duplicate_of",
        "crm_complaint_documents",
        "crm_complaint_documents",
        ["duplicate_of_document_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_crm_complaint_documents_duplicate_of_document_id",
        "crm_complaint_documents",
        ["duplicate_of_document_id"],
    )

    # Existing captures are preserved. Within each case and exact binary, elect
    # the earliest document as the display canonical and point later captures to it.
    op.execute(
        """
        WITH ranked AS (
            SELECT
                document.id,
                first_value(document.id) OVER (
                    PARTITION BY link.complaint_case_id, document.source_sha256
                    ORDER BY document.created_at, document.id::text
                ) AS canonical_id,
                row_number() OVER (
                    PARTITION BY link.complaint_case_id, document.source_sha256
                    ORDER BY document.created_at, document.id::text
                ) AS position
            FROM crm_complaint_documents AS document
            JOIN crm_complaint_document_case_links AS link
              ON link.complaint_document_id = document.id
            WHERE document.source_sha256 IS NOT NULL
        )
        UPDATE crm_complaint_documents AS document
        SET duplicate_of_document_id = ranked.canonical_id
        FROM ranked
        WHERE document.id = ranked.id AND ranked.position > 1
        """
    )


def downgrade() -> None:
    op.drop_index(
        "ix_crm_complaint_documents_duplicate_of_document_id",
        table_name="crm_complaint_documents",
    )
    op.drop_constraint(
        "fk_crm_complaint_documents_duplicate_of",
        "crm_complaint_documents",
        type_="foreignkey",
    )
    op.drop_column("crm_complaint_documents", "duplicate_of_document_id")

    processing_indexes = {
        "canonical_processing_item_id": "ix_wa_processing_items_canonical",
        "content_match_kind": "ix_wa_processing_items_match_kind",
        "normalized_content_sha256": "ix_wa_processing_items_content_sha",
    }
    for _column, index_name in processing_indexes.items():
        op.drop_index(
            index_name,
            table_name="whatsapp_inbound_processing_items",
        )
    op.drop_constraint(
        "fk_whatsapp_processing_items_canonical_item",
        "whatsapp_inbound_processing_items",
        type_="foreignkey",
    )
    op.drop_constraint(
        "ck_whatsapp_inbound_processing_items_content_match_kind",
        "whatsapp_inbound_processing_items",
        type_="check",
    )
    op.drop_column("whatsapp_inbound_processing_items", "content_match_details_json")
    op.drop_column("whatsapp_inbound_processing_items", "canonical_processing_item_id")
    op.drop_column("whatsapp_inbound_processing_items", "content_match_kind")
    op.drop_column("whatsapp_inbound_processing_items", "normalized_content_sha256")
    op.drop_column("whatsapp_inbound_processing_runs", "content_duplicate_items")
