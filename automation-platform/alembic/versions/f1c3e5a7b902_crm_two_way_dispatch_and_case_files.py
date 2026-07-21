"""Add two-way CRM dispatch, generic complaint packets and manual case files.

Revision ID: f1c3e5a7b902
Revises: ff29cbe4a5f0
Create Date: 2026-07-21
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "f1c3e5a7b902"
down_revision: Union[str, None] = "ff29cbe4a5f0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_constraint("ck_crm_complaint_documents_role", "crm_complaint_documents", type_="check")
    op.alter_column("crm_complaint_documents", "source_processing_item_id", existing_type=sa.Uuid(), nullable=True)
    op.add_column("crm_complaint_documents", sa.Column("source_kind", sa.String(length=32), nullable=False, server_default="whatsapp_inbound"))
    op.add_column("crm_complaint_documents", sa.Column("storage_path", sa.Text(), nullable=True))
    op.add_column("crm_complaint_documents", sa.Column("uploaded_by", sa.String(length=120), nullable=True))
    op.add_column("crm_complaint_documents", sa.Column("source_dispatch_batch_id", sa.Uuid(), nullable=True))
    op.add_column("crm_complaint_documents", sa.Column("source_dispatch_item_id", sa.Uuid(), nullable=True))
    op.create_check_constraint(
        "ck_crm_complaint_documents_role", "crm_complaint_documents",
        "role IN ('main_complaint', 'complaint_details', 'attachment', 'reply', 'report', 'policy', 'unclassified')",
    )
    op.create_check_constraint(
        "ck_crm_complaint_documents_source_kind", "crm_complaint_documents",
        "source_kind IN ('whatsapp_inbound', 'manual_upload')",
    )
    for column in ("source_kind", "uploaded_by", "source_dispatch_batch_id", "source_dispatch_item_id"):
        op.create_index(f"ix_crm_complaint_documents_{column}", "crm_complaint_documents", [column])
    op.create_foreign_key(
        "fk_crm_complaint_documents_dispatch_batch",
        "crm_complaint_documents", "crm_dispatch_batches",
        ["source_dispatch_batch_id"], ["id"], ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_crm_complaint_documents_dispatch_item",
        "crm_complaint_documents", "crm_dispatch_items",
        ["source_dispatch_item_id"], ["id"], ondelete="SET NULL",
    )

    op.add_column("crm_dispatch_batches", sa.Column("direction", sa.String(length=20), nullable=False, server_default="upward"))
    op.create_check_constraint(
        "ck_crm_dispatch_batches_direction", "crm_dispatch_batches",
        "direction IN ('downward', 'upward')",
    )
    op.create_index("ix_crm_dispatch_batches_direction", "crm_dispatch_batches", ["direction"])

    # Earlier B3.6 batches could contain more than one official-letter revision for the
    # same complaint. The two-way model uses one complaint item per batch, so retain the
    # newest record deterministically before enforcing the stronger invariant. Target rows
    # linked to discarded items are removed through the existing ON DELETE CASCADE key.
    op.execute(
        """
        WITH ranked AS (
            SELECT id,
                   row_number() OVER (
                       PARTITION BY batch_id, complaint_case_id
                       ORDER BY created_at DESC, id::text DESC
                   ) AS position
            FROM crm_dispatch_items
        )
        DELETE FROM crm_dispatch_items AS item
        USING ranked
        WHERE item.id = ranked.id AND ranked.position > 1
        """
    )

    op.drop_constraint("uq_crm_dispatch_items_batch_letter", "crm_dispatch_items", type_="unique")
    op.alter_column("crm_dispatch_items", "official_letter_id", existing_type=sa.Uuid(), nullable=True)
    op.alter_column("crm_dispatch_items", "complete_pdf_artifact_id", existing_type=sa.Uuid(), nullable=True)
    op.alter_column("crm_dispatch_items", "letter_number_snapshot", existing_type=sa.String(length=180), nullable=True)
    op.alter_column("crm_dispatch_items", "letter_date_snapshot", existing_type=sa.Date(), nullable=True)
    op.add_column("crm_dispatch_items", sa.Column("packet_artifact_id", sa.Uuid(), nullable=True))
    op.add_column("crm_dispatch_items", sa.Column("compliance_status", sa.String(length=24), nullable=False, server_default="not_requested"))
    op.create_check_constraint(
        "ck_crm_dispatch_items_compliance_status", "crm_dispatch_items",
        "compliance_status IN ('not_requested', 'requested', 'received', 'under_review', 'incorporated', 'submitted', 'closed')",
    )
    op.create_unique_constraint("uq_crm_dispatch_items_batch_case", "crm_dispatch_items", ["batch_id", "complaint_case_id"])
    op.create_index("ix_crm_dispatch_items_packet_artifact_id", "crm_dispatch_items", ["packet_artifact_id"])
    op.create_index("ix_crm_dispatch_items_compliance_status", "crm_dispatch_items", ["compliance_status"])

    op.create_table(
        "crm_dispatch_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_item_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=160), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("page_count", sa.Integer(), nullable=False),
        sa.Column("source_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("kind IN ('assignment_packet', 'submission_packet')", name="ck_crm_dispatch_artifacts_kind"),
        sa.ForeignKeyConstraint(["batch_id"], ["crm_dispatch_batches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["dispatch_item_id"], ["crm_dispatch_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dispatch_item_id", "kind", name="uq_crm_dispatch_artifacts_item_kind"),
    )
    for column in ("batch_id", "dispatch_item_id", "complaint_case_id", "kind", "sha256", "created_at"):
        op.create_index(f"ix_crm_dispatch_artifacts_{column}", "crm_dispatch_artifacts", [column])
    op.create_foreign_key(
        "fk_crm_dispatch_items_packet_artifact_id",
        "crm_dispatch_items", "crm_dispatch_artifacts", ["packet_artifact_id"], ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.execute("DELETE FROM crm_dispatch_targets WHERE dispatch_item_id IN (SELECT id FROM crm_dispatch_items WHERE official_letter_id IS NULL)")
    op.execute("DELETE FROM crm_dispatch_items WHERE official_letter_id IS NULL")
    op.execute("DELETE FROM crm_dispatch_batches WHERE direction = 'downward'")
    op.drop_constraint("fk_crm_dispatch_items_packet_artifact_id", "crm_dispatch_items", type_="foreignkey")
    op.drop_table("crm_dispatch_artifacts")
    op.drop_index("ix_crm_dispatch_items_compliance_status", table_name="crm_dispatch_items")
    op.drop_index("ix_crm_dispatch_items_packet_artifact_id", table_name="crm_dispatch_items")
    op.drop_constraint("uq_crm_dispatch_items_batch_case", "crm_dispatch_items", type_="unique")
    op.drop_constraint("ck_crm_dispatch_items_compliance_status", "crm_dispatch_items", type_="check")
    op.drop_column("crm_dispatch_items", "compliance_status")
    op.drop_column("crm_dispatch_items", "packet_artifact_id")
    op.alter_column("crm_dispatch_items", "letter_date_snapshot", existing_type=sa.Date(), nullable=False)
    op.alter_column("crm_dispatch_items", "letter_number_snapshot", existing_type=sa.String(length=180), nullable=False)
    op.alter_column("crm_dispatch_items", "complete_pdf_artifact_id", existing_type=sa.Uuid(), nullable=False)
    op.alter_column("crm_dispatch_items", "official_letter_id", existing_type=sa.Uuid(), nullable=False)
    op.create_unique_constraint("uq_crm_dispatch_items_batch_letter", "crm_dispatch_items", ["batch_id", "official_letter_id"])

    op.drop_index("ix_crm_dispatch_batches_direction", table_name="crm_dispatch_batches")
    op.drop_constraint("ck_crm_dispatch_batches_direction", "crm_dispatch_batches", type_="check")
    op.drop_column("crm_dispatch_batches", "direction")

    op.execute("DELETE FROM crm_complaint_document_case_links WHERE complaint_document_id IN (SELECT id FROM crm_complaint_documents WHERE source_kind = 'manual_upload')")
    op.execute("DELETE FROM crm_complaint_documents WHERE source_kind = 'manual_upload'")
    op.drop_constraint("fk_crm_complaint_documents_dispatch_item", "crm_complaint_documents", type_="foreignkey")
    op.drop_constraint("fk_crm_complaint_documents_dispatch_batch", "crm_complaint_documents", type_="foreignkey")
    for column in ("source_dispatch_item_id", "source_dispatch_batch_id", "uploaded_by", "source_kind"):
        op.drop_index(f"ix_crm_complaint_documents_{column}", table_name="crm_complaint_documents")
    op.drop_constraint("ck_crm_complaint_documents_source_kind", "crm_complaint_documents", type_="check")
    op.drop_constraint("ck_crm_complaint_documents_role", "crm_complaint_documents", type_="check")
    op.drop_column("crm_complaint_documents", "source_dispatch_item_id")
    op.drop_column("crm_complaint_documents", "source_dispatch_batch_id")
    op.drop_column("crm_complaint_documents", "uploaded_by")
    op.drop_column("crm_complaint_documents", "storage_path")
    op.drop_column("crm_complaint_documents", "source_kind")
    op.alter_column("crm_complaint_documents", "source_processing_item_id", existing_type=sa.Uuid(), nullable=False)
    op.create_check_constraint(
        "ck_crm_complaint_documents_role", "crm_complaint_documents",
        "role IN ('main_complaint', 'complaint_details', 'attachment', 'reply', 'report', 'unclassified')",
    )
