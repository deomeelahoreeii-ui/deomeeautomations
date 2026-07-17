"""Add the native CRM complaint registry and evidence ledger.

Revision ID: b8d2f4a6c901
Revises: a6e8c2d4f701
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa


revision = "b8d2f4a6c901"
down_revision = "a6e8c2d4f701"
branch_labels = None
depends_on = None


def _indexes(table: str, columns: list[str], *, unique: set[str] | None = None) -> None:
    unique = unique or set()
    for column in columns:
        op.create_index(f"ix_{table}_{column}", table, [column], unique=column in unique)


def upgrade() -> None:
    op.create_table(
        "crm_complaint_cases",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_system", sa.String(), nullable=False),
        sa.Column("complaint_number", sa.String(), nullable=True),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("complainant_name", sa.String(), nullable=True),
        sa.Column("complainant_mobile", sa.String(), nullable=True),
        sa.Column("complainant_cnic", sa.String(), nullable=True),
        sa.Column("complainant_address", sa.Text(), nullable=True),
        sa.Column("district", sa.String(), nullable=True),
        sa.Column("tehsil", sa.String(), nullable=True),
        sa.Column("department", sa.String(), nullable=True),
        sa.Column("category", sa.String(), nullable=True),
        sa.Column("sub_category", sa.String(), nullable=True),
        sa.Column("remarks", sa.Text(), nullable=True),
        sa.Column("canonical_paperless_document_id", sa.Integer(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "state IN ('candidate', 'review_required', 'fresh', 'existing', 'publishing', 'published', 'rejected')",
            name="ck_crm_complaint_cases_state",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_system", "complaint_number", name="uq_crm_case_source_number"),
    )
    _indexes(
        "crm_complaint_cases",
        [
            "source_system", "complaint_number", "state", "complainant_name",
            "complainant_mobile", "complainant_cnic", "district", "tehsil",
            "department", "category", "sub_category", "canonical_paperless_document_id",
            "created_at", "updated_at",
        ],
    )

    op.create_table(
        "crm_complaint_documents",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=True),
        sa.Column("source_processing_item_id", sa.Uuid(), nullable=False),
        sa.Column("source_attachment_id", sa.Uuid(), nullable=True),
        sa.Column("source_message_id", sa.Uuid(), nullable=True),
        sa.Column("source_sha256", sa.String(), nullable=True),
        sa.Column("original_filename", sa.String(), nullable=True),
        sa.Column("mime_type", sa.String(), nullable=True),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("relationship_confidence", sa.Float(), nullable=False),
        sa.Column("relationship_reason", sa.Text(), nullable=True),
        sa.Column("paperless_document_id", sa.Integer(), nullable=True),
        sa.Column("review_state", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "role IN ('main_complaint', 'complaint_details', 'attachment', 'reply', 'report', 'unclassified')",
            name="ck_crm_complaint_documents_role",
        ),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["source_processing_item_id"], ["whatsapp_inbound_processing_items.id"]),
        sa.ForeignKeyConstraint(["source_attachment_id"], ["whatsapp_inbound_attachments.id"]),
        sa.ForeignKeyConstraint(["source_message_id"], ["whatsapp_inbound_messages.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_processing_item_id", name="uq_crm_document_processing_item"),
    )
    _indexes(
        "crm_complaint_documents",
        [
            "complaint_case_id", "source_processing_item_id", "source_attachment_id",
            "source_message_id", "source_sha256", "original_filename", "mime_type", "role",
            "paperless_document_id", "review_state", "created_at", "updated_at",
        ],
    )

    op.create_table(
        "crm_document_extractions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_document_id", sa.Uuid(), nullable=False),
        sa.Column("extractor_name", sa.String(), nullable=False),
        sa.Column("extractor_version", sa.String(), nullable=False),
        sa.Column("extraction_method", sa.String(), nullable=False),
        sa.Column("content_sha256", sa.String(), nullable=False),
        sa.Column("raw_text", sa.Text(), nullable=False),
        sa.Column("normalized_text", sa.Text(), nullable=False),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["complaint_document_id"], ["crm_complaint_documents.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "complaint_document_id", "extractor_name", "extractor_version", "content_sha256",
            name="uq_crm_document_extraction_version",
        ),
    )
    _indexes(
        "crm_document_extractions",
        ["complaint_document_id", "extractor_name", "extractor_version", "extraction_method", "content_sha256", "created_at"],
    )

    op.create_table(
        "crm_complaint_field_observations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=True),
        sa.Column("extraction_id", sa.Uuid(), nullable=False),
        sa.Column("field_name", sa.String(), nullable=False),
        sa.Column("raw_value", sa.Text(), nullable=False),
        sa.Column("normalized_value", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("source_locator", sa.String(), nullable=True),
        sa.Column("decision", sa.String(), nullable=False),
        sa.Column("reviewed_by", sa.String(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["extraction_id"], ["crm_document_extractions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    _indexes(
        "crm_complaint_field_observations",
        ["complaint_case_id", "extraction_id", "field_name", "source_locator", "decision", "reviewed_by", "reviewed_at", "created_at"],
    )

    op.create_table(
        "crm_complaint_matches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("processing_item_id", sa.Uuid(), nullable=True),
        sa.Column("matched_case_id", sa.Uuid(), nullable=True),
        sa.Column("paperless_document_id", sa.Integer(), nullable=True),
        sa.Column("proposed_decision", sa.String(), nullable=False),
        sa.Column("final_decision", sa.String(), nullable=True),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("signals_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["matched_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["processing_item_id"], ["whatsapp_inbound_processing_items.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    _indexes(
        "crm_complaint_matches",
        ["complaint_case_id", "processing_item_id", "matched_case_id", "paperless_document_id", "proposed_decision", "final_decision", "created_at"],
    )

    op.create_table(
        "crm_paperless_publications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_document_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("intended_fields_json", sa.JSON(), nullable=False),
        sa.Column("paperless_task_id", sa.String(), nullable=True),
        sa.Column("paperless_document_id", sa.Integer(), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["complaint_document_id"], ["crm_complaint_documents.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_crm_paperless_publication_key"),
    )
    _indexes(
        "crm_paperless_publications",
        ["complaint_case_id", "complaint_document_id", "idempotency_key", "state", "paperless_task_id", "paperless_document_id", "created_at", "updated_at"],
    )


def downgrade() -> None:
    op.drop_table("crm_paperless_publications")
    op.drop_table("crm_complaint_matches")
    op.drop_table("crm_complaint_field_observations")
    op.drop_table("crm_document_extractions")
    op.drop_table("crm_complaint_documents")
    op.drop_table("crm_complaint_cases")
