"""WhatsApp inbound CRM categorization and dry-run review queue.

Revision ID: d7c9e4a1b620
Revises: c4b8d2e5f710
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa

revision = "d7c9e4a1b620"
down_revision = "c4b8d2e5f710"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_inbound_processing_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_code", sa.String(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("classifier_version", sa.String(), nullable=False),
        sa.Column("crm_rule_version", sa.String(), nullable=False),
        sa.Column("paperless_check_requested", sa.Boolean(), nullable=False),
        sa.Column("paperless_check_status", sa.String(), nullable=False),
        sa.Column("paperless_error", sa.Text(), nullable=True),
        sa.Column("total_items", sa.Integer(), nullable=False),
        sa.Column("processed_items", sa.Integer(), nullable=False),
        sa.Column("crm_complaints", sa.Integer(), nullable=False),
        sa.Column("possible_crm", sa.Integer(), nullable=False),
        sa.Column("supporting_documents", sa.Integer(), nullable=False),
        sa.Column("reply_reports", sa.Integer(), nullable=False),
        sa.Column("non_crm", sa.Integer(), nullable=False),
        sa.Column("unknown_items", sa.Integer(), nullable=False),
        sa.Column("duplicate_items", sa.Integer(), nullable=False),
        sa.Column("eligible_items", sa.Integer(), nullable=False),
        sa.Column("review_items", sa.Integer(), nullable=False),
        sa.Column("failed_items", sa.Integer(), nullable=False),
        sa.Column("configuration_json", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('queued', 'extracting', 'checking_paperless', 'completed', 'completed_with_errors', 'failed', 'cancelled')",
            name="ck_whatsapp_inbound_processing_runs_status",
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["whatsapp_inbound_batches.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_code"),
    )
    for column in [
        "run_code", "batch_id", "status", "classifier_version", "crm_rule_version",
        "paperless_check_requested", "paperless_check_status", "created_at", "started_at",
        "finished_at", "updated_at",
    ]:
        op.create_index(
            f"ix_whatsapp_inbound_processing_runs_{column}",
            "whatsapp_inbound_processing_runs",
            [column],
        )

    op.create_table(
        "whatsapp_inbound_processing_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("batch_item_id", sa.Uuid(), nullable=False),
        sa.Column("attachment_id", sa.Uuid(), nullable=False),
        sa.Column("stored_object_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("primary_category", sa.String(), nullable=False),
        sa.Column("detected_complaint_number", sa.String(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("evidence_json", sa.JSON(), nullable=False),
        sa.Column("extracted_text", sa.Text(), nullable=True),
        sa.Column("extraction_method", sa.String(), nullable=True),
        sa.Column("extracted_metadata_json", sa.JSON(), nullable=False),
        sa.Column("paperless_category", sa.String(), nullable=False),
        sa.Column("paperless_reason", sa.Text(), nullable=True),
        sa.Column("paperless_document_ids", sa.JSON(), nullable=False),
        sa.Column("paperless_statuses", sa.JSON(), nullable=False),
        sa.Column("review_status", sa.String(), nullable=False),
        sa.Column("reviewer_note", sa.Text(), nullable=True),
        sa.Column("reviewed_by", sa.String(), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("derived_object_key", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('queued', 'extracting', 'extracted', 'duplicate_in_paperless', 'eligible', 'needs_review', 'approved', 'rejected', 'deferred', 'unsupported', 'failed')",
            name="ck_whatsapp_inbound_processing_items_status",
        ),
        sa.CheckConstraint(
            "review_status IN ('pending', 'approved', 'rejected', 'deferred')",
            name="ck_whatsapp_inbound_processing_items_review_status",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["whatsapp_inbound_processing_runs.id"]),
        sa.ForeignKeyConstraint(["batch_item_id"], ["whatsapp_inbound_batch_items.id"]),
        sa.ForeignKeyConstraint(["attachment_id"], ["whatsapp_inbound_attachments.id"]),
        sa.ForeignKeyConstraint(["stored_object_id"], ["whatsapp_inbound_stored_objects.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "run_id", "batch_item_id",
            name="uq_whatsapp_inbound_processing_item_batch_item",
        ),
    )
    for column in [
        "run_id", "batch_item_id", "attachment_id", "stored_object_id", "status",
        "primary_category", "detected_complaint_number", "extraction_method",
        "paperless_category", "review_status", "reviewed_by", "reviewed_at",
        "created_at", "started_at", "finished_at", "updated_at",
    ]:
        op.create_index(
            f"ix_whatsapp_inbound_processing_items_{column}",
            "whatsapp_inbound_processing_items",
            [column],
        )

    op.create_table(
        "whatsapp_inbound_processing_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("processing_item_id", sa.Uuid(), nullable=True),
        sa.Column("level", sa.String(), nullable=False),
        sa.Column("event_type", sa.String(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["run_id"], ["whatsapp_inbound_processing_runs.id"]),
        sa.ForeignKeyConstraint(["processing_item_id"], ["whatsapp_inbound_processing_items.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in ["run_id", "processing_item_id", "level", "event_type", "created_at"]:
        op.create_index(
            f"ix_whatsapp_inbound_processing_events_{column}",
            "whatsapp_inbound_processing_events",
            [column],
        )


def downgrade() -> None:
    op.drop_table("whatsapp_inbound_processing_events")
    op.drop_table("whatsapp_inbound_processing_items")
    op.drop_table("whatsapp_inbound_processing_runs")
