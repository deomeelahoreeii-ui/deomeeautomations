"""Separate spreadsheet candidates from durable CRM complaint cases.

Revision ID: e9b2c4d6f801
Revises: d8f1a3c5e702
Create Date: 2026-07-22
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "e9b2c4d6f801"
down_revision = "d8f1a3c5e702"
branch_labels = None
depends_on = None


MEDIA_TYPES_DEFAULT = sa.text("'[\"image\", \"pdf\", \"spreadsheet\"]'")


def upgrade() -> None:
    op.add_column(
        "crm_complaint_cases",
        sa.Column(
            "registry_status",
            sa.String(length=24),
            nullable=False,
            server_default="active",
        ),
    )
    op.add_column(
        "crm_complaint_cases",
        sa.Column("quarantine_reason", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        "ck_crm_complaint_cases_registry_status",
        "crm_complaint_cases",
        "registry_status IN ('active', 'quarantined')",
    )
    op.create_index(
        "ix_crm_complaint_cases_registry_status",
        "crm_complaint_cases",
        ["registry_status"],
    )

    op.add_column(
        "whatsapp_inbound_history_requests",
        sa.Column(
            "media_types_json",
            sa.JSON(),
            nullable=False,
            server_default=MEDIA_TYPES_DEFAULT,
        ),
    )
    op.add_column(
        "whatsapp_inbound_batches",
        sa.Column(
            "media_types_json",
            sa.JSON(),
            nullable=False,
            server_default=MEDIA_TYPES_DEFAULT,
        ),
    )
    op.add_column(
        "whatsapp_inbound_batches",
        sa.Column("files_excluded", sa.Integer(), nullable=False, server_default="0"),
    )

    op.create_table(
        "crm_spreadsheet_intake_batches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("processing_item_id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("source_document_id", sa.Uuid(), nullable=True),
        sa.Column("source_filename", sa.String(length=255), nullable=False),
        sa.Column("source_sha256", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("total_rows", sa.Integer(), nullable=False),
        sa.Column("candidate_rows", sa.Integer(), nullable=False),
        sa.Column("invalid_rows", sa.Integer(), nullable=False),
        sa.Column("existing_rows", sa.Integer(), nullable=False),
        sa.Column("fresh_rows", sa.Integer(), nullable=False),
        sa.Column("manual_review_rows", sa.Integer(), nullable=False),
        sa.Column("approved_rows", sa.Integer(), nullable=False),
        sa.Column("rejected_rows", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('extracting', 'checking_paperless', 'awaiting_review', "
            "'completed', 'rejected', 'failed')",
            name="ck_crm_spreadsheet_intake_batches_status",
        ),
        sa.ForeignKeyConstraint(
            ["processing_item_id"],
            ["whatsapp_inbound_processing_items.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["whatsapp_inbound_processing_runs.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_document_id"],
            ["crm_complaint_documents.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "processing_item_id",
            name="uq_crm_spreadsheet_intake_batches_processing_item",
        ),
    )
    for column in (
        "processing_item_id",
        "run_id",
        "source_document_id",
        "source_filename",
        "source_sha256",
        "status",
        "created_at",
        "updated_at",
        "completed_at",
    ):
        op.create_index(
            f"ix_crm_spreadsheet_intake_batches_{column}",
            "crm_spreadsheet_intake_batches",
            [column],
        )

    op.create_table(
        "crm_spreadsheet_intake_rows",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("processing_item_id", sa.Uuid(), nullable=False),
        sa.Column("sheet_name", sa.String(length=180), nullable=False),
        sa.Column("row_number", sa.Integer(), nullable=False),
        sa.Column("source_locator", sa.String(length=320), nullable=False),
        sa.Column("row_sha256", sa.String(length=64), nullable=False),
        sa.Column("values_json", sa.JSON(), nullable=False),
        sa.Column("complaint_number", sa.String(length=80), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("paperless_category", sa.String(length=40), nullable=False),
        sa.Column("paperless_reason", sa.Text(), nullable=True),
        sa.Column("paperless_document_ids", sa.JSON(), nullable=False),
        sa.Column("paperless_statuses", sa.JSON(), nullable=False),
        sa.Column("paperless_checked_at", sa.DateTime(), nullable=True),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=True),
        sa.Column("reviewer_note", sa.Text(), nullable=True),
        sa.Column("reviewed_by", sa.String(length=120), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('candidate', 'existing', 'fresh', 'manual_review', "
            "'approved', 'rejected', 'invalid')",
            name="ck_crm_spreadsheet_intake_rows_status",
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            ["crm_spreadsheet_intake_batches.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["processing_item_id"],
            ["whatsapp_inbound_processing_items.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["complaint_case_id"],
            ["crm_complaint_cases.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "batch_id",
            "source_locator",
            name="uq_crm_spreadsheet_intake_rows_source",
        ),
    )
    for column in (
        "batch_id",
        "processing_item_id",
        "sheet_name",
        "row_number",
        "source_locator",
        "row_sha256",
        "complaint_number",
        "status",
        "paperless_category",
        "paperless_checked_at",
        "complaint_case_id",
        "reviewed_by",
        "reviewed_at",
        "created_at",
        "updated_at",
    ):
        op.create_index(
            f"ix_crm_spreadsheet_intake_rows_{column}",
            "crm_spreadsheet_intake_rows",
            [column],
        )

    # Existing rows created solely from an unreviewed spreadsheet are retained
    # for audit, but removed from the active complaint registry and its metrics.
    op.execute(
        """
        UPDATE crm_complaint_cases AS complaint_case
        SET
            registry_status = 'quarantined',
            quarantine_reason = 'Legacy spreadsheet row was materialized before row approval.'
        WHERE EXISTS (
            SELECT 1
            FROM crm_complaint_document_case_links AS link
            WHERE link.complaint_case_id = complaint_case.id
              AND link.role = 'source_row'
        )
          AND NOT EXISTS (
            SELECT 1
            FROM crm_complaint_document_case_links AS link
            WHERE link.complaint_case_id = complaint_case.id
              AND link.role <> 'source_row'
        )
          AND NOT EXISTS (
            SELECT 1 FROM crm_paperless_publications AS publication
            WHERE publication.complaint_case_id = complaint_case.id
        )
          AND NOT EXISTS (
            SELECT 1 FROM crm_dispatch_items AS dispatch_item
            WHERE dispatch_item.complaint_case_id = complaint_case.id
        )
          AND NOT EXISTS (
            SELECT 1 FROM crm_complaint_audit_events AS audit_event
            WHERE audit_event.complaint_case_id = complaint_case.id
        )
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE crm_complaint_cases
        SET registry_status = 'active', quarantine_reason = NULL
        WHERE registry_status = 'quarantined'
          AND quarantine_reason = 'Legacy spreadsheet row was materialized before row approval.'
        """
    )
    op.drop_table("crm_spreadsheet_intake_rows")
    op.drop_table("crm_spreadsheet_intake_batches")
    op.drop_column("whatsapp_inbound_batches", "files_excluded")
    op.drop_column("whatsapp_inbound_batches", "media_types_json")
    op.drop_column("whatsapp_inbound_history_requests", "media_types_json")
    op.drop_index(
        "ix_crm_complaint_cases_registry_status",
        table_name="crm_complaint_cases",
    )
    op.drop_constraint(
        "ck_crm_complaint_cases_registry_status",
        "crm_complaint_cases",
        type_="check",
    )
    op.drop_column("crm_complaint_cases", "quarantine_reason")
    op.drop_column("crm_complaint_cases", "registry_status")
