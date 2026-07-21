"""Add CRM complaint dispatch routing and generic preview sources.

Revision ID: fd07e9a1c408
Revises: fbe5c7d9e286
Create Date: 2026-07-21
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "fd07e9a1c408"
down_revision: Union[str, None] = "fbe5c7d9e286"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _index(table: str, *columns: str) -> None:
    for column in columns:
        op.create_index(f"ix_{table}_{column}", table, [column])


def upgrade() -> None:
    op.alter_column(
        "whatsapp_dispatch_previews",
        "source_job_id",
        existing_type=sa.Uuid(),
        nullable=True,
    )
    op.add_column(
        "whatsapp_dispatch_previews",
        sa.Column("source_kind", sa.String(length=40), nullable=False, server_default="antidengue_job"),
    )
    op.add_column(
        "whatsapp_dispatch_previews",
        sa.Column("source_reference_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "whatsapp_dispatch_previews",
        sa.Column("source_revision", sa.Integer(), nullable=False, server_default="1"),
    )
    op.create_index("ix_whatsapp_dispatch_previews_source_kind", "whatsapp_dispatch_previews", ["source_kind"])
    op.create_index("ix_whatsapp_dispatch_previews_source_reference_id", "whatsapp_dispatch_previews", ["source_reference_id"])

    op.create_table(
        "crm_dispatch_rules",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("selection_mode", sa.String(length=24), nullable=False),
        sa.Column("conditions_json", sa.JSON(), nullable=False),
        sa.Column("dispatch_profile_ids_json", sa.JSON(), nullable=False),
        sa.Column("stop_after_match", sa.Boolean(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("effective_from", sa.DateTime(), nullable=True),
        sa.Column("effective_to", sa.DateTime(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("created_by", sa.String(length=120), nullable=False),
        sa.Column("updated_by", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("selection_mode IN ('suggested', 'manual_only', 'fallback')", name="ck_crm_dispatch_rules_selection_mode"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_crm_dispatch_rules_name"),
    )
    _index("crm_dispatch_rules", "name", "priority", "selection_mode", "enabled", "effective_from", "effective_to", "created_by", "updated_by", "created_at", "updated_at")

    op.create_table(
        "crm_dispatch_batches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_number", sa.String(length=80), nullable=False),
        sa.Column("status", sa.String(length=40), nullable=False),
        sa.Column("source_mode", sa.String(length=40), nullable=False),
        sa.Column("purpose", sa.String(length=80), nullable=False),
        sa.Column("total_items", sa.Integer(), nullable=False),
        sa.Column("ready_items", sa.Integer(), nullable=False),
        sa.Column("blocked_items", sa.Integer(), nullable=False),
        sa.Column("excluded_items", sa.Integer(), nullable=False),
        sa.Column("queued_items", sa.Integer(), nullable=False),
        sa.Column("successful_items", sa.Integer(), nullable=False),
        sa.Column("failed_items", sa.Integer(), nullable=False),
        sa.Column("response_due_at", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(length=120), nullable=False),
        sa.Column("approved_by", sa.String(length=120), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("approved_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "status IN ('draft', 'resolving_routes', 'review_required', 'ready', 'approved', 'queued', 'sending', 'completed', 'completed_with_errors', 'failed', 'cancelled', 'stale')",
            name="ck_crm_dispatch_batches_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_number", name="uq_crm_dispatch_batches_number"),
    )
    _index("crm_dispatch_batches", "batch_number", "status", "source_mode", "purpose", "response_due_at", "created_by", "approved_by", "created_at", "updated_at", "approved_at", "completed_at")

    op.create_table(
        "crm_dispatch_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("official_letter_id", sa.Uuid(), nullable=False),
        sa.Column("complete_pdf_artifact_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_number_snapshot", sa.String(length=80), nullable=False),
        sa.Column("letter_number_snapshot", sa.String(length=180), nullable=False),
        sa.Column("letter_date_snapshot", sa.Date(), nullable=False),
        sa.Column("packet_sha256", sa.String(length=64), nullable=False),
        sa.Column("packet_size_bytes", sa.Integer(), nullable=False),
        sa.Column("packet_page_count", sa.Integer(), nullable=False),
        sa.Column("route_status", sa.String(length=24), nullable=False),
        sa.Column("route_summary_json", sa.JSON(), nullable=False),
        sa.Column("excluded", sa.Boolean(), nullable=False),
        sa.Column("exclusion_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("route_status IN ('ready', 'needs_review', 'conflict', 'blocked', 'excluded')", name="ck_crm_dispatch_items_route_status"),
        sa.ForeignKeyConstraint(["batch_id"], ["crm_dispatch_batches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["official_letter_id"], ["crm_official_letters.id"]),
        sa.ForeignKeyConstraint(["complete_pdf_artifact_id"], ["crm_official_letter_artifacts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_id", "official_letter_id", name="uq_crm_dispatch_items_batch_letter"),
    )
    _index("crm_dispatch_items", "batch_id", "complaint_case_id", "official_letter_id", "complete_pdf_artifact_id", "complaint_number_snapshot", "letter_number_snapshot", "letter_date_snapshot", "packet_sha256", "route_status", "excluded", "created_at", "updated_at")

    op.create_table(
        "crm_dispatch_targets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_item_id", sa.Uuid(), nullable=False),
        sa.Column("routing_rule_id", sa.Uuid(), nullable=True),
        sa.Column("dispatch_profile_id", sa.Uuid(), nullable=False),
        sa.Column("selection_source", sa.String(length=20), nullable=False),
        sa.Column("manual_override_reason", sa.Text(), nullable=True),
        sa.Column("recipient_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("message_snapshot", sa.Text(), nullable=False),
        sa.Column("message_sha256", sa.String(length=64), nullable=False),
        sa.Column("preview_id", sa.Uuid(), nullable=True),
        sa.Column("preview_delivery_ids_json", sa.JSON(), nullable=False),
        sa.Column("whatsapp_delivery_ids_json", sa.JSON(), nullable=False),
        sa.Column("business_status", sa.String(length=40), nullable=False),
        sa.Column("response_due_at", sa.DateTime(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("sent_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("selection_source IN ('rule', 'manual', 'fallback')", name="ck_crm_dispatch_targets_selection_source"),
        sa.CheckConstraint("business_status IN ('planned', 'blocked', 'excluded', 'approved', 'queued', 'sent_pending_confirmation', 'delivered', 'failed', 'timed_out', 'cancelled')", name="ck_crm_dispatch_targets_business_status"),
        sa.ForeignKeyConstraint(["dispatch_item_id"], ["crm_dispatch_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["routing_rule_id"], ["crm_dispatch_rules.id"]),
        sa.ForeignKeyConstraint(["dispatch_profile_id"], ["whatsapp_dispatch_profiles.id"]),
        sa.ForeignKeyConstraint(["preview_id"], ["whatsapp_dispatch_previews.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dispatch_item_id", "dispatch_profile_id", name="uq_crm_dispatch_targets_item_profile"),
    )
    _index("crm_dispatch_targets", "dispatch_item_id", "routing_rule_id", "dispatch_profile_id", "selection_source", "message_sha256", "preview_id", "business_status", "response_due_at", "sent_at", "completed_at", "created_at", "updated_at")


def downgrade() -> None:
    op.drop_table("crm_dispatch_targets")
    op.drop_table("crm_dispatch_items")
    op.drop_table("crm_dispatch_batches")
    op.drop_table("crm_dispatch_rules")
    op.drop_index("ix_whatsapp_dispatch_previews_source_reference_id", table_name="whatsapp_dispatch_previews")
    op.drop_index("ix_whatsapp_dispatch_previews_source_kind", table_name="whatsapp_dispatch_previews")
    op.drop_column("whatsapp_dispatch_previews", "source_revision")
    op.drop_column("whatsapp_dispatch_previews", "source_reference_id")
    op.drop_column("whatsapp_dispatch_previews", "source_kind")
    op.alter_column(
        "whatsapp_dispatch_previews",
        "source_job_id",
        existing_type=sa.Uuid(),
        nullable=False,
    )
