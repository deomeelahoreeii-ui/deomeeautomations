"""Make imported replies first-class editable workspace drafts.

Revision ID: f7d1e3a5c842
Revises: f5c9e1a3b620
Create Date: 2026-07-21
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f7d1e3a5c842"
down_revision: Union[str, None] = "f5c9e1a3b620"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("crm_complaint_replies", sa.Column("inquiry_findings", sa.Text(), nullable=True))
    op.add_column("crm_complaint_replies", sa.Column("school_version", sa.Text(), nullable=True))
    op.add_column("crm_complaint_replies", sa.Column("applicable_policy", sa.Text(), nullable=True))
    op.add_column("crm_complaint_replies", sa.Column("disposal_outcome", sa.Text(), nullable=True))
    op.add_column(
        "crm_complaint_replies",
        sa.Column("ai_eligible", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "crm_complaint_replies",
        sa.Column("source_kind", sa.String(length=32), nullable=False, server_default="legacy"),
    )
    op.add_column(
        "crm_complaint_replies",
        sa.Column("workspace_status", sa.String(length=40), nullable=False, server_default="Imported Draft"),
    )
    op.add_column(
        "crm_complaint_replies",
        sa.Column("sync_status", sa.String(length=32), nullable=False, server_default="not_synced"),
    )
    op.add_column("crm_complaint_replies", sa.Column("sync_error", sa.Text(), nullable=True))
    op.add_column("crm_complaint_replies", sa.Column("source_batch_id", sa.Uuid(), nullable=True))
    op.add_column("crm_complaint_replies", sa.Column("source_item_id", sa.Uuid(), nullable=True))
    op.add_column("crm_complaint_replies", sa.Column("last_synced_at", sa.DateTime(), nullable=True))

    op.create_foreign_key(
        "fk_crm_complaint_replies_source_batch",
        "crm_complaint_replies",
        "crm_bulk_operation_batches",
        ["source_batch_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_crm_complaint_replies_source_item",
        "crm_complaint_replies",
        "crm_bulk_operation_items",
        ["source_item_id"],
        ["id"],
        ondelete="SET NULL",
    )
    for column in (
        "source_kind",
        "workspace_status",
        "sync_status",
        "source_batch_id",
        "source_item_id",
        "last_synced_at",
    ):
        op.create_index(
            f"ix_crm_complaint_replies_{column}",
            "crm_complaint_replies",
            [column],
        )

    op.create_check_constraint(
        "ck_crm_complaint_replies_source_kind",
        "crm_complaint_replies",
        "source_kind IN ('legacy', 'bulk_import', 'manual_editor', 'frappe_helpdesk')",
    )
    op.create_check_constraint(
        "ck_crm_complaint_replies_workspace_status",
        "crm_complaint_replies",
        "workspace_status IN ('Imported Draft', 'Not Prepared', 'Draft', 'Pending Approval', 'Approved', 'Issued', 'Rejected')",
    )
    op.create_check_constraint(
        "ck_crm_complaint_replies_sync_status",
        "crm_complaint_replies",
        "sync_status IN ('not_synced', 'pending', 'synchronized', 'failed', 'conflict')",
    )

    connection = op.get_bind()
    connection.execute(
        sa.text(
            "UPDATE crm_complaint_replies "
            "SET source_kind = CASE "
            "WHEN source_filename LIKE 'frappe-helpdesk/%' THEN 'frappe_helpdesk' "
            "ELSE 'bulk_import' END, "
            "workspace_status = CASE "
            "WHEN source_filename LIKE 'frappe-helpdesk/%' THEN COALESCE(("
            "SELECT CASE WHEN c.frappe_reply_approval_status IN ('Approved', 'Issued') "
            "THEN c.frappe_reply_approval_status ELSE 'Approved' END "
            "FROM crm_complaint_cases c WHERE c.id = crm_complaint_replies.complaint_case_id"
            "), 'Approved') ELSE 'Imported Draft' END, "
            "sync_status = CASE "
            "WHEN source_filename LIKE 'frappe-helpdesk/%' THEN 'synchronized' "
            "ELSE 'not_synced' END, "
            "last_synced_at = CASE "
            "WHEN source_filename LIKE 'frappe-helpdesk/%' THEN updated_at ELSE NULL END, "
            "inquiry_findings = COALESCE(inquiry_findings, (SELECT c.frappe_inquiry_findings FROM crm_complaint_cases c WHERE c.id = crm_complaint_replies.complaint_case_id)), "
            "school_version = COALESCE(school_version, (SELECT c.frappe_school_version FROM crm_complaint_cases c WHERE c.id = crm_complaint_replies.complaint_case_id)), "
            "applicable_policy = COALESCE(applicable_policy, (SELECT c.frappe_applicable_policy FROM crm_complaint_cases c WHERE c.id = crm_complaint_replies.complaint_case_id)), "
            "disposal_outcome = COALESCE(disposal_outcome, (SELECT c.frappe_disposal_outcome FROM crm_complaint_cases c WHERE c.id = crm_complaint_replies.complaint_case_id)), "
            "ai_eligible = COALESCE((SELECT c.frappe_ai_eligible FROM crm_complaint_cases c WHERE c.id = crm_complaint_replies.complaint_case_id), false)"
        )
    )

    dialect = connection.dialect.name
    if dialect == "postgresql":
        connection.execute(
            sa.text(
                "UPDATE crm_complaint_replies AS reply "
                "SET source_item_id = matched.id, source_batch_id = matched.batch_id "
                "FROM ("
                "  SELECT DISTINCT ON (complaint_case_id) id, batch_id, complaint_case_id "
                "  FROM crm_bulk_operation_items "
                "  WHERE status IN ('imported', 'updated', 'unchanged') "
                "  ORDER BY complaint_case_id, processed_at DESC NULLS LAST, created_at DESC"
                ") AS matched "
                "WHERE reply.complaint_case_id = matched.complaint_case_id "
                "AND reply.source_kind = 'bulk_import'"
            )
        )
    else:
        connection.execute(
            sa.text(
                "UPDATE crm_complaint_replies "
                "SET source_item_id = ("
                "  SELECT item.id FROM crm_bulk_operation_items item "
                "  WHERE item.complaint_case_id = crm_complaint_replies.complaint_case_id "
                "  AND item.status IN ('imported', 'updated', 'unchanged') "
                "  ORDER BY item.processed_at DESC, item.created_at DESC LIMIT 1"
                "), source_batch_id = ("
                "  SELECT item.batch_id FROM crm_bulk_operation_items item "
                "  WHERE item.complaint_case_id = crm_complaint_replies.complaint_case_id "
                "  AND item.status IN ('imported', 'updated', 'unchanged') "
                "  ORDER BY item.processed_at DESC, item.created_at DESC LIMIT 1"
                ") WHERE source_kind = 'bulk_import'"
            )
        )

    op.alter_column("crm_complaint_replies", "source_kind", server_default=None)
    op.alter_column("crm_complaint_replies", "workspace_status", server_default=None)
    op.alter_column("crm_complaint_replies", "sync_status", server_default=None)


def downgrade() -> None:
    op.drop_constraint(
        "ck_crm_complaint_replies_sync_status",
        "crm_complaint_replies",
        type_="check",
    )
    op.drop_constraint(
        "ck_crm_complaint_replies_workspace_status",
        "crm_complaint_replies",
        type_="check",
    )
    op.drop_constraint(
        "ck_crm_complaint_replies_source_kind",
        "crm_complaint_replies",
        type_="check",
    )
    for column in (
        "last_synced_at",
        "source_item_id",
        "source_batch_id",
        "sync_status",
        "workspace_status",
        "source_kind",
    ):
        op.drop_index(
            f"ix_crm_complaint_replies_{column}",
            table_name="crm_complaint_replies",
        )
    op.drop_constraint(
        "fk_crm_complaint_replies_source_item",
        "crm_complaint_replies",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_crm_complaint_replies_source_batch",
        "crm_complaint_replies",
        type_="foreignkey",
    )
    op.drop_column("crm_complaint_replies", "last_synced_at")
    op.drop_column("crm_complaint_replies", "source_item_id")
    op.drop_column("crm_complaint_replies", "source_batch_id")
    op.drop_column("crm_complaint_replies", "sync_error")
    op.drop_column("crm_complaint_replies", "sync_status")
    op.drop_column("crm_complaint_replies", "workspace_status")
    op.drop_column("crm_complaint_replies", "source_kind")
    op.drop_column("crm_complaint_replies", "ai_eligible")
    op.drop_column("crm_complaint_replies", "disposal_outcome")
    op.drop_column("crm_complaint_replies", "applicable_policy")
    op.drop_column("crm_complaint_replies", "school_version")
    op.drop_column("crm_complaint_replies", "inquiry_findings")
