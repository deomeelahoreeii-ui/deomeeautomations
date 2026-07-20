"""Add Helpdesk routing, workflow snapshots and approved reply history.

Revision ID: b2c7d9e1f304
Revises: b1f4a7c9d203
Create Date: 2026-07-20
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "b2c7d9e1f304"
down_revision: Union[str, None] = "b1f4a7c9d203"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("officers", sa.Column("helpdesk_user_email", sa.String(), nullable=False, server_default=""))
    op.add_column("officers", sa.Column("helpdesk_enabled", sa.Boolean(), nullable=False, server_default=sa.false()))
    op.create_index("ix_officers_helpdesk_user_email", "officers", ["helpdesk_user_email"])
    op.create_index("ix_officers_helpdesk_enabled", "officers", ["helpdesk_enabled"])

    columns = [
        ("frappe_workflow_status", sa.String(), True),
        ("frappe_agent_group", sa.String(), True),
        ("frappe_assigned_users_json", sa.JSON(), False),
        ("frappe_assigned_officer_id", sa.Uuid(), True),
        ("frappe_assigned_officer_name", sa.String(), True),
        ("frappe_assigned_officer_role", sa.String(), True),
        ("frappe_routing_status", sa.String(), False),
        ("frappe_routing_reason", sa.Text(), True),
        ("frappe_last_workflow_pull_at", sa.DateTime(), True),
        ("frappe_reply_approval_status", sa.String(), True),
        ("frappe_disposal_outcome", sa.String(), True),
        ("frappe_ai_eligible", sa.Boolean(), False),
        ("frappe_workflow_hash", sa.String(), True),
        ("frappe_reply_hash", sa.String(), True),
        ("frappe_inquiry_findings", sa.Text(), True),
        ("frappe_school_version", sa.Text(), True),
        ("frappe_applicable_policy", sa.Text(), True),
    ]
    for name, type_, nullable in columns:
        kwargs = {"nullable": nullable}
        if name == "frappe_assigned_users_json":
            kwargs["server_default"] = sa.text("'[]'::json")
        elif name == "frappe_routing_status":
            kwargs["server_default"] = "not_routed"
        elif name == "frappe_ai_eligible":
            kwargs["server_default"] = sa.false()
        op.add_column("crm_complaint_cases", sa.Column(name, type_, **kwargs))

    op.create_foreign_key(
        "fk_crm_complaint_cases_frappe_assigned_officer",
        "crm_complaint_cases", "officers",
        ["frappe_assigned_officer_id"], ["id"],
        ondelete="SET NULL",
    )
    op.create_check_constraint(
        "ck_crm_complaint_cases_frappe_routing_status",
        "crm_complaint_cases",
        "frappe_routing_status IN ('not_routed', 'planned', 'routed', 'manual_preserved', 'failed')",
    )
    for name in (
        "frappe_workflow_status", "frappe_agent_group", "frappe_assigned_officer_id",
        "frappe_assigned_officer_name", "frappe_assigned_officer_role", "frappe_routing_status",
        "frappe_last_workflow_pull_at", "frappe_reply_approval_status", "frappe_disposal_outcome",
        "frappe_workflow_hash", "frappe_reply_hash",
    ):
        op.create_index(f"ix_crm_complaint_cases_{name}", "crm_complaint_cases", [name])

    op.create_table(
        "crm_complaint_reply_revisions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("reply_text", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(), nullable=False),
        sa.Column("source_system", sa.String(), nullable=False),
        sa.Column("source_reference", sa.String(), nullable=False),
        sa.Column("approval_status", sa.String(), nullable=False),
        sa.Column("remote_modified_at", sa.DateTime(), nullable=True),
        sa.Column("captured_at", sa.DateTime(), nullable=False),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.CheckConstraint("approval_status IN ('Approved', 'Issued')", name="ck_crm_reply_revision_approval_status"),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("complaint_case_id", "content_hash", name="uq_crm_reply_revision_case_hash"),
    )
    for name in (
        "complaint_case_id", "content_hash", "source_system", "source_reference",
        "approval_status", "remote_modified_at", "captured_at", "is_current",
    ):
        op.create_index(f"ix_crm_complaint_reply_revisions_{name}", "crm_complaint_reply_revisions", [name])


def downgrade() -> None:
    op.drop_table("crm_complaint_reply_revisions")
    op.drop_constraint("ck_crm_complaint_cases_frappe_routing_status", "crm_complaint_cases", type_="check")
    op.drop_constraint("fk_crm_complaint_cases_frappe_assigned_officer", "crm_complaint_cases", type_="foreignkey")
    indexed = (
        "frappe_workflow_status", "frappe_agent_group", "frappe_assigned_officer_id",
        "frappe_assigned_officer_name", "frappe_assigned_officer_role", "frappe_routing_status",
        "frappe_last_workflow_pull_at", "frappe_reply_approval_status", "frappe_disposal_outcome",
        "frappe_workflow_hash", "frappe_reply_hash",
    )
    for name in indexed:
        op.drop_index(f"ix_crm_complaint_cases_{name}", table_name="crm_complaint_cases")
    for name in (
        "frappe_applicable_policy", "frappe_school_version", "frappe_inquiry_findings",
        "frappe_reply_hash", "frappe_workflow_hash", "frappe_ai_eligible",
        "frappe_disposal_outcome", "frappe_reply_approval_status", "frappe_last_workflow_pull_at",
        "frappe_routing_reason", "frappe_routing_status", "frappe_assigned_officer_role",
        "frappe_assigned_officer_name", "frappe_assigned_officer_id", "frappe_assigned_users_json",
        "frappe_agent_group", "frappe_workflow_status",
    ):
        op.drop_column("crm_complaint_cases", name)
    op.drop_index("ix_officers_helpdesk_enabled", table_name="officers")
    op.drop_index("ix_officers_helpdesk_user_email", table_name="officers")
    op.drop_column("officers", "helpdesk_enabled")
    op.drop_column("officers", "helpdesk_user_email")
