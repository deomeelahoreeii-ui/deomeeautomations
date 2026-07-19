"""Add Frappe Helpdesk synchronization state for CRM complaints.

Revision ID: b1f4a7c9d203
Revises: a1c3e5f7b902
Create Date: 2026-07-20
"""

from alembic import op
import sqlalchemy as sa


revision = "b1f4a7c9d203"
down_revision = "a1c3e5f7b902"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("crm_complaint_cases", sa.Column("frappe_ticket_id", sa.String(), nullable=True))
    op.add_column("crm_complaint_cases", sa.Column("frappe_ticket_url", sa.Text(), nullable=True))
    op.add_column(
        "crm_complaint_cases",
        sa.Column("frappe_sync_status", sa.String(), nullable=False, server_default="not_synced"),
    )
    op.add_column("crm_complaint_cases", sa.Column("frappe_last_synced_at", sa.DateTime(), nullable=True))
    op.add_column("crm_complaint_cases", sa.Column("frappe_last_error", sa.Text(), nullable=True))
    op.add_column("crm_complaint_cases", sa.Column("frappe_remote_modified_at", sa.DateTime(), nullable=True))
    op.add_column(
        "crm_complaint_cases",
        sa.Column("frappe_sync_version", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column("crm_complaint_cases", sa.Column("frappe_payload_hash", sa.String(), nullable=True))
    op.create_check_constraint(
        "ck_crm_complaint_cases_frappe_sync_status",
        "crm_complaint_cases",
        "frappe_sync_status IN ('not_synced', 'pending', 'synchronized', 'update_pending', 'failed')",
    )
    for column in (
        "frappe_ticket_id",
        "frappe_sync_status",
        "frappe_last_synced_at",
        "frappe_remote_modified_at",
        "frappe_payload_hash",
    ):
        op.create_index(f"ix_crm_complaint_cases_{column}", "crm_complaint_cases", [column])

    op.create_table(
        "crm_frappe_sync_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("direction", sa.String(), nullable=False),
        sa.Column("operation", sa.String(), nullable=False),
        sa.Column("state", sa.String(), nullable=False),
        sa.Column("remote_ticket_id", sa.String(), nullable=True),
        sa.Column("request_fingerprint", sa.String(), nullable=True),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "direction IN ('crm_to_frappe', 'frappe_to_crm')",
            name="ck_crm_frappe_sync_events_direction",
        ),
        sa.CheckConstraint(
            "state IN ('started', 'succeeded', 'failed', 'skipped')",
            name="ck_crm_frappe_sync_events_state",
        ),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "complaint_case_id",
        "direction",
        "operation",
        "state",
        "remote_ticket_id",
        "request_fingerprint",
        "http_status",
        "created_at",
        "completed_at",
    ):
        op.create_index(f"ix_crm_frappe_sync_events_{column}", "crm_frappe_sync_events", [column])


def downgrade() -> None:
    op.drop_table("crm_frappe_sync_events")
    op.drop_constraint(
        "ck_crm_complaint_cases_frappe_sync_status",
        "crm_complaint_cases",
        type_="check",
    )
    for column in (
        "frappe_payload_hash",
        "frappe_remote_modified_at",
        "frappe_last_synced_at",
        "frappe_sync_status",
        "frappe_ticket_id",
    ):
        op.drop_index(f"ix_crm_complaint_cases_{column}", table_name="crm_complaint_cases")
    for column in (
        "frappe_payload_hash",
        "frappe_sync_version",
        "frappe_remote_modified_at",
        "frappe_last_error",
        "frappe_last_synced_at",
        "frappe_sync_status",
        "frappe_ticket_url",
        "frappe_ticket_id",
    ):
        op.drop_column("crm_complaint_cases", column)
