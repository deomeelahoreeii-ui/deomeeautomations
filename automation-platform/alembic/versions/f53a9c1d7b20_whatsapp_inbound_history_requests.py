"""whatsapp inbound history request lifecycle

Revision ID: f53a9c1d7b20
Revises: e41b7d6c9a20
"""
from alembic import op
import sqlalchemy as sa

revision = "f53a9c1d7b20"
down_revision = "e41b7d6c9a20"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_inbound_history_requests",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.String(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("contact_id", sa.Uuid(), nullable=False),
        sa.Column("worker_key", sa.String(), nullable=False),
        sa.Column("requested_count", sa.Integer(), nullable=False),
        sa.Column("remote_jid", sa.String(), nullable=True),
        sa.Column("anchor_message_id", sa.String(), nullable=True),
        sa.Column("anchor_timestamp", sa.DateTime(), nullable=True),
        sa.Column("operation_id", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("baseline_messages", sa.Integer(), nullable=False),
        sa.Column("baseline_attachments", sa.Integer(), nullable=False),
        sa.Column("messages_received", sa.Integer(), nullable=False),
        sa.Column("attachments_discovered", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("requested_at", sa.DateTime(), nullable=False),
        sa.Column("accepted_at", sa.DateTime(), nullable=True),
        sa.Column("last_activity_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('requested', 'accepted', 'syncing', 'succeeded', 'no_results', 'failed', 'timed_out')",
            name="ck_whatsapp_inbound_history_requests_status",
        ),
        sa.ForeignKeyConstraint(["account_id"], ["whatsapp_accounts.id"]),
        sa.ForeignKeyConstraint(["contact_id"], ["whatsapp_directory_contacts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("request_id"),
    )
    for column in [
        "request_id", "account_id", "contact_id", "worker_key", "remote_jid",
        "anchor_message_id", "anchor_timestamp", "operation_id", "status",
        "requested_at", "accepted_at", "last_activity_at", "finished_at", "updated_at",
    ]:
        op.create_index(
            f"ix_whatsapp_inbound_history_requests_{column}",
            "whatsapp_inbound_history_requests",
            [column],
        )


def downgrade() -> None:
    op.drop_table("whatsapp_inbound_history_requests")
