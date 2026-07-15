"""whatsapp inbound media exports

Revision ID: e41b7d6c9a20
Revises: a31f7c9d2e44
"""
from alembic import op
import sqlalchemy as sa

revision = "e41b7d6c9a20"
down_revision = "a31f7c9d2e44"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("whatsapp_inbound_attachments", sa.Column("detected_mime_type", sa.String(), nullable=True))
    op.add_column("whatsapp_inbound_attachments", sa.Column("media_category", sa.String(), nullable=True))
    op.add_column("whatsapp_inbound_attachments", sa.Column("safe_extension", sa.String(), nullable=True))
    op.add_column("whatsapp_inbound_attachments", sa.Column("actual_size", sa.Integer(), nullable=True))
    op.add_column("whatsapp_inbound_attachments", sa.Column("actual_sha256", sa.String(), nullable=True))
    op.add_column("whatsapp_inbound_attachments", sa.Column("stored_path", sa.Text(), nullable=True))
    op.add_column("whatsapp_inbound_attachments", sa.Column("archived_at", sa.DateTime(), nullable=True))
    for column in ["detected_mime_type", "media_category", "safe_extension", "actual_sha256", "archived_at"]:
        op.create_index(f"ix_whatsapp_inbound_attachments_{column}", "whatsapp_inbound_attachments", [column])

    op.create_table(
        "whatsapp_inbound_export_runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("job_id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("contact_id", sa.Uuid(), nullable=False),
        sa.Column("contact_name", sa.String(), nullable=False),
        sa.Column("date_from", sa.DateTime(), nullable=True),
        sa.Column("date_to", sa.DateTime(), nullable=True),
        sa.Column("chat_scope", sa.String(), nullable=False),
        sa.Column("media_types", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("files_matched", sa.Integer(), nullable=False),
        sa.Column("files_downloaded", sa.Integer(), nullable=False),
        sa.Column("files_reused", sa.Integer(), nullable=False),
        sa.Column("files_unavailable", sa.Integer(), nullable=False),
        sa.Column("total_bytes", sa.Integer(), nullable=False),
        sa.Column("coverage_earliest_at", sa.DateTime(), nullable=True),
        sa.Column("coverage_latest_at", sa.DateTime(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint("chat_scope IN ('direct', 'direct_and_groups')", name="ck_whatsapp_inbound_export_runs_scope"),
        sa.ForeignKeyConstraint(["account_id"], ["whatsapp_accounts.id"]),
        sa.ForeignKeyConstraint(["contact_id"], ["whatsapp_directory_contacts.id"]),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("job_id"),
    )
    for column in ["job_id", "account_id", "contact_id", "contact_name", "date_from", "date_to", "chat_scope", "status", "created_at", "started_at", "finished_at", "updated_at"]:
        op.create_index(f"ix_whatsapp_inbound_export_runs_{column}", "whatsapp_inbound_export_runs", [column])

    op.create_table(
        "whatsapp_inbound_export_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("export_run_id", sa.Uuid(), nullable=False),
        sa.Column("attachment_id", sa.Uuid(), nullable=False),
        sa.Column("media_category", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("output_path", sa.Text(), nullable=True),
        sa.Column("output_name", sa.String(), nullable=True),
        sa.Column("duplicate_of_item_id", sa.Uuid(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["attachment_id"], ["whatsapp_inbound_attachments.id"]),
        sa.ForeignKeyConstraint(["duplicate_of_item_id"], ["whatsapp_inbound_export_items.id"]),
        sa.ForeignKeyConstraint(["export_run_id"], ["whatsapp_inbound_export_runs.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("export_run_id", "attachment_id", name="uq_whatsapp_inbound_export_items_run_attachment"),
    )
    for column in ["export_run_id", "attachment_id", "media_category", "status", "duplicate_of_item_id", "created_at", "updated_at"]:
        op.create_index(f"ix_whatsapp_inbound_export_items_{column}", "whatsapp_inbound_export_items", [column])


def downgrade() -> None:
    op.drop_table("whatsapp_inbound_export_items")
    op.drop_table("whatsapp_inbound_export_runs")
    for column in ["archived_at", "actual_sha256", "safe_extension", "media_category", "detected_mime_type"]:
        op.drop_index(f"ix_whatsapp_inbound_attachments_{column}", table_name="whatsapp_inbound_attachments")
    for column in ["archived_at", "stored_path", "actual_sha256", "actual_size", "safe_extension", "media_category", "detected_mime_type"]:
        op.drop_column("whatsapp_inbound_attachments", column)
