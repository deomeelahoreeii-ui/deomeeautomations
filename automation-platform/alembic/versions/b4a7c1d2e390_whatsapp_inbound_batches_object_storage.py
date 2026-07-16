"""WhatsApp inbound batches and S3-compatible object storage ledger.

Revision ID: b4a7c1d2e390
Revises: 9b7e4c2d1a60
Create Date: 2026-07-17
"""

from alembic import op
import sqlalchemy as sa

revision = "b4a7c1d2e390"
down_revision = "9b7e4c2d1a60"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_inbound_stored_objects",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("backend", sa.String(), nullable=False),
        sa.Column("bucket", sa.String(), nullable=False),
        sa.Column("object_key", sa.String(), nullable=False),
        sa.Column("sha256", sa.String(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("content_type", sa.String(), nullable=True),
        sa.Column("etag", sa.String(), nullable=True),
        sa.Column("version_id", sa.String(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("verified_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "backend", "bucket", "object_key",
            name="uq_whatsapp_inbound_stored_object_location",
        ),
    )
    for column in [
        "backend", "bucket", "object_key", "sha256", "content_type", "etag",
        "version_id", "status", "created_at", "verified_at",
    ]:
        op.create_index(
            f"ix_whatsapp_inbound_stored_objects_{column}",
            "whatsapp_inbound_stored_objects",
            [column],
        )

    op.create_table(
        "whatsapp_inbound_batches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_code", sa.String(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("contact_id", sa.Uuid(), nullable=False),
        sa.Column("worker_key", sa.String(), nullable=False),
        sa.Column("provider", sa.String(), nullable=False),
        sa.Column("requested_count", sa.Integer(), nullable=False),
        sa.Column("remote_jid", sa.String(), nullable=True),
        sa.Column("anchor_message_id", sa.String(), nullable=True),
        sa.Column("anchor_timestamp", sa.DateTime(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("messages_discovered", sa.Integer(), nullable=False),
        sa.Column("files_discovered", sa.Integer(), nullable=False),
        sa.Column("files_stored", sa.Integer(), nullable=False),
        sa.Column("files_reused", sa.Integer(), nullable=False),
        sa.Column("files_failed", sa.Integer(), nullable=False),
        sa.Column("total_bytes", sa.Integer(), nullable=False),
        sa.Column("storage_backend", sa.String(), nullable=False),
        sa.Column("raw_bucket", sa.String(), nullable=True),
        sa.Column("manifest_bucket", sa.String(), nullable=True),
        sa.Column("manifest_object_key", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('created', 'fetching', 'storing', 'completed', 'completed_with_errors', 'failed', 'cancelled')",
            name="ck_whatsapp_inbound_batches_status",
        ),
        sa.ForeignKeyConstraint(["account_id"], ["whatsapp_accounts.id"]),
        sa.ForeignKeyConstraint(["contact_id"], ["whatsapp_directory_contacts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("batch_code"),
    )
    for column in [
        "batch_code", "account_id", "contact_id", "worker_key", "provider",
        "remote_jid", "anchor_message_id", "anchor_timestamp", "status",
        "storage_backend", "raw_bucket", "manifest_bucket", "created_at",
        "started_at", "finished_at", "updated_at",
    ]:
        op.create_index(
            f"ix_whatsapp_inbound_batches_{column}",
            "whatsapp_inbound_batches",
            [column],
        )

    with op.batch_alter_table("whatsapp_inbound_history_requests") as batch_op:
        batch_op.add_column(sa.Column("batch_id", sa.Uuid(), nullable=True))
        batch_op.create_foreign_key(
            "fk_whatsapp_inbound_history_requests_batch_id",
            "whatsapp_inbound_batches",
            ["batch_id"],
            ["id"],
        )
        batch_op.create_index(
            "ix_whatsapp_inbound_history_requests_batch_id",
            ["batch_id"],
            unique=True,
        )

    with op.batch_alter_table("whatsapp_inbound_attachments") as batch_op:
        batch_op.add_column(sa.Column("stored_object_id", sa.Uuid(), nullable=True))
        batch_op.add_column(
            sa.Column("storage_status", sa.String(), nullable=False, server_default="local_only")
        )
        batch_op.add_column(sa.Column("storage_error", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("storage_uploaded_at", sa.DateTime(), nullable=True))
        batch_op.create_foreign_key(
            "fk_whatsapp_inbound_attachments_stored_object_id",
            "whatsapp_inbound_stored_objects",
            ["stored_object_id"],
            ["id"],
        )
        batch_op.create_index(
            "ix_whatsapp_inbound_attachments_stored_object_id",
            ["stored_object_id"],
            unique=False,
        )
        batch_op.create_index(
            "ix_whatsapp_inbound_attachments_storage_status",
            ["storage_status"],
            unique=False,
        )
        batch_op.create_index(
            "ix_whatsapp_inbound_attachments_storage_uploaded_at",
            ["storage_uploaded_at"],
            unique=False,
        )

    op.create_table(
        "whatsapp_inbound_batch_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("attachment_id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("stored_object_id", sa.Uuid(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("original_filename", sa.String(), nullable=True),
        sa.Column("message_timestamp", sa.DateTime(), nullable=True),
        sa.Column("mime_type", sa.String(), nullable=True),
        sa.Column("sha256", sa.String(), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("object_key", sa.Text(), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("stored_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('discovered', 'downloading', 'hashing', 'uploading', 'stored', 'already_stored', 'storage_pending', 'unsupported', 'failed')",
            name="ck_whatsapp_inbound_batch_items_status",
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["whatsapp_inbound_batches.id"]),
        sa.ForeignKeyConstraint(["attachment_id"], ["whatsapp_inbound_attachments.id"]),
        sa.ForeignKeyConstraint(["message_id"], ["whatsapp_inbound_messages.id"]),
        sa.ForeignKeyConstraint(["stored_object_id"], ["whatsapp_inbound_stored_objects.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "batch_id", "attachment_id",
            name="uq_whatsapp_inbound_batch_item_attachment",
        ),
    )
    for column in [
        "batch_id", "attachment_id", "message_id", "stored_object_id", "status",
        "original_filename", "message_timestamp", "mime_type", "sha256",
        "created_at", "stored_at", "updated_at",
    ]:
        op.create_index(
            f"ix_whatsapp_inbound_batch_items_{column}",
            "whatsapp_inbound_batch_items",
            [column],
        )


def downgrade() -> None:
    op.drop_table("whatsapp_inbound_batch_items")
    with op.batch_alter_table("whatsapp_inbound_attachments") as batch_op:
        batch_op.drop_index("ix_whatsapp_inbound_attachments_storage_uploaded_at")
        batch_op.drop_index("ix_whatsapp_inbound_attachments_storage_status")
        batch_op.drop_index("ix_whatsapp_inbound_attachments_stored_object_id")
        batch_op.drop_constraint(
            "fk_whatsapp_inbound_attachments_stored_object_id", type_="foreignkey"
        )
        batch_op.drop_column("storage_uploaded_at")
        batch_op.drop_column("storage_error")
        batch_op.drop_column("storage_status")
        batch_op.drop_column("stored_object_id")
    with op.batch_alter_table("whatsapp_inbound_history_requests") as batch_op:
        batch_op.drop_index("ix_whatsapp_inbound_history_requests_batch_id")
        batch_op.drop_constraint(
            "fk_whatsapp_inbound_history_requests_batch_id", type_="foreignkey"
        )
        batch_op.drop_column("batch_id")
    op.drop_table("whatsapp_inbound_batches")
    op.drop_table("whatsapp_inbound_stored_objects")
