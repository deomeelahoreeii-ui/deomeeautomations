"""whatsapp inbound capture foundation

Revision ID: a31f7c9d2e44
Revises: c9d8e7f6a501
"""
from alembic import op
import sqlalchemy as sa

revision = "a31f7c9d2e44"
down_revision = "c9d8e7f6a501"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_inbound_messages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("worker_key", sa.String(), nullable=False),
        sa.Column("message_id", sa.String(), nullable=False),
        sa.Column("remote_jid", sa.String(), nullable=False),
        sa.Column("participant_jid", sa.String(), nullable=True),
        sa.Column("sender_jid", sa.String(), nullable=False),
        sa.Column("directory_contact_id", sa.Uuid(), nullable=True),
        sa.Column("from_me", sa.Boolean(), nullable=False),
        sa.Column("chat_scope", sa.String(), nullable=False),
        sa.Column("message_timestamp", sa.DateTime(), nullable=False),
        sa.Column("push_name", sa.String(), nullable=True),
        sa.Column("text_content", sa.Text(), nullable=True),
        sa.Column("message_type", sa.String(), nullable=False),
        sa.Column("ingestion_source", sa.String(), nullable=False),
        sa.Column("payload_sha256", sa.String(), nullable=False),
        sa.Column("raw_payload", sa.JSON(), nullable=False),
        sa.Column("first_ingested_at", sa.DateTime(), nullable=False),
        sa.Column("last_ingested_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["account_id"], ["whatsapp_accounts.id"]),
        sa.ForeignKeyConstraint(["directory_contact_id"], ["whatsapp_directory_contacts.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("account_id", "remote_jid", "message_id", name="uq_whatsapp_inbound_message_identity"),
    )
    for column in ["account_id", "worker_key", "message_id", "remote_jid", "participant_jid", "sender_jid", "directory_contact_id", "from_me", "chat_scope", "message_timestamp", "push_name", "message_type", "ingestion_source", "payload_sha256", "first_ingested_at", "last_ingested_at"]:
        op.create_index(f"ix_whatsapp_inbound_messages_{column}", "whatsapp_inbound_messages", [column])

    op.create_table(
        "whatsapp_inbound_attachments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("media_kind", sa.String(), nullable=False),
        sa.Column("message_key", sa.String(), nullable=False),
        sa.Column("original_filename", sa.String(), nullable=True),
        sa.Column("mime_type", sa.String(), nullable=True),
        sa.Column("declared_size", sa.Integer(), nullable=True),
        sa.Column("media_sha256", sa.String(), nullable=True),
        sa.Column("caption", sa.Text(), nullable=True),
        sa.Column("download_status", sa.String(), nullable=False),
        sa.Column("download_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["message_id"], ["whatsapp_inbound_messages.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", name="uq_whatsapp_inbound_attachment_message"),
    )
    for column in ["message_id", "media_kind", "message_key", "original_filename", "mime_type", "media_sha256", "download_status", "created_at", "updated_at"]:
        op.create_index(f"ix_whatsapp_inbound_attachments_{column}", "whatsapp_inbound_attachments", [column])


def downgrade() -> None:
    op.drop_table("whatsapp_inbound_attachments")
    op.drop_table("whatsapp_inbound_messages")
