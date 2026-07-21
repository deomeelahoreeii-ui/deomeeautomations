"""Add official-letter templates, signatures, settings, revisions and artifacts.

Revision ID: f9e3a5c7d064
Revises: f7d1e3a5c842
Create Date: 2026-07-21
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "f9e3a5c7d064"
down_revision: Union[str, None] = "f7d1e3a5c842"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "crm_official_letter_settings",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("singleton_key", sa.String(length=40), nullable=False),
        sa.Column("office_title", sa.Text(), nullable=False),
        sa.Column("default_recipient_name", sa.String(length=240), nullable=False),
        sa.Column("default_recipient_location", sa.String(length=160), nullable=False),
        sa.Column("default_subject_prefix", sa.String(length=160), nullable=False),
        sa.Column("default_cc_entries", sa.Text(), nullable=False),
        sa.Column("date_format", sa.String(length=40), nullable=False),
        sa.Column("numbering_prefix", sa.String(length=120), nullable=False),
        sa.Column("last_numeric_number", sa.Integer(), nullable=False),
        sa.Column("allow_manual_override", sa.Boolean(), nullable=False),
        sa.Column("require_unique_number", sa.Boolean(), nullable=False),
        sa.Column("default_template_id", sa.Uuid(), nullable=True),
        sa.Column("default_signature_id", sa.Uuid(), nullable=True),
        sa.Column("updated_by", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("singleton_key", name="uq_crm_official_letter_settings_singleton"),
    )
    for column in ("singleton_key", "default_template_id", "default_signature_id", "updated_by", "created_at", "updated_at"):
        op.create_index(f"ix_crm_official_letter_settings_{column}", "crm_official_letter_settings", [column])

    op.create_table(
        "crm_official_letter_templates",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("template_key", sa.String(length=120), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("signature_target_path", sa.String(length=500), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False),
        sa.Column("created_by", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("template_key", "version", name="uq_crm_official_letter_template_version"),
    )
    for column in ("template_key", "name", "sha256", "active", "is_default", "created_by", "created_at"):
        op.create_index(f"ix_crm_official_letter_templates_{column}", "crm_official_letter_templates", [column])

    op.create_table(
        "crm_official_letter_signature_profiles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("officer_name", sa.String(length=200), nullable=True),
        sa.Column("designation", sa.String(length=240), nullable=False),
        sa.Column("office_location", sa.String(length=160), nullable=False),
        sa.Column("image_path", sa.Text(), nullable=False),
        sa.Column("image_sha256", sa.String(length=64), nullable=False),
        sa.Column("image_size_bytes", sa.Integer(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("is_default", sa.Boolean(), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=True),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("created_by", sa.String(length=120), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in ("name", "image_sha256", "active", "is_default", "effective_from", "effective_to", "created_by", "created_at", "updated_at"):
        op.create_index(f"ix_crm_official_letter_signature_profiles_{column}", "crm_official_letter_signature_profiles", [column])

    op.create_table(
        "crm_official_letters",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("reply_revision_id", sa.Uuid(), nullable=False),
        sa.Column("template_id", sa.Uuid(), nullable=False),
        sa.Column("signature_profile_id", sa.Uuid(), nullable=False),
        sa.Column("letter_number", sa.String(length=180), nullable=False),
        sa.Column("letter_date", sa.Date(), nullable=False),
        sa.Column("recipient_name", sa.String(length=240), nullable=False),
        sa.Column("recipient_location", sa.String(length=160), nullable=False),
        sa.Column("subject_prefix", sa.String(length=160), nullable=False),
        sa.Column("cc_entries", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=24), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("supersedes_letter_id", sa.Uuid(), nullable=True),
        sa.Column("complaint_number_snapshot", sa.String(length=80), nullable=False),
        sa.Column("complaint_text_snapshot", sa.Text(), nullable=False),
        sa.Column("reply_text_snapshot", sa.Text(), nullable=False),
        sa.Column("configuration_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("generated_by", sa.String(length=120), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("generated_at", sa.DateTime(), nullable=True),
        sa.Column("finalized_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint("status IN ('preview', 'finalized', 'superseded', 'failed')", name="ck_crm_official_letters_status"),
        sa.ForeignKeyConstraint(["complaint_case_id"], ["crm_complaint_cases.id"]),
        sa.ForeignKeyConstraint(["reply_revision_id"], ["crm_complaint_reply_revisions.id"]),
        sa.ForeignKeyConstraint(["template_id"], ["crm_official_letter_templates.id"]),
        sa.ForeignKeyConstraint(["signature_profile_id"], ["crm_official_letter_signature_profiles.id"]),
        sa.ForeignKeyConstraint(["supersedes_letter_id"], ["crm_official_letters.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("letter_number", name="uq_crm_official_letters_number"),
        sa.UniqueConstraint("complaint_case_id", "revision", name="uq_crm_official_letters_case_revision"),
    )
    for column in ("complaint_case_id", "reply_revision_id", "template_id", "signature_profile_id", "letter_number", "letter_date", "status", "supersedes_letter_id", "complaint_number_snapshot", "generated_by", "created_at", "generated_at", "finalized_at"):
        op.create_index(f"ix_crm_official_letters_{column}", "crm_official_letters", [column])

    op.create_table(
        "crm_official_letter_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("official_letter_id", sa.Uuid(), nullable=False),
        sa.Column("kind", sa.String(length=40), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("path", sa.Text(), nullable=False),
        sa.Column("content_type", sa.String(length=160), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["official_letter_id"], ["crm_official_letters.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("official_letter_id", "kind", name="uq_crm_official_letter_artifact_kind"),
    )
    for column in ("official_letter_id", "kind", "sha256", "created_at"):
        op.create_index(f"ix_crm_official_letter_artifacts_{column}", "crm_official_letter_artifacts", [column])


def downgrade() -> None:
    op.drop_table("crm_official_letter_artifacts")
    op.drop_table("crm_official_letters")
    op.drop_table("crm_official_letter_signature_profiles")
    op.drop_table("crm_official_letter_templates")
    op.drop_table("crm_official_letter_settings")
