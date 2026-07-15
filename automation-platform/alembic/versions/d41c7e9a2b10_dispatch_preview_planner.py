"""dispatch preview planner

Revision ID: d41c7e9a2b10
Revises: 2a3df264ca01
Create Date: 2026-07-13 18:30:00
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


revision: str = "d41c7e9a2b10"
down_revision: Union[str, Sequence[str], None] = "2a3df264ca01"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "whatsapp_contact_links",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("directory_contact_id", sa.Uuid(), nullable=False),
        sa.Column("entity_type", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("entity_key", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("officer_id", sa.Uuid(), nullable=True),
        sa.Column("school_head_id", sa.Uuid(), nullable=True),
        sa.Column("wing_id", sa.Uuid(), nullable=False),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("source", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "(entity_type = 'officer' AND officer_id IS NOT NULL AND school_head_id IS NULL) "
            "OR (entity_type = 'school_head' AND school_head_id IS NOT NULL AND officer_id IS NULL)",
            name="ck_whatsapp_contact_links_entity",
        ),
        sa.CheckConstraint(
            "entity_type IN ('officer', 'school_head')",
            name="ck_whatsapp_contact_links_entity_type",
        ),
        sa.CheckConstraint(
            "status IN ('suggested', 'verified')",
            name="ck_whatsapp_contact_links_status",
        ),
        sa.ForeignKeyConstraint(["directory_contact_id"], ["whatsapp_directory_contacts.id"]),
        sa.ForeignKeyConstraint(["officer_id"], ["officers.id"]),
        sa.ForeignKeyConstraint(["school_head_id"], ["school_heads.id"]),
        sa.ForeignKeyConstraint(["wing_id"], ["wings.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "directory_contact_id",
            "entity_type",
            "entity_key",
            name="uq_whatsapp_contact_links_target",
        ),
    )
    for column in (
        "directory_contact_id",
        "entity_type",
        "entity_key",
        "officer_id",
        "school_head_id",
        "wing_id",
        "status",
        "source",
        "active",
    ):
        op.create_index(
            op.f(f"ix_whatsapp_contact_links_{column}"),
            "whatsapp_contact_links",
            [column],
            unique=False,
        )

    op.create_table(
        "whatsapp_dispatch_previews",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("preview_key", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("application_id", sa.Uuid(), nullable=False),
        sa.Column("source_job_id", sa.Uuid(), nullable=False),
        sa.Column("dispatch_profile_id", sa.Uuid(), nullable=False),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("profile_version", sa.Integer(), nullable=False),
        sa.Column("application_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("report_type_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("audience_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("profile_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("account_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("template_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("wing_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("ready_count", sa.Integer(), nullable=False),
        sa.Column("warning_count", sa.Integer(), nullable=False),
        sa.Column("blocked_count", sa.Integer(), nullable=False),
        sa.Column("skipped_count", sa.Integer(), nullable=False),
        sa.Column("delivery_count", sa.Integer(), nullable=False),
        sa.Column("artifact_count", sa.Integer(), nullable=False),
        sa.Column("issues", sa.JSON(), nullable=True),
        sa.Column("configuration_snapshot", sa.JSON(), nullable=True),
        sa.Column("content_sha256", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_by", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("frozen_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('ready', 'blocked', 'stale')",
            name="ck_whatsapp_dispatch_previews_status",
        ),
        sa.ForeignKeyConstraint(["application_id"], ["whatsapp_applications.id"]),
        sa.ForeignKeyConstraint(["dispatch_profile_id"], ["whatsapp_dispatch_profiles.id"]),
        sa.ForeignKeyConstraint(["source_job_id"], ["jobs.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column, unique in (
        ("preview_key", True),
        ("application_id", False),
        ("source_job_id", False),
        ("dispatch_profile_id", False),
        ("status", False),
        ("content_sha256", False),
        ("created_by", False),
        ("created_at", False),
        ("frozen_at", False),
    ):
        op.create_index(
            op.f(f"ix_whatsapp_dispatch_previews_{column}"),
            "whatsapp_dispatch_previews",
            [column],
            unique=unique,
        )

    op.create_table(
        "whatsapp_dispatch_preview_artifacts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("preview_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_id", sa.Integer(), nullable=True),
        sa.Column("report_type_id", sa.Uuid(), nullable=False),
        sa.Column("wing_id", sa.Uuid(), nullable=True),
        sa.Column("role", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("path_snapshot", sa.Text(), nullable=True),
        sa.Column("mime_type", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("checksum_sha256", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("issues", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "role IN ('delivery', 'manifest', 'audit', 'supporting')",
            name="ck_whatsapp_dispatch_preview_artifacts_role",
        ),
        sa.CheckConstraint(
            "status IN ('ready', 'warning', 'blocked')",
            name="ck_whatsapp_dispatch_preview_artifacts_status",
        ),
        sa.ForeignKeyConstraint(["artifact_id"], ["artifacts.id"]),
        sa.ForeignKeyConstraint(["preview_id"], ["whatsapp_dispatch_previews.id"]),
        sa.ForeignKeyConstraint(["report_type_id"], ["whatsapp_report_types.id"]),
        sa.ForeignKeyConstraint(["wing_id"], ["wings.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in (
        "preview_id",
        "artifact_id",
        "report_type_id",
        "wing_id",
        "role",
        "name",
        "checksum_sha256",
        "status",
        "created_at",
    ):
        op.create_index(
            op.f(f"ix_whatsapp_dispatch_preview_artifacts_{column}"),
            "whatsapp_dispatch_preview_artifacts",
            [column],
            unique=False,
        )

    op.create_table(
        "whatsapp_dispatch_preview_deliveries",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("preview_id", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("source_route_key", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("target_type", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("target_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("target_jid", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("directory_group_id", sa.Uuid(), nullable=True),
        sa.Column("directory_contact_id", sa.Uuid(), nullable=True),
        sa.Column("contact_link_id", sa.Uuid(), nullable=True),
        sa.Column("wing_id", sa.Uuid(), nullable=True),
        sa.Column("wing_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("route_kind", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("route_scope", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("attachment_ids", sa.JSON(), nullable=True),
        sa.Column("routing_snapshot", sa.JSON(), nullable=True),
        sa.Column("issues", sa.JSON(), nullable=True),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("idempotency_key", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('ready', 'warning', 'blocked', 'skipped')",
            name="ck_whatsapp_dispatch_preview_deliveries_status",
        ),
        sa.CheckConstraint(
            "target_type IN ('group', 'contact')",
            name="ck_whatsapp_dispatch_preview_deliveries_target_type",
        ),
        sa.ForeignKeyConstraint(["contact_link_id"], ["whatsapp_contact_links.id"]),
        sa.ForeignKeyConstraint(["directory_contact_id"], ["whatsapp_directory_contacts.id"]),
        sa.ForeignKeyConstraint(["directory_group_id"], ["whatsapp_directory_groups.id"]),
        sa.ForeignKeyConstraint(["preview_id"], ["whatsapp_dispatch_previews.id"]),
        sa.ForeignKeyConstraint(["wing_id"], ["wings.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "preview_id",
            "idempotency_key",
            name="uq_whatsapp_dispatch_preview_delivery_idempotency",
        ),
    )
    for column in (
        "preview_id",
        "sequence",
        "source_route_key",
        "target_type",
        "target_name",
        "target_jid",
        "directory_group_id",
        "directory_contact_id",
        "contact_link_id",
        "wing_id",
        "route_kind",
        "route_scope",
        "status",
        "idempotency_key",
        "created_at",
    ):
        op.create_index(
            op.f(f"ix_whatsapp_dispatch_preview_deliveries_{column}"),
            "whatsapp_dispatch_preview_deliveries",
            [column],
            unique=False,
        )


def downgrade() -> None:
    op.drop_table("whatsapp_dispatch_preview_deliveries")
    op.drop_table("whatsapp_dispatch_preview_artifacts")
    op.drop_table("whatsapp_dispatch_previews")
    op.drop_table("whatsapp_contact_links")
