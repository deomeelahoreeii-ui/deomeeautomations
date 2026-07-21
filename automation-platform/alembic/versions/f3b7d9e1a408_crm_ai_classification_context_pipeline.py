"""Add versioned SOP, controlled tags, AI classification and reply context lineage.

Revision ID: f3b7d9e1a408
Revises: f1a5c7e9b306
Create Date: 2026-07-21
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f3b7d9e1a408"
down_revision: Union[str, None] = "f1a5c7e9b306"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _index(table: str, *names: str) -> None:
    for name in names:
        op.create_index(f"ix_{table}_{name}", table, [name])


def upgrade() -> None:
    op.drop_constraint(
        "ck_crm_bulk_operation_batches_type",
        "crm_bulk_operation_batches",
        type_="check",
    )
    op.create_check_constraint(
        "ck_crm_bulk_operation_batches_type",
        "crm_bulk_operation_batches",
        "operation_type IN ('classification_export', 'classification_import', "
        "'reply_context_export', 'reply_export', 'reply_import', 'formal_letters')",
    )
    op.drop_constraint(
        "ck_crm_bulk_operation_items_status",
        "crm_bulk_operation_items",
        type_="check",
    )
    op.create_check_constraint(
        "ck_crm_bulk_operation_items_status",
        "crm_bulk_operation_items",
        "status IN ('pending', 'exported', 'valid', 'invalid', 'imported', 'updated', "
        "'unchanged', 'generated', 'classified', 'review_required', 'approved', "
        "'rejected', 'suggested', 'failed', 'skipped', 'missing')",
    )

    op.create_table(
        "crm_prompt_profiles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(length=120), nullable=False),
        sa.Column("name", sa.String(length=180), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_crm_prompt_profiles_slug"),
    )
    _index("crm_prompt_profiles", "slug", "name", "active", "created_at", "updated_at")

    op.create_table(
        "crm_prompt_profile_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("profile_id", sa.Uuid(), nullable=False),
        sa.Column("version_label", sa.String(length=80), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("structured_json", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("effective_from", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(length=120), nullable=False, server_default="system"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["profile_id"], ["crm_prompt_profiles.id"],
            name="fk_crm_prompt_profile_versions_profile", ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "profile_id", "version_label", name="uq_crm_prompt_profile_versions_label"
        ),
    )
    _index(
        "crm_prompt_profile_versions",
        "profile_id", "version_label", "content_sha256", "is_active",
        "effective_from", "created_by", "created_at",
    )

    op.create_table(
        "crm_complaint_tags",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("normalized_name", sa.String(length=120), nullable=False),
        sa.Column("group_name", sa.String(length=80), nullable=False, server_default="issue"),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "normalized_name", name="uq_crm_complaint_tags_normalized_name"
        ),
    )
    _index(
        "crm_complaint_tags",
        "name", "normalized_name", "group_name", "active", "created_at", "updated_at",
    )

    op.create_table(
        "crm_complaint_classifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=True),
        sa.Column("suggested_category_id", sa.Uuid(), nullable=True),
        sa.Column("suggested_subcategory_id", sa.Uuid(), nullable=True),
        sa.Column("suggested_category_name", sa.String(length=140), nullable=True),
        sa.Column("suggested_subcategory_name", sa.String(length=140), nullable=True),
        sa.Column("approved_category_id", sa.Uuid(), nullable=True),
        sa.Column("approved_subcategory_id", sa.Uuid(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="0"),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("tags_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("decision_source", sa.String(length=40), nullable=False, server_default="ai_csv"),
        sa.Column("taxonomy_snapshot_json", sa.JSON(), nullable=False),
        sa.Column("decided_by", sa.String(length=120), nullable=True),
        sa.Column("decided_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "status IN ('pending', 'auto_accepted', 'review_required', 'approved', "
            "'rejected', 'superseded')",
            name="ck_crm_complaint_classifications_status",
        ),
        sa.ForeignKeyConstraint(
            ["complaint_case_id"], ["crm_complaint_cases.id"],
            name="fk_crm_complaint_classifications_case", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"], ["crm_bulk_operation_batches.id"],
            name="fk_crm_complaint_classifications_batch", ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["suggested_category_id"], ["crm_complaint_categories.id"],
            name="fk_crm_complaint_classifications_suggested_category", ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["suggested_subcategory_id"], ["crm_complaint_subcategories.id"],
            name="fk_crm_complaint_classifications_suggested_subcategory", ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["approved_category_id"], ["crm_complaint_categories.id"],
            name="fk_crm_complaint_classifications_approved_category", ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["approved_subcategory_id"], ["crm_complaint_subcategories.id"],
            name="fk_crm_complaint_classifications_approved_subcategory", ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    _index(
        "crm_complaint_classifications",
        "complaint_case_id", "batch_id", "suggested_category_id",
        "suggested_subcategory_id", "suggested_category_name",
        "suggested_subcategory_name", "approved_category_id",
        "approved_subcategory_id", "status", "decision_source", "decided_by",
        "decided_at", "created_at", "updated_at",
    )

    op.create_table(
        "crm_complaint_tag_links",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("tag_id", sa.Uuid(), nullable=False),
        sa.Column("classification_id", sa.Uuid(), nullable=True),
        sa.Column("source", sa.String(length=40), nullable=False, server_default="manual"),
        sa.Column("created_by", sa.String(length=120), nullable=False, server_default="web-operator"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["complaint_case_id"], ["crm_complaint_cases.id"],
            name="fk_crm_complaint_tag_links_case", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tag_id"], ["crm_complaint_tags.id"],
            name="fk_crm_complaint_tag_links_tag", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["classification_id"], ["crm_complaint_classifications.id"],
            name="fk_crm_complaint_tag_links_classification", ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "complaint_case_id", "tag_id", name="uq_crm_complaint_tag_links_case_tag"
        ),
    )
    _index(
        "crm_complaint_tag_links",
        "complaint_case_id", "tag_id", "classification_id", "source",
        "created_by", "created_at",
    )

    op.create_table(
        "crm_taxonomy_suggestions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("classification_id", sa.Uuid(), nullable=False),
        sa.Column("proposal_type", sa.String(length=24), nullable=False),
        sa.Column("parent_category_id", sa.Uuid(), nullable=True),
        sa.Column("proposed_name", sa.String(length=140), nullable=False),
        sa.Column("normalized_name", sa.String(length=140), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("supporting_count", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("status", sa.String(length=24), nullable=False, server_default="pending"),
        sa.Column("resolved_category_id", sa.Uuid(), nullable=True),
        sa.Column("resolved_subcategory_id", sa.Uuid(), nullable=True),
        sa.Column("resolved_tag_id", sa.Uuid(), nullable=True),
        sa.Column("decided_by", sa.String(length=120), nullable=True),
        sa.Column("decided_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "proposal_type IN ('category', 'subcategory', 'tag')",
            name="ck_crm_taxonomy_suggestions_type",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'approved', 'merged', 'rejected', 'deferred')",
            name="ck_crm_taxonomy_suggestions_status",
        ),
        sa.ForeignKeyConstraint(
            ["classification_id"], ["crm_complaint_classifications.id"],
            name="fk_crm_taxonomy_suggestions_classification", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["parent_category_id"], ["crm_complaint_categories.id"],
            name="fk_crm_taxonomy_suggestions_parent_category", ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["resolved_category_id"], ["crm_complaint_categories.id"],
            name="fk_crm_taxonomy_suggestions_resolved_category", ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["resolved_subcategory_id"], ["crm_complaint_subcategories.id"],
            name="fk_crm_taxonomy_suggestions_resolved_subcategory", ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["resolved_tag_id"], ["crm_complaint_tags.id"],
            name="fk_crm_taxonomy_suggestions_resolved_tag", ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    _index(
        "crm_taxonomy_suggestions",
        "classification_id", "proposal_type", "parent_category_id",
        "proposed_name", "normalized_name", "status", "resolved_category_id",
        "resolved_subcategory_id", "resolved_tag_id", "decided_by", "decided_at",
        "created_at",
    )

    op.create_table(
        "crm_reply_context_examples",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=False),
        sa.Column("example_case_id", sa.Uuid(), nullable=False),
        sa.Column("reply_revision_id", sa.Uuid(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("matched_by", sa.String(length=80), nullable=False, server_default="subcategory"),
        sa.Column("snapshot_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["batch_id"], ["crm_bulk_operation_batches.id"],
            name="fk_crm_reply_context_examples_batch", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["complaint_case_id"], ["crm_complaint_cases.id"],
            name="fk_crm_reply_context_examples_case", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["example_case_id"], ["crm_complaint_cases.id"],
            name="fk_crm_reply_context_examples_example_case", ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["reply_revision_id"], ["crm_complaint_reply_revisions.id"],
            name="fk_crm_reply_context_examples_revision", ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "batch_id", "complaint_case_id", "reply_revision_id",
            name="uq_crm_reply_context_examples_batch_case_revision",
        ),
    )
    _index(
        "crm_reply_context_examples",
        "batch_id", "complaint_case_id", "example_case_id", "reply_revision_id",
        "matched_by", "created_at",
    )


def downgrade() -> None:
    op.drop_table("crm_reply_context_examples")
    op.drop_table("crm_taxonomy_suggestions")
    op.drop_table("crm_complaint_tag_links")
    op.drop_table("crm_complaint_classifications")
    op.drop_table("crm_complaint_tags")
    op.drop_table("crm_prompt_profile_versions")
    op.drop_table("crm_prompt_profiles")

    op.drop_constraint(
        "ck_crm_bulk_operation_items_status",
        "crm_bulk_operation_items",
        type_="check",
    )
    op.create_check_constraint(
        "ck_crm_bulk_operation_items_status",
        "crm_bulk_operation_items",
        "status IN ('pending', 'exported', 'valid', 'invalid', 'imported', 'updated', "
        "'unchanged', 'generated', 'failed', 'skipped', 'missing')",
    )
    op.drop_constraint(
        "ck_crm_bulk_operation_batches_type",
        "crm_bulk_operation_batches",
        type_="check",
    )
    op.create_check_constraint(
        "ck_crm_bulk_operation_batches_type",
        "crm_bulk_operation_batches",
        "operation_type IN ('reply_export', 'reply_import', 'formal_letters')",
    )
