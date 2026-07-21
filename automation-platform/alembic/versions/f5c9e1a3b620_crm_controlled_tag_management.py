"""Add controlled tag groups, aliases, AI visibility and merge history.

Revision ID: f5c9e1a3b620
Revises: f3b7d9e1a408
Create Date: 2026-07-21
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "f5c9e1a3b620"
down_revision: Union[str, None] = "f3b7d9e1a408"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "crm_complaint_tag_groups",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("slug", sa.String(length=80), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug", name="uq_crm_complaint_tag_groups_slug"),
    )
    for column in ("slug", "name", "display_order", "active", "created_at", "updated_at"):
        op.create_index(f"ix_crm_complaint_tag_groups_{column}", "crm_complaint_tag_groups", [column])

    op.add_column(
        "crm_taxonomy_suggestions",
        sa.Column("proposed_group_name", sa.String(length=80), nullable=True),
    )
    op.create_index(
        "ix_crm_taxonomy_suggestions_proposed_group_name",
        "crm_taxonomy_suggestions",
        ["proposed_group_name"],
    )

    op.add_column("crm_complaint_tags", sa.Column("display_name", sa.String(length=140), nullable=True))
    op.add_column(
        "crm_complaint_tags",
        sa.Column("aliases_json", sa.JSON(), nullable=False, server_default=sa.text("'[]'::json")),
    )
    op.add_column(
        "crm_complaint_tags",
        sa.Column("ai_available", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.add_column("crm_complaint_tags", sa.Column("merged_into_id", sa.Uuid(), nullable=True))
    op.add_column(
        "crm_complaint_tags",
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.create_foreign_key(
        "fk_crm_complaint_tags_merged_into",
        "crm_complaint_tags",
        "crm_complaint_tags",
        ["merged_into_id"],
        ["id"],
        ondelete="SET NULL",
    )
    for column in ("display_name", "ai_available", "merged_into_id"):
        op.create_index(f"ix_crm_complaint_tags_{column}", "crm_complaint_tags", [column])

    connection = op.get_bind()
    connection.execute(
        sa.text(
            "UPDATE crm_complaint_tags "
            "SET display_name = initcap(replace(name, '-', ' ')) "
            "WHERE display_name IS NULL OR btrim(display_name) = ''"
        )
    )
    op.alter_column("crm_complaint_tags", "display_name", nullable=False)

    now = datetime.utcnow()
    groups = (
        ("institution", "Institution", "Institution or provider type", 10),
        ("issue", "Issue", "Primary or secondary complaint issue", 20),
        ("evidence", "Evidence", "Evidence supplied or missing", 30),
        ("routing", "Routing", "Jurisdiction and referral destination", 40),
        ("handling", "Handling", "Administrative handling and review context", 50),
        ("outcome", "Outcome", "Resolution and disposal outcome", 60),
    )
    table = sa.table(
        "crm_complaint_tag_groups",
        sa.column("id", sa.Uuid()),
        sa.column("slug", sa.String()),
        sa.column("name", sa.String()),
        sa.column("description", sa.Text()),
        sa.column("display_order", sa.Integer()),
        sa.column("active", sa.Boolean()),
        sa.column("created_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
    )
    op.bulk_insert(
        table,
        [
            {
                "id": uuid.uuid4(),
                "slug": slug,
                "name": name,
                "description": description,
                "display_order": order,
                "active": True,
                "created_at": now,
                "updated_at": now,
            }
            for slug, name, description, order in groups
        ],
    )
    existing_groups = [row[0] for row in connection.execute(sa.text("SELECT DISTINCT group_name FROM crm_complaint_tags"))]
    known = {row[0] for row in groups}
    extras = [str(value).strip() for value in existing_groups if value and str(value).strip() not in known]
    if extras:
        op.bulk_insert(
            table,
            [
                {
                    "id": uuid.uuid4(),
                    "slug": slug,
                    "name": slug.replace("_", " ").replace("-", " ").title(),
                    "description": "Existing controlled tag group",
                    "display_order": 100 + index,
                    "active": True,
                    "created_at": now,
                    "updated_at": now,
                }
                for index, slug in enumerate(sorted(set(extras)))
            ],
        )


def downgrade() -> None:
    for column in ("merged_into_id", "ai_available", "display_name"):
        op.drop_index(f"ix_crm_complaint_tags_{column}", table_name="crm_complaint_tags")
    op.drop_constraint("fk_crm_complaint_tags_merged_into", "crm_complaint_tags", type_="foreignkey")
    op.drop_column("crm_complaint_tags", "version")
    op.drop_column("crm_complaint_tags", "merged_into_id")
    op.drop_column("crm_complaint_tags", "ai_available")
    op.drop_column("crm_complaint_tags", "aliases_json")
    op.drop_column("crm_complaint_tags", "display_name")
    op.drop_index(
        "ix_crm_taxonomy_suggestions_proposed_group_name",
        table_name="crm_taxonomy_suggestions",
    )
    op.drop_column("crm_taxonomy_suggestions", "proposed_group_name")
    op.drop_table("crm_complaint_tag_groups")
