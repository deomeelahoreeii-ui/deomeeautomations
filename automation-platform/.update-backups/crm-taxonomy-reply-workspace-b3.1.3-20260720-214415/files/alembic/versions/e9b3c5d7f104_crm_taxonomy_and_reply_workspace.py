"""Add complaint taxonomy, reply workspace snapshots and audit events.

Revision ID: e9b3c5d7f104
Revises: d6e9f2a4b705
Create Date: 2026-07-20
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "e9b3c5d7f104"
down_revision: Union[str, None] = "d6e9f2a4b705"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_NAMESPACE = uuid.UUID("796499d8-96b3-4ff5-a791-1c9d37d9be55")


def _clean(value: object) -> str:
    return " ".join(str(value or "").split()).strip()


def _normalized(value: object) -> str:
    return _clean(value).casefold()


def _category_id(name: str) -> uuid.UUID:
    return uuid.uuid5(_NAMESPACE, f"category:{_normalized(name)}")


def _subcategory_id(category_name: str, name: str) -> uuid.UUID:
    return uuid.uuid5(
        _NAMESPACE,
        f"subcategory:{_normalized(category_name)}:{_normalized(name)}",
    )


def upgrade() -> None:
    op.create_table(
        "crm_complaint_categories",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=140), nullable=False),
        sa.Column("normalized_name", sa.String(length=140), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("default_ai_eligible", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("reply_guidance", sa.Text(), nullable=True),
        sa.Column("policy_notes", sa.Text(), nullable=True),
        sa.Column("frappe_ticket_type_name", sa.String(length=140), nullable=False, server_default=""),
        sa.Column("frappe_sync_status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("frappe_last_synced_at", sa.DateTime(), nullable=True),
        sa.Column("frappe_last_error", sa.Text(), nullable=True),
        sa.Column("merged_into_id", sa.Uuid(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "frappe_sync_status IN ('pending', 'synchronized', 'failed', 'not_required')",
            name="ck_crm_complaint_categories_frappe_sync_status",
        ),
        sa.ForeignKeyConstraint(
            ["merged_into_id"],
            ["crm_complaint_categories.id"],
            name="fk_crm_complaint_categories_merged_into",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "normalized_name",
            name="uq_crm_complaint_categories_normalized_name",
        ),
    )
    for name in (
        "name",
        "normalized_name",
        "display_order",
        "active",
        "frappe_ticket_type_name",
        "frappe_sync_status",
        "frappe_last_synced_at",
        "merged_into_id",
        "created_at",
        "updated_at",
    ):
        op.create_index(
            f"ix_crm_complaint_categories_{name}",
            "crm_complaint_categories",
            [name],
        )

    op.create_table(
        "crm_complaint_subcategories",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("category_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=140), nullable=False),
        sa.Column("normalized_name", sa.String(length=140), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("display_order", sa.Integer(), nullable=False, server_default="100"),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("reply_guidance", sa.Text(), nullable=True),
        sa.Column("policy_notes", sa.Text(), nullable=True),
        sa.Column("merged_into_id", sa.Uuid(), nullable=True),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["category_id"],
            ["crm_complaint_categories.id"],
            name="fk_crm_complaint_subcategories_category",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["merged_into_id"],
            ["crm_complaint_subcategories.id"],
            name="fk_crm_complaint_subcategories_merged_into",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "category_id",
            "normalized_name",
            name="uq_crm_complaint_subcategories_category_name",
        ),
    )
    for name in (
        "category_id",
        "name",
        "normalized_name",
        "display_order",
        "active",
        "merged_into_id",
        "created_at",
        "updated_at",
    ):
        op.create_index(
            f"ix_crm_complaint_subcategories_{name}",
            "crm_complaint_subcategories",
            [name],
        )

    op.create_table(
        "crm_complaint_audit_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("complaint_case_id", sa.Uuid(), nullable=True),
        sa.Column("entity_type", sa.String(length=80), nullable=False),
        sa.Column("entity_id", sa.String(length=140), nullable=False, server_default=""),
        sa.Column("event_type", sa.String(length=120), nullable=False),
        sa.Column("state", sa.String(), nullable=False, server_default="succeeded"),
        sa.Column("actor", sa.String(length=120), nullable=False, server_default="web-operator"),
        sa.Column("before_json", sa.JSON(), nullable=False),
        sa.Column("after_json", sa.JSON(), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.CheckConstraint(
            "state IN ('started', 'succeeded', 'failed', 'partial')",
            name="ck_crm_complaint_audit_events_state",
        ),
        sa.ForeignKeyConstraint(
            ["complaint_case_id"],
            ["crm_complaint_cases.id"],
            name="fk_crm_complaint_audit_events_case",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    for name in (
        "complaint_case_id",
        "entity_type",
        "entity_id",
        "event_type",
        "state",
        "actor",
        "created_at",
    ):
        op.create_index(
            f"ix_crm_complaint_audit_events_{name}",
            "crm_complaint_audit_events",
            [name],
        )

    op.add_column(
        "crm_complaint_cases",
        sa.Column("category_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "crm_complaint_cases",
        sa.Column("sub_category_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "crm_complaint_cases",
        sa.Column(
            "classification_sync_status",
            sa.String(),
            nullable=False,
            server_default="not_synced",
        ),
    )
    op.add_column(
        "crm_complaint_cases",
        sa.Column("classification_last_synced_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "crm_complaint_cases",
        sa.Column("classification_last_error", sa.Text(), nullable=True),
    )
    op.add_column(
        "crm_complaint_cases",
        sa.Column("frappe_reply_text_snapshot", sa.Text(), nullable=True),
    )
    op.create_foreign_key(
        "fk_crm_complaint_cases_category",
        "crm_complaint_cases",
        "crm_complaint_categories",
        ["category_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_crm_complaint_cases_subcategory",
        "crm_complaint_cases",
        "crm_complaint_subcategories",
        ["sub_category_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_check_constraint(
        "ck_crm_complaint_cases_classification_sync_status",
        "crm_complaint_cases",
        "classification_sync_status IN ('not_synced', 'pending', 'synchronized', 'failed')",
    )
    for name in (
        "category_id",
        "sub_category_id",
        "classification_sync_status",
        "classification_last_synced_at",
    ):
        op.create_index(
            f"ix_crm_complaint_cases_{name}",
            "crm_complaint_cases",
            [name],
        )

    connection = op.get_bind()
    now = datetime.utcnow()
    case_rows = list(
        connection.execute(
            sa.text(
                "SELECT id, category, sub_category, frappe_ticket_id, "
                "frappe_last_synced_at FROM crm_complaint_cases"
            )
        ).mappings()
    )
    category_names = sorted(
        {_clean(row["category"]) for row in case_rows if _clean(row["category"])},
        key=str.casefold,
    )
    category_table = sa.table(
        "crm_complaint_categories",
        sa.column("id", sa.Uuid()),
        sa.column("name", sa.String()),
        sa.column("normalized_name", sa.String()),
        sa.column("description", sa.Text()),
        sa.column("display_order", sa.Integer()),
        sa.column("active", sa.Boolean()),
        sa.column("default_ai_eligible", sa.Boolean()),
        sa.column("reply_guidance", sa.Text()),
        sa.column("policy_notes", sa.Text()),
        sa.column("frappe_ticket_type_name", sa.String()),
        sa.column("frappe_sync_status", sa.String()),
        sa.column("frappe_last_synced_at", sa.DateTime()),
        sa.column("frappe_last_error", sa.Text()),
        sa.column("merged_into_id", sa.Uuid()),
        sa.column("version", sa.Integer()),
        sa.column("created_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
    )
    if category_names:
        connection.execute(
            category_table.insert(),
            [
                {
                    "id": _category_id(name),
                    "name": name,
                    "normalized_name": _normalized(name),
                    "description": "Migrated from existing CRM complaint classifications.",
                    "display_order": (index + 1) * 10,
                    "active": True,
                    "default_ai_eligible": False,
                    "reply_guidance": None,
                    "policy_notes": None,
                    "frappe_ticket_type_name": name,
                    "frappe_sync_status": "synchronized",
                    "frappe_last_synced_at": now,
                    "frappe_last_error": None,
                    "merged_into_id": None,
                    "version": 1,
                    "created_at": now,
                    "updated_at": now,
                }
                for index, name in enumerate(category_names)
            ],
        )

    subcategory_pairs = sorted(
        {
            (_clean(row["category"]), _clean(row["sub_category"]))
            for row in case_rows
            if _clean(row["category"]) and _clean(row["sub_category"])
        },
        key=lambda item: (item[0].casefold(), item[1].casefold()),
    )
    subcategory_table = sa.table(
        "crm_complaint_subcategories",
        sa.column("id", sa.Uuid()),
        sa.column("category_id", sa.Uuid()),
        sa.column("name", sa.String()),
        sa.column("normalized_name", sa.String()),
        sa.column("description", sa.Text()),
        sa.column("display_order", sa.Integer()),
        sa.column("active", sa.Boolean()),
        sa.column("reply_guidance", sa.Text()),
        sa.column("policy_notes", sa.Text()),
        sa.column("merged_into_id", sa.Uuid()),
        sa.column("version", sa.Integer()),
        sa.column("created_at", sa.DateTime()),
        sa.column("updated_at", sa.DateTime()),
    )
    if subcategory_pairs:
        per_category_order: dict[str, int] = {}
        values = []
        for category_name, subcategory_name in subcategory_pairs:
            per_category_order[category_name] = per_category_order.get(category_name, 0) + 10
            values.append(
                {
                    "id": _subcategory_id(category_name, subcategory_name),
                    "category_id": _category_id(category_name),
                    "name": subcategory_name,
                    "normalized_name": _normalized(subcategory_name),
                    "description": "Migrated from existing CRM complaint classifications.",
                    "display_order": per_category_order[category_name],
                    "active": True,
                    "reply_guidance": None,
                    "policy_notes": None,
                    "merged_into_id": None,
                    "version": 1,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        connection.execute(subcategory_table.insert(), values)

    for row in case_rows:
        category_name = _clean(row["category"])
        subcategory_name = _clean(row["sub_category"])
        values = {
            "category_id": _category_id(category_name) if category_name else None,
            "sub_category_id": (
                _subcategory_id(category_name, subcategory_name)
                if category_name and subcategory_name
                else None
            ),
            "classification_sync_status": (
                "synchronized"
                if category_name and row["frappe_ticket_id"]
                else "not_synced"
            ),
            "classification_last_synced_at": (
                row["frappe_last_synced_at"]
                if category_name and row["frappe_ticket_id"]
                else None
            ),
        }
        connection.execute(
            sa.text(
                "UPDATE crm_complaint_cases SET "
                "category_id=:category_id, sub_category_id=:sub_category_id, "
                "classification_sync_status=:classification_sync_status, "
                "classification_last_synced_at=:classification_last_synced_at "
                "WHERE id=:case_id"
            ),
            {**values, "case_id": row["id"]},
        )


def downgrade() -> None:
    for name in (
        "classification_last_synced_at",
        "classification_sync_status",
        "sub_category_id",
        "category_id",
    ):
        op.drop_index(f"ix_crm_complaint_cases_{name}", table_name="crm_complaint_cases")
    op.drop_constraint(
        "ck_crm_complaint_cases_classification_sync_status",
        "crm_complaint_cases",
        type_="check",
    )
    op.drop_constraint(
        "fk_crm_complaint_cases_subcategory",
        "crm_complaint_cases",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_crm_complaint_cases_category",
        "crm_complaint_cases",
        type_="foreignkey",
    )
    for name in (
        "frappe_reply_text_snapshot",
        "classification_last_error",
        "classification_last_synced_at",
        "classification_sync_status",
        "sub_category_id",
        "category_id",
    ):
        op.drop_column("crm_complaint_cases", name)
    op.drop_table("crm_complaint_audit_events")
    op.drop_table("crm_complaint_subcategories")
    op.drop_table("crm_complaint_categories")
